from __future__ import annotations

import secrets
import time
from contextlib import suppress
from typing import Any

import httpx
from curl_cffi.requests.exceptions import Timeout as CurlTimeout

from .actions import ActionGate
from .config import LiveAccount
from .errors import CleanupError, LiveApiError, SafetyError
from .journal import _DEFAULT_CLEANUP_FILE, CLEANUP_FILE, cleanup_file, write_cleanup_state


def _safe_error_label(exc: Exception) -> str:
    if isinstance(exc, CurlTimeout):
        message = str(exc)
        if "Resolving timed out" in message:
            return "provider DNS resolution timeout"
        if "Connection timed out" in message or "Connection timeout" in message:
            return "provider connection timeout"
        return "provider network timeout"
    if isinstance(exc, httpx.TimeoutException):
        return "provider network timeout"
    if isinstance(exc, TimeoutError):
        return "operation timeout"
    kind = type(exc).__name__
    prefix = "ChatGPT returned HTTP "
    message = str(exc)
    if kind == "ChatGPTError" and message.startswith(prefix):
        status = message[len(prefix) :].split(" ", 1)[0]
        if len(status) == 3 and status.isdecimal():
            return f"{kind} (HTTP {status})"
    return kind


class LiveContext:
    def __init__(self, session: Any, account: LiveAccount, api: str, *, state=None, gate=None):
        self.session = session
        self.account = account
        self.api = api
        self.asynchronous = api == "async"
        self.gate = gate or ActionGate()
        self.marker = state["run_id"] if state else f"webai-live-{secrets.token_hex(16)}"
        self.state = state or {
            "version": 1,
            "provider": account.provider,
            "account": account.alias,
            "run_id": self.marker,
            "resources": [],
        }
        self.results: list[tuple[str, int]] = []
        self.skipped: list[str] = []
        self._cleanup_attempted: set[int] = set()

    def call(self, label: str, action, *, expected_errors: tuple[type[Exception], ...] = ()):
        started = time.monotonic()
        try:
            value = self.gate.call_sync(action)
        except SafetyError:
            raise
        except Exception as exc:
            if isinstance(exc, expected_errors):
                raise
            raise LiveApiError(
                f"{label} failed after {round((time.monotonic() - started) * 1000)} ms: "
                f"{_safe_error_label(exc)}; provider details suppressed"
            ) from None
        elapsed = round((time.monotonic() - started) * 1000)
        self.results.append((label, elapsed))
        print(f"PASS  {label}  {elapsed} ms", flush=True)
        return value

    async def call_async(
        self, label: str, action, *, expected_errors: tuple[type[Exception], ...] = ()
    ):
        started = time.monotonic()
        try:
            value = await self.gate.call_async(action)
        except SafetyError:
            raise
        except Exception as exc:
            if isinstance(exc, expected_errors):
                raise
            raise LiveApiError(
                f"{label} failed after {round((time.monotonic() - started) * 1000)} ms: "
                f"{_safe_error_label(exc)}; provider details suppressed"
            ) from None
        elapsed = round((time.monotonic() - started) * 1000)
        self.results.append((label, elapsed))
        print(f"PASS  {label}  {elapsed} ms", flush=True)
        return value

    def skip(self, label: str) -> None:
        self.skipped.append(label)

    def _cleanup_path(self):
        return (
            cleanup_file(self.account.provider)
            if CLEANUP_FILE == _DEFAULT_CLEANUP_FILE
            else CLEANUP_FILE
        )

    def save(self) -> None:
        if CLEANUP_FILE == _DEFAULT_CLEANUP_FILE:
            write_cleanup_state(self.state, provider=self.account.provider)
        else:
            write_cleanup_state(self.state)

    def new_remote(self, kind: str, marker: str) -> dict[str, Any]:
        entry = {"kind": kind, "markers": [marker], "remote_id": None, "attempted": False}
        self.state["resources"].append(entry)
        self.save()
        return entry

    def record_chat_id(self, entry: dict[str, Any], chat: Any) -> None:
        remote_id = str(chat.remote_id) if chat.remote_id else None
        if remote_id and entry.get("remote_id") != remote_id:
            entry["remote_id"] = remote_id
            self.save()

    def cleanup_pending(self) -> None:
        self.session._call(self.cleanup_pending_async())

    def cleanup_entry(self, entry: dict[str, Any]) -> None:
        self.session._call(self.cleanup_entry_async(entry))

    async def cleanup_pending_async(self) -> None:
        failures = 0
        for entry in list(reversed(self.state["resources"])):
            if id(entry) in self._cleanup_attempted:
                failures += 1
                continue
            self._cleanup_attempted.add(id(entry))
            try:
                await self._cleanup_one_async(entry)
            except Exception:
                failures += 1
        if failures:
            self.save()
            raise CleanupError(
                "At least one temporary remote object could not be verified as deleted"
            )
        self.state["resources"].clear()
        with suppress(FileNotFoundError):
            self._cleanup_path().unlink()

    async def cleanup_entry_async(self, entry: dict[str, Any]) -> None:
        self._cleanup_attempted.add(id(entry))
        await self._cleanup_one_async(entry)

    async def _cleanup_one_async(self, entry: dict[str, Any]) -> None:
        from webaiprovider.errors import ProviderError, RemoteNotFound

        kind = entry["kind"]
        if kind not in {"chat", "project"}:
            raise CleanupError("Cleanup journal contains an unknown object type")
        if not entry.get("attempted"):
            self._remove_entry(entry)
            return

        provider = self.session if self.asynchronous else self.session._provider
        get_remote = getattr(provider, f"get_{kind}")
        delete_remote = getattr(provider, f"delete_{kind}")
        remote_id = entry.get("remote_id")
        if not remote_id:
            values = await self._all_records(kind)
            markers = tuple(entry.get("markers", ()))
            matches = [value for value in values if self._matches_marker(value, kind, markers)]
            if len(matches) != 1 or not (remote_id := self._remote_id(matches[0])):
                raise CleanupError("A possibly-created object could not be uniquely located")
            remote_id = entry["remote_id"] = str(remote_id)
            self.save()

        async def listed_remote() -> Any | None:
            values = await self._all_records(kind)
            matches = [
                value for value in values if str(self._remote_id(value) or "") == str(remote_id)
            ]
            if len(matches) > 1:
                raise CleanupError("The temporary object could not be uniquely verified")
            return matches[0] if matches else None

        try:
            remote = await self.call_async(
                f"cleanup.get_{kind}",
                lambda: get_remote(str(remote_id)),
                expected_errors=(ProviderError,),
            )
        except RemoteNotFound:
            self._remove_entry(entry)
            return
        except ProviderError:
            remote = await listed_remote()
        if remote is None:
            self._remove_entry(entry)
            return
        if str(self._remote_id(remote)) != str(remote_id) or not self._matches_marker(
            remote, kind, tuple(entry["markers"])
        ):
            raise CleanupError("The remote object does not match this run's unique test marker")

        with suppress(ProviderError):
            await self.call_async(
                f"cleanup.delete_{kind}",
                lambda: delete_remote(str(remote_id)),
                expected_errors=(ProviderError,),
            )
        try:
            remaining = await self.call_async(
                f"cleanup.verify_{kind}_deleted",
                lambda: get_remote(str(remote_id)),
                expected_errors=(ProviderError,),
            )
        except RemoteNotFound:
            remaining = None
        except ProviderError:
            remaining = await listed_remote()
        if remaining is not None:
            raise CleanupError("The provider did not confirm deletion of a temporary object")
        self._remove_entry(entry)

    @staticmethod
    def _title(value: Any, kind: str) -> str:
        title = getattr(value, "title" if kind == "chat" else "name", "")
        if not title and kind == "project":
            title = getattr(value, "title", "")
        return title if isinstance(title, str) else ""

    @staticmethod
    def _remote_id(value: Any) -> Any:
        return getattr(value, "remote_id", None) or getattr(
            getattr(value, "origin", None), "remote_id", None
        )

    async def _all_records(self, kind: str) -> list[Any]:
        provider = self.session if self.asynchronous else self.session._provider
        list_remote = getattr(provider, "list_chats" if kind == "chat" else "list_projects")
        cursor = None
        seen: set[str] = set()
        records = []
        for _ in range(100):
            page = await self.call_async(
                f"cleanup.locate_{kind}", lambda cursor=cursor: list_remote(cursor)
            )
            records.extend(page.items)
            next_cursor = page.next_cursor
            if next_cursor is None:
                return records
            if next_cursor == cursor or next_cursor in seen:
                raise CleanupError("Remote listing returned a repeated cursor")
            seen.add(next_cursor)
            cursor = next_cursor
        raise CleanupError("Remote listing exceeded the safe recovery page limit")

    @classmethod
    def _matches_marker(cls, value: Any, kind: str, markers: tuple[str, ...]) -> bool:
        if any(marker in cls._title(value, kind) for marker in markers):
            return True
        return kind == "chat" and cls._messages_match_markers(
            getattr(value, "messages", ()), markers
        )

    @staticmethod
    def _messages_match_markers(messages: Any, markers: tuple[str, ...]) -> bool:
        try:
            iterator = iter(messages)
        except TypeError:
            return False
        for message in iterator:
            content = getattr(message, "content", ())
            if isinstance(content, str):
                if any(marker in content for marker in markers):
                    return True
                continue
            try:
                blocks = iter(content)
            except TypeError:
                continue
            for block in blocks:
                text = getattr(block, "text", "")
                if isinstance(text, str) and any(marker in text for marker in markers):
                    return True
        return False

    def _remove_entry(self, entry: dict[str, Any]) -> None:
        self.state["resources"].remove(entry)
        if self.state["resources"]:
            self.save()
        else:
            with suppress(FileNotFoundError):
                self._cleanup_path().unlink()

    def close(self) -> None:
        try:
            self.session.close()
        except Exception as exc:
            raise LiveApiError(
                f"session.close failed: {type(exc).__name__}; provider details suppressed"
            ) from None

    async def aclose(self) -> None:
        try:
            await self.session.close()
        except Exception as exc:
            raise LiveApiError(
                f"session.close failed: {type(exc).__name__}; provider details suppressed"
            ) from None

    def print_report(self) -> None:
        for label in self.skipped:
            print(f"SKIP  {label}", flush=True)
        print(
            f"Completed {self.account.provider} live API checks; {self.gate.calls} actions.",
            flush=True,
        )
