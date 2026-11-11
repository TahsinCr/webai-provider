from __future__ import annotations

import secrets
import time
from typing import Any

from .actions import ActionGate
from .config import LiveAccount
from .errors import CleanupError, LiveApiError, SafetyError
from .journal import CLEANUP_FILE, write_cleanup_state


class LiveContext:
    def __init__(
        self, session: Any, account: LiveAccount, api: str, loop=None, state=None, gate=None
    ):
        self.session = session
        self.account = account
        self.api = api
        self.loop = loop
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
            if self.asynchronous:
                value = self.loop.run_until_complete(self.gate.call_async(action))
            else:
                value = self.gate.call_sync(action)
        except SafetyError:
            raise
        except Exception as exc:
            if isinstance(exc, expected_errors):
                raise
            raise LiveApiError(
                f"{label} failed: {type(exc).__name__}; provider details suppressed"
            ) from None
        self.results.append((label, round((time.monotonic() - started) * 1000)))
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
                f"{label} failed: {type(exc).__name__}; provider details suppressed"
            ) from None
        self.results.append((label, round((time.monotonic() - started) * 1000)))
        return value

    def skip(self, label: str) -> None:
        self.skipped.append(label)

    def save(self) -> None:
        write_cleanup_state(self.state)

    def new_remote(self, kind: str, marker: str) -> dict[str, Any]:
        entry = {"kind": kind, "markers": [marker], "remote_id": None, "attempted": False}
        self.state["resources"].append(entry)
        self.save()
        return entry

    def record_chat_id(self, entry: dict[str, Any], chat: Any) -> None:
        if chat.remote_id:
            entry["remote_id"] = str(chat.remote_id)
            self.save()

    def cleanup_pending(self) -> None:
        from webaiprovider.errors import RemoteNotFound

        failures = 0
        for entry in list(reversed(self.state["resources"])):
            if id(entry) in self._cleanup_attempted:
                failures += 1
                continue
            self._cleanup_attempted.add(id(entry))
            try:
                self._cleanup_one(entry, RemoteNotFound)
            except Exception:
                failures += 1
        if failures:
            self.save()
            raise CleanupError(
                "At least one temporary remote object could not be verified as deleted"
            )
        self.state["resources"].clear()
        try:
            CLEANUP_FILE.unlink()
        except FileNotFoundError:
            pass

    def cleanup_entry(self, entry: dict[str, Any]) -> None:
        from webaiprovider.errors import RemoteNotFound

        self._cleanup_attempted.add(id(entry))
        self._cleanup_one(entry, RemoteNotFound)

    async def cleanup_pending_async(self) -> None:
        from webaiprovider.errors import RemoteNotFound

        failures = 0
        for entry in list(reversed(self.state["resources"])):
            if id(entry) in self._cleanup_attempted:
                failures += 1
                continue
            self._cleanup_attempted.add(id(entry))
            try:
                await self._cleanup_one_async(entry, RemoteNotFound)
            except Exception:
                failures += 1
        if failures:
            self.save()
            raise CleanupError(
                "At least one temporary remote object could not be verified as deleted"
            )
        self.state["resources"].clear()
        try:
            CLEANUP_FILE.unlink()
        except FileNotFoundError:
            pass

    async def cleanup_entry_async(self, entry: dict[str, Any]) -> None:
        from webaiprovider.errors import RemoteNotFound

        self._cleanup_attempted.add(id(entry))
        await self._cleanup_one_async(entry, RemoteNotFound)

    async def _cleanup_one_async(
        self, entry: dict[str, Any], missing_type: type[Exception]
    ) -> None:
        from webaiprovider.errors import ProviderError

        kind = entry["kind"]
        if kind not in {"chat", "project"}:
            raise CleanupError("Cleanup journal contains an unknown object type")
        if not entry.get("attempted"):
            self._remove_entry(entry)
            return

        plural = "chats" if kind == "chat" else "projects"
        collection = getattr(self.session, plural)
        get_remote = getattr(self.session, "get_chat" if kind == "chat" else "get_project")
        delete_remote = getattr(self.session, "delete_chat" if kind == "chat" else "delete_project")
        remote_id = entry.get("remote_id")
        if not remote_id:
            values = (
                await self._all_projects_async()
                if kind == "project"
                else await self.call_async(f"cleanup.locate_{kind}", collection.refresh)
            )
            markers = tuple(entry.get("markers", ()))
            matches = [value for value in values if self._matches_marker(value, kind, markers)]
            if len(matches) != 1 or not (remote_id := self._remote_id(matches[0])):
                raise CleanupError("A possibly-created object could not be uniquely located")
            remote_id = entry["remote_id"] = str(remote_id)
            self.save()

        async def listed_remote() -> Any | None:
            values = await self.call_async(f"cleanup.verify_{kind}_list", collection.refresh)
            matches = [
                value for value in values if str(getattr(value, "remote_id", "")) == str(remote_id)
            ]
            if len(matches) > 1:
                raise CleanupError("The temporary object could not be uniquely verified")
            return matches[0] if matches else None

        try:
            remote = await self.call_async(
                f"cleanup.get_{kind}",
                lambda: get_remote(str(remote_id)),
                expected_errors=(missing_type, ProviderError),
            )
        except missing_type:
            self._remove_entry(entry)
            return
        except ProviderError:
            remote = await listed_remote()
        if remote is None:
            self._remove_entry(entry)
            return
        if not await self._matches_marker_async(remote, kind, tuple(entry["markers"])):
            raise CleanupError("The remote object does not match this run's unique test marker")

        await self.call_async(f"cleanup.delete_{kind}", lambda: delete_remote(str(remote_id)))
        try:
            remaining = await self.call_async(
                f"cleanup.verify_{kind}_deleted",
                lambda: get_remote(str(remote_id)),
                expected_errors=(missing_type, ProviderError),
            )
        except missing_type:
            remaining = None
        except ProviderError:
            remaining = await listed_remote()
        if remaining is not None:
            raise CleanupError("The provider did not confirm deletion of a temporary object")
        self._remove_entry(entry)

    def _cleanup_one(self, entry: dict[str, Any], missing_type: type[Exception]) -> None:
        from webaiprovider.errors import ProviderError

        kind = entry["kind"]
        if kind not in {"chat", "project"}:
            raise CleanupError("Cleanup journal contains an unknown object type")
        if not entry.get("attempted"):
            self._remove_entry(entry)
            return

        plural = "chats" if kind == "chat" else "projects"
        collection = getattr(self.session, plural)
        get_remote = getattr(self.session, "get_chat" if kind == "chat" else "get_project")
        delete_remote = getattr(self.session, "delete_chat" if kind == "chat" else "delete_project")
        remote_id = entry.get("remote_id")
        if not remote_id:
            values = (
                self._all_projects()
                if kind == "project"
                else self.call(f"cleanup.locate_{kind}", collection.refresh)
            )
            markers = tuple(entry.get("markers", ()))
            matches = [value for value in values if self._matches_marker(value, kind, markers)]
            if len(matches) != 1 or not (remote_id := self._remote_id(matches[0])):
                raise CleanupError("A possibly-created object could not be uniquely located")
            remote_id = entry["remote_id"] = str(remote_id)
            self.save()

        def listed_remote() -> Any | None:
            values = self.call(f"cleanup.verify_{kind}_list", collection.refresh)
            matches = [
                value for value in values if str(getattr(value, "remote_id", "")) == str(remote_id)
            ]
            if len(matches) > 1:
                raise CleanupError("The temporary object could not be uniquely verified")
            return matches[0] if matches else None

        try:
            remote = self.call(
                f"cleanup.get_{kind}",
                lambda: get_remote(str(remote_id)),
                expected_errors=(missing_type, ProviderError),
            )
        except missing_type:
            self._remove_entry(entry)
            return
        except ProviderError:
            remote = listed_remote()
        if remote is None:
            self._remove_entry(entry)
            return
        if not self._matches_marker_sync(remote, kind, tuple(entry["markers"])):
            raise CleanupError("The remote object does not match this run's unique test marker")

        self.call(f"cleanup.delete_{kind}", lambda: delete_remote(str(remote_id)))
        try:
            remaining = self.call(
                f"cleanup.verify_{kind}_deleted",
                lambda: get_remote(str(remote_id)),
                expected_errors=(missing_type, ProviderError),
            )
        except missing_type:
            remaining = None
        except ProviderError:
            remaining = listed_remote()
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

    def _all_projects(self) -> list[Any]:
        provider = self.session._require_provider()
        cursor = None
        seen: set[str] = set()
        projects = []
        for _ in range(100):
            page = self.call("cleanup.locate_project", lambda: provider.list_projects(cursor))
            projects.extend(page.items)
            next_cursor = page.next_cursor
            if next_cursor is None:
                return projects
            if next_cursor == cursor or next_cursor in seen:
                raise CleanupError("Project listing returned a repeated cursor")
            seen.add(next_cursor)
            cursor = next_cursor
        raise CleanupError("Project listing exceeded the safe recovery page limit")

    async def _all_projects_async(self) -> list[Any]:
        provider = self.session._require_provider()
        cursor = None
        seen: set[str] = set()
        projects = []
        for _ in range(100):
            page = await self.call_async(
                "cleanup.locate_project", lambda: provider.list_projects(cursor)
            )
            projects.extend(page.items)
            next_cursor = page.next_cursor
            if next_cursor is None:
                return projects
            if next_cursor == cursor or next_cursor in seen:
                raise CleanupError("Project listing returned a repeated cursor")
            seen.add(next_cursor)
            cursor = next_cursor
        raise CleanupError("Project listing exceeded the safe recovery page limit")

    @classmethod
    def _matches_marker(cls, value: Any, kind: str, markers: tuple[str, ...]) -> bool:
        if any(marker in cls._title(value, kind) for marker in markers):
            return True
        return kind == "chat" and cls._messages_match_markers(
            getattr(value, "messages", ()), markers
        )

    def _matches_marker_sync(self, value: Any, kind: str, markers: tuple[str, ...]) -> bool:
        if self._matches_marker(value, kind, markers):
            return True
        if kind != "chat":
            return False
        refresh = getattr(getattr(value, "messages", None), "refresh", None)
        return callable(refresh) and self._messages_match_markers(
            self.call("cleanup.read_chat_history", refresh), markers
        )

    async def _matches_marker_async(self, value: Any, kind: str, markers: tuple[str, ...]) -> bool:
        if self._matches_marker(value, kind, markers):
            return True
        if kind != "chat":
            return False
        refresh = getattr(getattr(value, "messages", None), "refresh", None)
        if not callable(refresh):
            return False
        messages = await self.call_async("cleanup.read_chat_history", refresh)
        return self._messages_match_markers(messages, markers)

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
            try:
                CLEANUP_FILE.unlink()
            except FileNotFoundError:
                pass

    def close(self) -> None:
        try:
            if self.asynchronous:
                self.loop.run_until_complete(self.session.close())
            else:
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
        for label, milliseconds in self.results:
            print(f"PASS  {label}  {milliseconds} ms")
        for label in self.skipped:
            print(f"SKIP  {label}")
        print(f"Completed {self.account.provider} live API checks; {self.gate.calls} actions.")
