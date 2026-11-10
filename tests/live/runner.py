from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import stat
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, TypeVar

ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / ".config"
CREDENTIALS_FILE = CONFIG_DIR / "credentials.json"
CLEANUP_FILE = CONFIG_DIR / "live-cleanup.json"
LOCK_FILE = CONFIG_DIR / "live-run.lock"
PROVIDERS = frozenset({"chatgpt", "gemini", "claude", "grok"})
_ACCOUNT_ID = re.compile(r"^[A-Za-z0-9_-]{1,48}$")
_MAX_ACTIONS = 20
_ACTION_TIMEOUT = 90.0
T = TypeVar("T")


class SafetyError(RuntimeError):
    """A live run was stopped before it could risk unrelated account data."""


class CleanupError(SafetyError):
    """A test-created remote object could not be verified as deleted."""


async def _run_sync(action: Callable[[], T]) -> T:
    task = asyncio.create_task(asyncio.to_thread(action))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            await asyncio.shield(task)
        except Exception:
            pass
        raise


def _contains_credentials(value: Any) -> bool:
    if isinstance(value, dict):
        return any(
            key == "credentials" or _contains_credentials(item) for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_credentials(item) for item in value)
    return False


@dataclass(frozen=True, repr=False)
class LiveAccount:
    alias: str
    provider: str
    credentials: dict[str, Any]


def _private_directory(path: Path) -> None:
    try:
        info = path.lstat()
    except OSError:
        raise SafetyError("The private .config directory is unavailable") from None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise SafetyError("The private .config path must be a real directory")
    if os.name == "posix":
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise SafetyError("The private .config directory must be owned by you with mode 0700")


def _private_json(path: Path, *, require_ignored: bool = False) -> Any:
    _private_directory(path.parent)
    try:
        info = path.lstat()
    except OSError:
        raise SafetyError("A required private JSON file is missing") from None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise SafetyError("Private JSON files must be regular files, not symlinks")
    if os.name == "posix":
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
            raise SafetyError("Private JSON files must be owned by you with mode 0600")
    if require_ignored:
        relative = path.relative_to(ROOT).as_posix()
        check = subprocess.run(
            ["git", "check-ignore", "--quiet", "--", relative],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if check.returncode != 0:
            raise SafetyError("The credential file is not excluded from Git")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise SafetyError(
            "Private JSON file is unreadable or malformed; contents were not printed"
        ) from None


def parse_accounts(data: Any) -> tuple[LiveAccount, ...]:
    if not isinstance(data, dict) or data.get("version") != 1:
        raise SafetyError("Credentials JSON must use version 1")
    raw_accounts = data.get("accounts")
    if not isinstance(raw_accounts, list):
        raise SafetyError("Credentials JSON must contain an accounts list")
    accounts: list[LiveAccount] = []
    seen: set[tuple[str, str]] = set()
    for record in raw_accounts:
        if not isinstance(record, dict):
            raise SafetyError("Each account entry must be an object")
        alias = record.get("id")
        provider = record.get("provider")
        credentials = record.get("credentials")
        if not isinstance(alias, str) or not _ACCOUNT_ID.fullmatch(alias):
            raise SafetyError(
                "Account IDs must be short local aliases using letters, digits, _ or -"
            )
        if not isinstance(provider, str) or provider.casefold() not in PROVIDERS:
            raise SafetyError("Each account must name a supported provider")
        provider = provider.casefold()
        if not isinstance(credentials, dict) or not credentials:
            raise SafetyError("Each account must contain a non-empty credentials object")
        key = (provider, alias)
        if key in seen:
            raise SafetyError("Account aliases must be unique within a provider")
        seen.add(key)
        accounts.append(LiveAccount(alias, provider, credentials))
    return tuple(accounts)


def load_accounts(
    path: Path = CREDENTIALS_FILE, *, require_ignored: bool = True
) -> tuple[LiveAccount, ...]:
    return parse_accounts(_private_json(path, require_ignored=require_ignored))


def select_account(accounts: tuple[LiveAccount, ...], provider: str, alias: str) -> LiveAccount:
    matches = [item for item in accounts if item.provider == provider and item.alias == alias]
    if len(matches) != 1:
        raise SafetyError("Select exactly one configured provider account by its local alias")
    return matches[0]


def _write_private_json(path: Path, data: dict[str, Any]) -> None:
    _private_directory(path.parent)
    if _contains_credentials(data):
        raise SafetyError("Cleanup state must never contain credentials")
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(temporary, flags, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(data, stream, ensure_ascii=False, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        if os.name == "posix":
            os.chmod(path, 0o600)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _read_cleanup_state() -> dict[str, Any] | None:
    if not CLEANUP_FILE.exists():
        return None
    data = _private_json(CLEANUP_FILE, require_ignored=True)
    run_id = data.get("run_id") if isinstance(data, dict) else None
    resources = data.get("resources") if isinstance(data, dict) else None
    if (
        not isinstance(data, dict)
        or data.get("version") != 1
        or data.get("provider") not in PROVIDERS
        or not isinstance(data.get("account"), str)
        or not _ACCOUNT_ID.fullmatch(data["account"])
        or not isinstance(run_id, str)
        or not re.fullmatch(r"webai-live-[0-9a-f]{32}", run_id)
        or not isinstance(resources, list)
        or len(resources) > 2
        or set(data) != {"version", "provider", "account", "run_id", "resources"}
    ):
        raise SafetyError("Cleanup journal is invalid; no new live tests were started")
    kinds: set[str] = set()
    for item in resources:
        if not isinstance(item, dict) or set(item) != {
            "kind",
            "markers",
            "remote_id",
            "attempted",
        }:
            raise SafetyError("Cleanup journal contains an invalid resource record")
        kind = item.get("kind")
        markers = item.get("markers")
        prefix = f"{run_id}-{kind}" if kind in {"chat", "project"} else ""
        if (
            not prefix
            or kind in kinds
            or not isinstance(markers, list)
            or not markers
            or any(marker not in {prefix, f"{prefix}-renamed"} for marker in markers)
            or not isinstance(item.get("attempted"), bool)
            or (item.get("remote_id") is not None and not isinstance(item["remote_id"], str))
        ):
            raise SafetyError("Cleanup journal contains an invalid resource record")
        kinds.add(kind)
    return data


def _write_cleanup_state(state: dict[str, Any]) -> None:
    _write_private_json(CLEANUP_FILE, state)


class RunLock:
    def __init__(self) -> None:
        self._identity: tuple[int, int] | None = None

    def __enter__(self) -> RunLock:
        _private_directory(CONFIG_DIR)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(LOCK_FILE, flags, 0o600)
        except FileExistsError:
            raise SafetyError(
                "Another live run or a stale lock exists; inspect .config/live-run.lock first"
            ) from None
        info = os.fstat(fd)
        self._identity = (info.st_dev, info.st_ino)
        try:
            with os.fdopen(fd, "w", encoding="ascii") as stream:
                stream.write(f"{os.getpid()}\n")
        except BaseException:
            self._unlink_owned()
            raise
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._unlink_owned()

    def _unlink_owned(self) -> None:
        try:
            info = LOCK_FILE.lstat()
            if (
                stat.S_ISREG(info.st_mode)
                and (os.name != "posix" or info.st_uid == os.getuid())
                and self._identity == (info.st_dev, info.st_ino)
            ):
                LOCK_FILE.unlink()
        except FileNotFoundError:
            pass


class ActionGate:
    """Serializes public API actions and leaves a fixed cooldown after each one."""

    def __init__(
        self, interval: float = 1.0, timeout: float = _ACTION_TIMEOUT, limit: int = _MAX_ACTIONS
    ):
        self.interval = interval
        self.timeout = timeout
        self.limit = limit
        self.calls = 0
        self._next_allowed = 0.0
        self._lock = asyncio.Lock()

    async def call(self, action: Callable[[], Any], *, asynchronous: bool = True) -> T:
        async with self._lock:
            if self.calls >= self.limit:
                raise SafetyError("The live run reached its fixed API-action limit")
            delay = self._next_allowed - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self.calls += 1
            try:
                if asynchronous:
                    return await asyncio.wait_for(action(), timeout=self.timeout)
                return await _run_sync(action)
            finally:
                self._next_allowed = time.monotonic() + self.interval


class LiveSuite:
    def __init__(
        self,
        session: Any,
        account: LiveAccount,
        state: dict[str, Any] | None = None,
        *,
        asynchronous: bool = True,
    ):
        self.session = session
        self.account = account
        self.asynchronous = asynchronous
        self.gate = ActionGate()
        self.marker = f"webai-live-{secrets.token_hex(16)}"
        self.state = state or {
            "version": 1,
            "provider": account.provider,
            "account": account.alias,
            "run_id": self.marker,
            "resources": [],
        }
        self.results: list[tuple[str, int]] = []
        self.skipped: list[str] = []

    async def call(self, label: str, action: Callable[[], Any]) -> T:
        started = time.monotonic()
        value = await self.gate.call(action, asynchronous=self.asynchronous)
        self.results.append((label, int((time.monotonic() - started) * 1000)))
        return value

    def skip(self, label: str) -> None:
        self.skipped.append(label)

    def _save(self) -> None:
        _write_cleanup_state(self.state)

    def _new_remote(self, kind: str, marker: str) -> dict[str, Any]:
        item = {
            "kind": kind,
            "markers": [marker],
            "remote_id": None,
            "attempted": False,
        }
        self.state["resources"].append(item)
        self._save()
        return item

    async def read_only(self) -> None:
        from webaiprovider.base import Capability

        capabilities = self.session.capabilities
        if Capability.MODELS in capabilities:
            models = await self.call("models.list", self.session.models.list)
            if not models:
                raise SafetyError("The authenticated account returned an empty model catalog")
        else:
            self.skip("models.list: unsupported")
        if Capability.CHAT in capabilities:
            await self.call("chats.list_metadata", self.session.chats.refresh)
        else:
            self.skip("chats.list_metadata: unsupported")
        if Capability.PROJECTS in capabilities:
            await self.call("projects.list_metadata", self.session.projects.refresh)
        else:
            self.skip("projects.list_metadata: unsupported")

    async def disposable_project(self) -> None:
        from webaiprovider.base import Capability

        capabilities = self.session.capabilities
        if not {Capability.PROJECT_CREATE, Capability.PROJECT_DELETE}.issubset(capabilities):
            self.skip(
                "project create/update/resources/delete: provider cannot guarantee project cleanup"
            )
            return

        marker = f"{self.marker}-project"
        entry = self._new_remote("project", marker)
        entry["attempted"] = True
        self._save()
        project = await self.call("projects.create", lambda: self.session.projects.create(marker))
        if not project.remote_id:
            raise SafetyError("The temporary project was not assigned a verifiable remote ID")
        entry["remote_id"] = str(project.remote_id)
        self._save()

        if Capability.PROJECT_UPDATE in capabilities:
            renamed = f"{marker}-renamed"
            entry["markers"].append(renamed)
            self._save()
            await self.call("projects.rename", lambda: project.rename(renamed))
            await self.call(
                "projects.instructions.edit",
                lambda: project.instruction.edit(
                    f"Temporary live API test {self.marker}; remove this project."
                ),
            )
        else:
            self.skip("projects.rename/instructions.edit: unsupported")

        if Capability.RESOURCES in capabilities:
            resource_name = f"{marker}-resource"
            resource = await self.call(
                "projects.resources.add",
                lambda: project.resources.add(self.marker, "text", name=resource_name),
            )
            resource_id = resource.get("id") if isinstance(resource, dict) else None
            if not resource_id:
                raise SafetyError("The temporary project resource has no verifiable ID")
            removed = await self.call(
                "projects.resources.remove", lambda: project.resources.remove(str(resource_id))
            )
            if removed is not True:
                raise SafetyError("The temporary project resource removal was not confirmed")
        else:
            self.skip("projects.resources.add/remove: unsupported")

    async def disposable_chat(self) -> None:
        from webaiprovider.base import Capability
        from webaiprovider.resources.model.collections import Models

        capabilities = self.session.capabilities
        if not {Capability.CHAT, Capability.DELETE, Capability.MODELS}.issubset(capabilities):
            self.skip("chat message/stream/rename/delete: provider cannot guarantee chat cleanup")
            return

        marker = f"{self.marker}-chat"
        entry = self._new_remote("chat", marker)
        chat = self.session.chats.create(title=marker)
        operation = chat.messages.create(
            f"{marker} Reply with exactly OK.",
            model=Models.LOW,
        )
        entry["attempted"] = True
        self._save()

        if self.asynchronous:

            async def consume_stream() -> int:
                events = 0
                async for _event in operation.stream():
                    events += 1
                    self._record_chat_id(entry, chat)
                self._record_chat_id(entry, chat)
                return events

            event_count = await self.call("messages.create.stream", consume_stream)
            response = await self.call("messages.wait", lambda: operation)
        else:

            def consume_stream() -> int:
                events = 0
                for _event in operation.stream():
                    events += 1
                    self._record_chat_id(entry, chat)
                self._record_chat_id(entry, chat)
                return events

            event_count = await self.call("messages.create.stream", consume_stream)
            response = await self.call("messages.wait", operation.wait)
        self._record_chat_id(entry, chat)
        if not chat.remote_id:
            raise SafetyError("The temporary conversation has no verifiable remote ID")
        if not event_count or not getattr(response.message, "content", ""):
            raise SafetyError("The temporary message did not produce a verifiable streamed answer")

        messages = await self.call("messages.list_temporary_chat", chat.messages.refresh)
        roles = {getattr(message.role, "value", message.role) for message in messages}
        if "user" not in roles or "assistant" not in roles:
            raise SafetyError(
                "The temporary conversation history did not contain both message roles"
            )

        if Capability.RENAME in capabilities:
            renamed = f"{marker}-renamed"
            entry["markers"].append(renamed)
            self._save()
            await self.call("chats.rename_temporary", lambda: chat.rename(renamed))
        else:
            self.skip("chats.rename: unsupported")

    def _record_chat_id(self, entry: dict[str, Any], chat: Any) -> None:
        if chat.remote_id:
            entry["remote_id"] = str(chat.remote_id)
            self._save()

    async def cleanup_pending(self) -> None:
        from webaiprovider.errors import RemoteNotFound

        failures = 0
        for entry in list(reversed(self.state["resources"])):
            try:
                await self._cleanup_one(entry, RemoteNotFound)
            except Exception:
                failures += 1
        if failures:
            self._save()
            raise CleanupError(
                "At least one temporary remote object could not be verified as deleted"
            )
        self.state["resources"].clear()
        if CLEANUP_FILE.exists():
            CLEANUP_FILE.unlink()

    async def _cleanup_one(self, entry: dict[str, Any], missing_type: type[Exception]) -> None:
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
            values = await self.call(f"cleanup.locate_{kind}", collection.refresh)
            markers = tuple(entry.get("markers", ()))
            matches = [
                value
                for value in values
                if any(marker in self._remote_title(value, kind) for marker in markers)
            ]
            if len(matches) != 1:
                raise CleanupError(
                    "A possibly-created temporary object could not be uniquely located"
                )
            remote_id = getattr(matches[0], "remote_id", None)
            if not remote_id:
                raise CleanupError("A possibly-created temporary object has no remote ID")
            entry["remote_id"] = str(remote_id)
            self._save()

        try:
            remote = await self.call(f"cleanup.get_{kind}", lambda: get_remote(str(remote_id)))
        except missing_type:
            self._remove_entry(entry)
            return
        if remote is None:
            self._remove_entry(entry)
            return
        if not any(
            marker in self._remote_title(remote, kind) for marker in entry.get("markers", ())
        ):
            raise CleanupError("The remote object does not match this run's unique test marker")

        await self.call(f"cleanup.delete_{kind}", lambda: delete_remote(str(remote_id)))
        try:
            remaining = await self.call(
                f"cleanup.verify_{kind}_deleted", lambda: get_remote(str(remote_id))
            )
        except missing_type:
            remaining = None
        if remaining is not None:
            raise CleanupError("The provider did not confirm deletion of a temporary object")
        self._remove_entry(entry)

    @staticmethod
    def _remote_title(value: Any, kind: str) -> str:
        field = "title" if kind == "chat" else "name"
        result = getattr(value, field, "")
        if not result and kind == "project":
            result = getattr(value, "title", "")
        return result if isinstance(result, str) else ""

    def _remove_entry(self, entry: dict[str, Any]) -> None:
        self.state["resources"].remove(entry)
        if self.state["resources"]:
            self._save()
        elif CLEANUP_FILE.exists():
            CLEANUP_FILE.unlink()

    def print_report(self) -> None:
        for label, milliseconds in self.results:
            print(f"PASS  {label}  {milliseconds} ms")
        for label in self.skipped:
            print(f"SKIP  {label}")
        print(
            f"Completed {self.account.provider} live checks; {self.gate.calls} API actions; no content or credentials logged."
        )


async def execute_live(account: LiveAccount, args, cleanup_state: dict[str, Any] | None) -> None:
    from webaiprovider.accounts.account import Account

    account_object = Account(
        id=account.alias,
        provider=account.provider,
        credentials=account.credentials,
        name="Live API test",
    )
    if args.api == "async":
        from webaiprovider.async_provider import AsyncProvider

        session = AsyncProvider(account_object)
    else:
        from webaiprovider.provider import Provider

        session = Provider(account_object)
    suite = LiveSuite(session, account, cleanup_state, asynchronous=args.api == "async")
    opened = False
    try:
        await suite.call("session.open", session.open)
        opened = True
        if args.recover:
            await suite.cleanup_pending()
        else:
            await suite.read_only()
            if args.allow_temporary_writes:
                await suite.disposable_project()
            if args.allow_one_generation:
                await suite.disposable_chat()
    finally:
        try:
            if opened and suite.state["resources"]:
                await suite.cleanup_pending()
        finally:
            if args.api == "async":
                await session.close()
            else:
                await _run_sync(session.close)
    suite.print_report()


def main(argv: list[str] | None = None) -> int:
    import argparse
    import logging

    parser = argparse.ArgumentParser(description="Explicit, rate-limited live provider API checks")
    parser.add_argument("--execute", action="store_true", help="allow real provider requests")
    parser.add_argument("--provider", choices=sorted(PROVIDERS), help="test exactly one provider")
    parser.add_argument("--account", help="local account alias from .config/credentials.json")
    parser.add_argument(
        "--api", choices=("async", "sync"), default="async", help="test one API style"
    )
    parser.add_argument(
        "--allow-temporary-writes",
        action="store_true",
        help="create and delete one disposable project",
    )
    parser.add_argument(
        "--allow-one-generation",
        action="store_true",
        help="also send one low-effort message in a disposable chat",
    )
    parser.add_argument(
        "--recover",
        action="store_true",
        help="only recover and verify cleanup from a previous interrupted run",
    )
    args = parser.parse_args(argv)

    if not args.execute:
        if args.allow_temporary_writes or args.allow_one_generation or args.recover:
            parser.error("write and recovery flags require --execute")
        print("Live tests are disabled by default; this command made no network requests.")
        print(
            "Configure .config/credentials.json, then select one API style, provider, and account with --execute."
        )
        return 0
    if not args.provider or not args.account:
        parser.error("--execute requires exactly one --provider and one --account")
    if args.allow_one_generation and not args.allow_temporary_writes:
        parser.error("--allow-one-generation also requires --allow-temporary-writes")
    if args.recover and (args.allow_temporary_writes or args.allow_one_generation):
        parser.error("--recover cannot be combined with new test writes")

    try:
        accounts = load_accounts()
        account = select_account(accounts, args.provider, args.account)
        state = _read_cleanup_state()
        if state is not None:
            if state.get("provider") != account.provider or state.get("account") != account.alias:
                raise SafetyError(
                    "Pending cleanup belongs to a different account; no provider request was made"
                )
            if not args.recover:
                raise SafetyError(
                    "Pending cleanup must be recovered before starting any new live run"
                )
        elif args.recover:
            print("No pending cleanup record exists; no provider request was made.")
            return 0

        for logger_name in ("httpx", "httpcore", "curl_cffi", "websockets", "urllib3"):
            logging.getLogger(logger_name).setLevel(logging.WARNING)
        with RunLock():
            asyncio.run(execute_live(account, args, state))
        return 0
    except CleanupError as exc:
        print(
            f"CLEANUP BLOCKED: {exc} Further live requests are stopped; the journal remains for --recover."
        )
        return 3
    except SafetyError as exc:
        print(f"REFUSED: {exc}")
        return 2
    except KeyboardInterrupt:
        print("Interrupted. Cleanup was attempted; inspect the journal before any next run.")
        return 130
    except Exception as exc:
        print(f"STOPPED: {type(exc).__name__}; details suppressed to protect account data.")
        if CLEANUP_FILE.exists():
            print("A cleanup journal remains. Recover it before starting another live run.")
        return 1
