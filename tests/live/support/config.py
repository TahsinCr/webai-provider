from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import tempfile
import threading
import time
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import SafetyError

ROOT = Path(__file__).resolve().parents[3]
CONFIG_DIR = ROOT / ".config"
CREDENTIALS_FILE = CONFIG_DIR / "credentials.json"
PROVIDERS = frozenset({"chatgpt", "gemini", "claude", "grok"})
_PROVIDER_ORIGINS = {
    "chatgpt": "https://chatgpt.com",
    "gemini": "https://gemini.google.com",
    "claude": "https://claude.ai",
    "grok": "https://grok.com",
}
_ACCOUNT_ID = re.compile(r"^[A-Za-z0-9_-]{1,48}$")


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
    if os.name == "posix" and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700):
        raise SafetyError("The private .config directory must be owned by you with mode 0700")


def read_private_json(path: Path, *, require_ignored: bool = False) -> Any:
    _private_directory(path.parent)
    try:
        info = path.lstat()
    except OSError:
        raise SafetyError("A required private JSON file is missing") from None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise SafetyError("Private JSON files must be regular files, not symlinks")
    if os.name == "posix" and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600):
        raise SafetyError("Private JSON files must be owned by you with mode 0600")
    if require_ignored:
        _require_ignored(path)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise SafetyError(
            "Private JSON file is unreadable or malformed; contents were not printed"
        ) from None


def _ensure_private_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
    except OSError:
        raise SafetyError("The private .config directory is unavailable") from None
    _private_directory(path)


def _require_ignored(path: Path) -> None:
    try:
        relative = path.relative_to(ROOT).as_posix()
    except ValueError:
        raise SafetyError(
            "The credential file must be inside the project config directory"
        ) from None
    check = subprocess.run(
        ["git", "check-ignore", "--quiet", "--", relative],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if check.returncode != 0:
        raise SafetyError("The credential file is not excluded from Git")


def _write_private_json(path: Path, value: Any, *, require_ignored: bool) -> None:
    _ensure_private_directory(path.parent)
    if require_ignored:
        _require_ignored(path)
    try:
        path.lstat()
    except FileNotFoundError:
        pass
    else:
        read_private_json(path, require_ignored=False)

    temporary_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as output:
            temporary_name = output.name
            if os.name == "posix":
                os.fchmod(output.fileno(), 0o600)
            json.dump(value, output, ensure_ascii=False)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_name, path)
    except (OSError, TypeError, ValueError):
        raise SafetyError("Could not securely save provider credentials") from None
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)
    read_private_json(path, require_ignored=False)


def load_accounts(
    path: Path = CREDENTIALS_FILE, *, require_ignored: bool = True
) -> tuple[LiveAccount, ...]:
    with _credentials_lock(path):
        return parse_accounts(read_private_json(path, require_ignored=require_ignored))


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
        alias, provider, credentials = (
            record.get("id"),
            record.get("provider"),
            record.get("credentials"),
        )
        if not isinstance(alias, str) or not _ACCOUNT_ID.fullmatch(alias):
            raise SafetyError(
                "Account IDs must be short local aliases using letters, digits, _ or -"
            )
        if not isinstance(provider, str) or provider.casefold() not in PROVIDERS:
            raise SafetyError("Each account must name a supported provider")
        if not isinstance(credentials, dict) or not credentials:
            raise SafetyError("Each account must contain a non-empty credentials object")
        provider = provider.casefold()
        if (provider, alias) in seen:
            raise SafetyError("Account aliases must be unique within a provider")
        seen.add((provider, alias))
        accounts.append(LiveAccount(alias, provider, credentials))
    return tuple(accounts)


def select_account(accounts: tuple[LiveAccount, ...], provider: str, alias: str) -> LiveAccount:
    matches = [item for item in accounts if item.provider == provider and item.alias == alias]
    if len(matches) != 1:
        raise SafetyError("Select exactly one configured provider account by its local alias")
    return matches[0]


_CREDENTIALS_LOCKS_GUARD = threading.Lock()
_CREDENTIALS_LOCKS: dict[str, threading.RLock] = {}
_LOCK_WAIT_SECONDS = 60.0
_LOCK_RETRY_SECONDS = 0.05


def _thread_lock_for(path: Path) -> threading.RLock:
    key = os.fspath(path.resolve(strict=False))
    with _CREDENTIALS_LOCKS_GUARD:
        lock = _CREDENTIALS_LOCKS.get(key)
        if lock is None:
            lock = _CREDENTIALS_LOCKS[key] = threading.RLock()
        return lock


def supports_process_credentials_lock() -> bool:
    """Return True when the platform exposes OS file locking for credentials."""
    if os.name == "posix":
        try:
            import fcntl  # noqa: F401
        except ImportError:
            return False
        return True
    if os.name == "nt":
        try:
            import msvcrt  # noqa: F401
        except ImportError:
            return False
        return True
    return False


@contextmanager
def _posix_lock(lock_path: Path):
    import fcntl

    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(lock_path, flags, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


@contextmanager
def _windows_lock(lock_path: Path):
    import msvcrt

    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        if os.fstat(fd).st_size == 0:
            os.write(fd, b"\0")
        deadline = time.monotonic() + _LOCK_WAIT_SECONDS
        while True:
            os.lseek(fd, 0, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise SafetyError("Timed out waiting for credentials lock") from None
                time.sleep(_LOCK_RETRY_SECONDS)
        try:
            yield
        finally:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    finally:
        os.close(fd)


@contextmanager
def _exclusive_file_lock(lock_path: Path):
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    deadline = time.monotonic() + _LOCK_WAIT_SECONDS
    while True:
        try:
            fd = os.open(lock_path, flags, 0o600)
            break
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise SafetyError("Timed out waiting for credentials lock") from None
            time.sleep(_LOCK_RETRY_SECONDS)
    try:
        os.write(fd, f"{os.getpid()}\n".encode("ascii"))
        os.close(fd)
        fd = -1
        yield
    finally:
        if fd >= 0:
            with suppress(OSError):
                os.close(fd)
        with suppress(FileNotFoundError):
            lock_path.unlink()


@contextmanager
def lock_file_guard(lock_path: Path):
    """Acquire an in-process and cross-process advisory lock at ``lock_path``."""
    _ensure_private_directory(lock_path.parent)
    if lock_path.is_symlink():
        raise SafetyError("Lock file must not be a symlink")
    with _thread_lock_for(lock_path):
        if os.name == "posix":
            with _posix_lock(lock_path):
                yield
            return
        if os.name == "nt":
            with _windows_lock(lock_path):
                yield
            return
        with _exclusive_file_lock(lock_path):
            yield


@contextmanager
def _credentials_lock(path: Path):
    with lock_file_guard(path.with_name(".credentials.lock")):
        yield


def load_or_import_account(
    provider: str,
    alias: str = "",
    *,
    browser: str | None = None,
    refresh: bool = False,
    path: Path = CREDENTIALS_FILE,
    require_ignored: bool = True,
) -> LiveAccount:
    provider = provider.casefold()
    if provider not in PROVIDERS:
        raise SafetyError("Select a supported provider before running live tests")
    if alias and not _ACCOUNT_ID.fullmatch(alias):
        raise SafetyError("Account IDs must be short local aliases using letters, digits, _ or -")
    if refresh and alias != "default-browser":
        raise SafetyError("Automatic cookie refresh is limited to the default-browser account")

    with _credentials_lock(path):
        if require_ignored:
            _require_ignored(path)
        try:
            path.lstat()
        except FileNotFoundError:
            data: dict[str, Any] = {"version": 1, "accounts": []}
        else:
            data = read_private_json(path, require_ignored=False)
        accounts = parse_accounts(data)
        matching = [item for item in accounts if item.provider == provider]
        replacing = False
        if alias:
            selected = [item for item in matching if item.alias == alias]
            if selected:
                if not refresh:
                    return select_account(accounts, provider, alias)
                replacing = True
            if matching and not selected:
                raise SafetyError("The selected provider account alias is not configured")
        elif matching:
            if len(matching) == 1:
                return matching[0]
            raise SafetyError("Select one provider account with WEBAI_LIVE_ACCOUNT")

        account_alias = alias or "default-browser"
        try:
            from webaiprovider.accounts.account import Account
            from webaiprovider.accounts.filters import ProviderCookieFilter

            account = Account.from_browser(
                provider,
                browser=browser,
                id=account_alias,
                name=f"{provider.capitalize()} default browser",
            )
            credentials = ProviderCookieFilter.filter_cookies(
                _PROVIDER_ORIGINS[provider], account.store().load()
            )
        except Exception as error:
            raise SafetyError(
                f"Could not import {provider} credentials from the default browser "
                f"({type(error).__name__}); details suppressed"
            ) from None
        if not isinstance(credentials, dict) or not credentials:
            raise SafetyError(f"No {provider} session cookies were found in the default browser")

        record = {"id": account_alias, "provider": provider, "credentials": credentials}
        if replacing:
            for index, existing in enumerate(data["accounts"]):
                if (
                    isinstance(existing, dict)
                    and existing.get("id") == account_alias
                    and str(existing.get("provider", "")).casefold() == provider
                ):
                    data["accounts"][index] = record
                    break
            else:
                raise SafetyError("The selected browser account changed during cookie refresh")
        else:
            data["accounts"].append(record)
        _write_private_json(path, data, require_ignored=require_ignored)
        return LiveAccount(account_alias, provider, credentials)
