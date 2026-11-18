import asyncio
import json
import os
import stat
import tempfile
import threading
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from weakref import WeakValueDictionary

from ..errors import AccountPaused


@runtime_checkable
class CredentialStore(Protocol):
    def load(self) -> dict: ...
    def save(self, value: dict) -> None: ...
    def exists(self) -> bool: ...
    def delete(self) -> None: ...


class _WeakLock:
    """Weakref-capable mutex wrapper for per-path write serialization."""

    __slots__ = ("_lock", "__weakref__")

    def __init__(self) -> None:
        self._lock = threading.Lock()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        return self._lock.acquire(blocking, timeout)

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> "_WeakLock":
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._lock.release()


_SESSION_WRITE_LOCKS: WeakValueDictionary[str, _WeakLock] = WeakValueDictionary()
_SESSION_WRITE_LOCKS_GUARD = threading.Lock()


def _session_write_lock(path: Path | str) -> _WeakLock:
    key = os.path.normcase(os.path.abspath(str(path)))
    with _SESSION_WRITE_LOCKS_GUARD:
        lock = _SESSION_WRITE_LOCKS.get(key)
        if lock is None:
            lock = _WeakLock()
            _SESSION_WRITE_LOCKS[key] = lock
        return lock


def _replace_with_retry(source: Path | str, destination: Path | str) -> None:
    for attempt in range(5):
        try:
            os.replace(source, destination)
            return
        except PermissionError:
            if os.name != "nt" or attempt == 4:
                raise
            time.sleep(0.01 * (2**attempt))


class FileCredentialStore:
    def __init__(self, path: Path | str):
        self.path = Path(path)

    def exists(self) -> bool:
        return self.path.is_file()

    def delete(self) -> None:
        self.path.unlink(missing_ok=True)

    def load(self) -> dict:
        try:
            with open(self.path, encoding="utf-8") as handle:
                info = os.fstat(handle.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size > 1_048_576:
                    raise ValueError("session size")
                raw = handle.read(1_048_577)
                if len(raw) > 1_048_576:
                    raise ValueError("session size")
                value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError("session shape")
            return value
        except (OSError, ValueError):
            raise AccountPaused(
                "No saved session was found or the session is invalid; verify it in the Accounts section."
            ) from None

    def save(self, value: dict, *, ensure_ascii: bool = False, newline: bool = False) -> None:
        if not isinstance(value, dict):
            raise ValueError("session shape must be dict")
        with _session_write_lock(self.path):
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            name = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w", encoding="utf-8", dir=self.path.parent, delete=False
                ) as output:
                    name = output.name
                    if os.name != "nt":
                        os.chmod(name, 0o600)
                    json.dump(value, output, ensure_ascii=ensure_ascii)
                    if newline:
                        output.write("\n")
                    output.flush()
                    os.fsync(output.fileno())
                _replace_with_retry(name, self.path)
            finally:
                if name and os.path.exists(name):
                    os.unlink(name)


class MemoryCredentialStore:
    def __init__(self, initial: dict | None = None):
        self._lock = threading.Lock()
        self._value = deepcopy(initial) if initial is not None else None

    def exists(self) -> bool:
        with self._lock:
            return self._value is not None

    def delete(self) -> None:
        with self._lock:
            self._value = None

    def load(self) -> dict:
        with self._lock:
            if self._value is None:
                raise AccountPaused("No valid saved session; verify the account")
            return deepcopy(self._value)

    def save(self, value: dict) -> None:
        if not isinstance(value, dict):
            raise ValueError("session shape must be dict")
        with self._lock:
            self._value = deepcopy(value)


def session_exists(source: Any) -> bool:
    return source.exists() if isinstance(source, CredentialStore) else Path(source).is_file()


def delete_session(source: Any) -> None:
    if isinstance(source, CredentialStore):
        source.delete()
    else:
        Path(source).unlink(missing_ok=True)


def read_session(path: Any) -> dict:
    if isinstance(path, CredentialStore):
        try:
            value = path.load()
            if not isinstance(value, dict):
                raise ValueError("session shape")
            return value
        except (OSError, ValueError):
            raise AccountPaused("No valid saved session; verify the account") from None
    return FileCredentialStore(path).load()


async def read_session_async(path: Any) -> dict:
    if isinstance(path, MemoryCredentialStore):
        return path.load()
    return await asyncio.to_thread(read_session, path)


def save_session(
    path: Any, value: dict, *, ensure_ascii: bool = False, newline: bool = False
) -> None:
    if isinstance(path, CredentialStore):
        path.save(value)
        return
    FileCredentialStore(path).save(value, ensure_ascii=ensure_ascii, newline=newline)


async def save_session_async(
    path: Any, value: dict, *, ensure_ascii: bool = False, newline: bool = False
) -> None:
    kwargs = {"ensure_ascii": ensure_ascii, "newline": newline} if ensure_ascii or newline else {}
    worker = asyncio.create_task(asyncio.to_thread(save_session, path, value, **kwargs))
    try:
        await asyncio.shield(worker)
    except asyncio.CancelledError:
        await asyncio.gather(worker, return_exceptions=True)
        raise
