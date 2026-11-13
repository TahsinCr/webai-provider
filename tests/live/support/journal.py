from __future__ import annotations

import json
import os
import re
import stat
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Any

from .config import CONFIG_DIR, _private_directory, read_private_json
from .errors import SafetyError

CLEANUP_FILE = CONFIG_DIR / "live-cleanup.json"
LOCK_FILE = CONFIG_DIR / "live-run.lock"
_DEFAULT_CLEANUP_FILE = CLEANUP_FILE
_DEFAULT_LOCK_FILE = LOCK_FILE
_ACCOUNT_ID = re.compile(r"^[A-Za-z0-9_-]{1,48}$")
_RUN_ID = re.compile(r"^webai-live-[0-9a-f]{32}$")
_PROVIDERS = {"chatgpt", "gemini", "claude", "grok"}
_REQUIRED_RESOURCE_KEYS = {"kind", "markers", "remote_id", "attempted"}
_OPTIONAL_RESOURCE_KEYS = {"exclude_ids"}


def cleanup_file(provider: str | None = None) -> Path:
    if CLEANUP_FILE != _DEFAULT_CLEANUP_FILE:
        return CLEANUP_FILE
    selected = (provider or os.environ.get("WEBAI_LIVE_PROVIDER", "")).casefold()
    if selected in _PROVIDERS:
        return CONFIG_DIR / f"live-cleanup-{selected}.json"
    return CLEANUP_FILE


def lock_file(provider: str | None = None) -> Path:
    if LOCK_FILE != _DEFAULT_LOCK_FILE:
        return LOCK_FILE
    selected = (provider or os.environ.get("WEBAI_LIVE_PROVIDER", "")).casefold()
    if selected in _PROVIDERS:
        return CONFIG_DIR / f"live-run-{selected}.lock"
    return LOCK_FILE


def pending_cleanup_files() -> list[Path]:
    if CLEANUP_FILE != _DEFAULT_CLEANUP_FILE:
        return [CLEANUP_FILE] if CLEANUP_FILE.exists() or CLEANUP_FILE.is_symlink() else []
    candidates = [
        CLEANUP_FILE,
        *(CONFIG_DIR / f"live-cleanup-{p}.json" for p in sorted(_PROVIDERS)),
    ]
    return [path for path in candidates if path.exists() or path.is_symlink()]


def _contains_credentials(value: Any) -> bool:
    if isinstance(value, dict):
        return any(
            key == "credentials" or _contains_credentials(item) for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_credentials(item) for item in value)
    return False


def write_private_json(path: Path, data: dict[str, Any]) -> None:
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
        with suppress(FileNotFoundError):
            temporary.unlink()


def write_cleanup_state(
    state: dict[str, Any], *, provider: str | None = None, path: Path | None = None
) -> None:
    _validate_cleanup_state(state)
    target = path or cleanup_file(provider or state.get("provider"))
    write_private_json(target, state)


def _validate_cleanup_state(data: Any) -> dict[str, Any]:
    run_id = data.get("run_id") if isinstance(data, dict) else None
    resources = data.get("resources") if isinstance(data, dict) else None
    if (
        not isinstance(data, dict)
        or data.get("version") != 1
        or not isinstance(data.get("provider"), str)
        or data["provider"] not in _PROVIDERS
        or not isinstance(data.get("account"), str)
        or not _ACCOUNT_ID.fullmatch(data["account"])
        or not isinstance(run_id, str)
        or not _RUN_ID.fullmatch(run_id)
        or not isinstance(resources, list)
        or len(resources) > 8
        or set(data) != {"version", "provider", "account", "run_id", "resources"}
    ):
        raise SafetyError("Cleanup journal is invalid; no new live tests were started")
    for item in resources:
        if not isinstance(item, dict):
            raise SafetyError("Cleanup journal contains an invalid resource record")
        keys = set(item)
        if not _REQUIRED_RESOURCE_KEYS.issubset(keys) or not keys.issubset(
            _REQUIRED_RESOURCE_KEYS | _OPTIONAL_RESOURCE_KEYS
        ):
            raise SafetyError("Cleanup journal contains an invalid resource record")
        kind, markers = item.get("kind"), item.get("markers")
        if not isinstance(kind, str) or kind not in {"chat", "project"}:
            raise SafetyError("Cleanup journal contains an invalid resource record")
        if (
            not isinstance(markers, list)
            or not markers
            or any(
                not isinstance(marker, str)
                or not marker.startswith(f"{run_id}-")
                or len(marker) > 96
                for marker in markers
            )
            or not isinstance(item.get("attempted"), bool)
            or (item.get("remote_id") is not None and not isinstance(item["remote_id"], str))
        ):
            raise SafetyError("Cleanup journal contains an invalid resource record")
        if "exclude_ids" in item:
            exclude_ids = item["exclude_ids"]
            if (
                not isinstance(exclude_ids, list)
                or len(exclude_ids) > 16
                or any(not isinstance(eid, str) or not eid for eid in exclude_ids)
            ):
                raise SafetyError("Cleanup journal contains an invalid resource record")
    return data


def _read_journal_file(path: Path) -> dict[str, Any] | None:
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    return _validate_cleanup_state(read_private_json(path, require_ignored=True))


def cleanup_journal_provider(path: Path) -> str | None:
    """Return the validated owning provider for a pending journal file, or raise SafetyError."""
    data = _read_journal_file(path)
    return data["provider"] if data is not None else None


def read_cleanup_journal(provider: str | None = None) -> tuple[dict[str, Any] | None, Path | None]:
    if CLEANUP_FILE != _DEFAULT_CLEANUP_FILE:
        data = _read_journal_file(CLEANUP_FILE)
        return (data, CLEANUP_FILE) if data is not None else (None, None)

    selected = (provider or os.environ.get("WEBAI_LIVE_PROVIDER", "")).casefold()
    if selected in _PROVIDERS:
        provider_path = CONFIG_DIR / f"live-cleanup-{selected}.json"
        data = _read_journal_file(provider_path)
        if data is not None:
            if data["provider"] != selected:
                raise SafetyError("Cleanup journal provider does not match the selected provider")
            return data, provider_path

    legacy_data = _read_journal_file(CLEANUP_FILE)
    if legacy_data is not None:
        if selected in _PROVIDERS and legacy_data["provider"] != selected:
            return None, None
        return legacy_data, CLEANUP_FILE
    return None, None


def read_cleanup_state(provider: str | None = None) -> dict[str, Any] | None:
    state, _ = read_cleanup_journal(provider)
    return state


class RunLock:
    def __init__(self, provider: str | None = None) -> None:
        self._identity: tuple[int, int] | None = None
        self._path = lock_file(provider)

    def __enter__(self) -> RunLock:
        _private_directory(CONFIG_DIR)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(self._path, flags, 0o600)
        except FileExistsError:
            raise SafetyError(
                f"Another live test or stale lock exists; inspect {self._path}"
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
            info = self._path.lstat()
            if (
                stat.S_ISREG(info.st_mode)
                and (os.name != "posix" or info.st_uid == os.getuid())
                and self._identity == (info.st_dev, info.st_ino)
            ):
                self._path.unlink()
        except FileNotFoundError:
            pass
