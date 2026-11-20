from __future__ import annotations

import json
import os
import re
import stat
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Any

from .config import CONFIG_DIR, PROVIDERS, _private_directory, read_private_json
from .errors import SafetyError

_ACCOUNT_ID = re.compile(r"^[A-Za-z0-9_-]{1,48}$")
_RUN_ID = re.compile(r"^webai-live-[0-9a-f]{32}$")
_REQUIRED_RESOURCE_KEYS = {"kind", "markers", "remote_id", "attempted"}
_OPTIONAL_RESOURCE_KEYS = {"exclude_ids"}


def _provider(provider: str | None) -> str:
    selected = (provider or os.environ.get("WEBAI_LIVE_PROVIDER", "")).casefold()
    return selected if selected in PROVIDERS else ""


def cleanup_file(provider: str | None = None) -> Path:
    selected = _provider(provider)
    return CONFIG_DIR / (f"live-cleanup-{selected}.json" if selected else "live-cleanup.json")


def lock_file(provider: str | None = None) -> Path:
    selected = _provider(provider)
    return CONFIG_DIR / (f"live-run-{selected}.lock" if selected else "live-run.lock")


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
        or data["provider"] not in PROVIDERS
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


def read_cleanup_journal(provider: str | None = None) -> tuple[dict[str, Any] | None, Path | None]:
    selected = _provider(provider)
    path = cleanup_file(provider)
    data = _read_journal_file(path)
    if data is not None:
        if selected and data["provider"] != selected:
            raise SafetyError("Cleanup journal provider does not match the selected provider")
        return data, path

    if selected:
        shared_path = CONFIG_DIR / "live-cleanup.json"
        data = _read_journal_file(shared_path)
        if data is not None and data["provider"] == selected:
            return data, shared_path
    return None, None


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
