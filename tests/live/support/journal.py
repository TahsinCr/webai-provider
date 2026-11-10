from __future__ import annotations

import json
import os
import re
import stat
import uuid
from pathlib import Path
from typing import Any

from .config import CONFIG_DIR, _private_directory, read_private_json
from .errors import SafetyError

CLEANUP_FILE = CONFIG_DIR / "live-cleanup.json"
LOCK_FILE = CONFIG_DIR / "live-run.lock"
_ACCOUNT_ID = re.compile(r"^[A-Za-z0-9_-]{1,48}$")
_RUN_ID = re.compile(r"^webai-live-[0-9a-f]{32}$")
_PROVIDERS = {"chatgpt", "gemini", "claude", "grok"}


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
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def write_cleanup_state(state: dict[str, Any]) -> None:
    write_private_json(CLEANUP_FILE, state)


def read_cleanup_state() -> dict[str, Any] | None:
    try:
        CLEANUP_FILE.lstat()
    except FileNotFoundError:
        return None
    data = read_private_json(CLEANUP_FILE, require_ignored=True)
    run_id = data.get("run_id") if isinstance(data, dict) else None
    resources = data.get("resources") if isinstance(data, dict) else None
    if (
        not isinstance(data, dict)
        or data.get("version") != 1
        or data.get("provider") not in _PROVIDERS
        or not isinstance(data.get("account"), str)
        or not _ACCOUNT_ID.fullmatch(data["account"])
        or not isinstance(run_id, str)
        or not _RUN_ID.fullmatch(run_id)
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
        kind, markers = item.get("kind"), item.get("markers")
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
                "Another live test or stale lock exists; inspect .config/live-run.lock"
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
