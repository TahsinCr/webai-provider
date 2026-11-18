from __future__ import annotations

import asyncio
import json
import os
import threading
import time
import uuid
import weakref
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any, TypeVar

from .config import lock_file_guard
from .errors import SafetyError

T = TypeVar("T")

DEFAULT_ACTION_INTERVAL = 0.35
DEFAULT_SINGLE_API_LIMIT = 96
DEFAULT_COMBINED_RUN_LIMIT = 180


class ActionGate:
    """Serialize live calls per account, spacing them >= 1.0s and enforcing a total run cap."""

    def __init__(
        self,
        interval: float = DEFAULT_ACTION_INTERVAL,
        timeout: float = 300.0,
        limit: int = 20,
        *,
        state_file: Path | None = None,
    ):
        self.interval = max(float(interval), 0.0)
        self.timeout = timeout
        self.limit = limit
        self._local_calls = 0
        self._next_allowed = 0.0
        self._state_file = state_file
        self._sync_lock = threading.Lock()
        self._async_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
            weakref.WeakKeyDictionary()
        )

    @property
    def calls(self) -> int:
        if self._state_file is None:
            return self._local_calls
        with self._sync_lock:
            return int(self._read_shared_state()["calls"])

    def _read_shared_state(self) -> dict[str, Any]:
        assert self._state_file is not None
        if self._state_file.is_symlink():
            raise SafetyError("Shared live gate state must not be a symlink")
        try:
            raw = self._state_file.read_text(encoding="utf-8")
        except FileNotFoundError:
            return {"calls": 0, "next_allowed_epoch": 0.0}
        try:
            data = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise SafetyError("Shared live gate state is invalid") from None
        if (
            not isinstance(data, dict)
            or not isinstance(data.get("calls"), int)
            or isinstance(data.get("calls"), bool)
            or data["calls"] < 0
            or not isinstance(data.get("next_allowed_epoch"), (int, float))
            or isinstance(data.get("next_allowed_epoch"), bool)
        ):
            raise SafetyError("Shared live gate state is invalid")
        return data

    def _write_shared_state(self, state: dict[str, Any]) -> None:
        assert self._state_file is not None
        temporary = self._state_file.with_name(f"{self._state_file.name}.{uuid.uuid4().hex}.tmp")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(temporary, flags, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(state, stream, separators=(",", ":"))
            os.replace(temporary, self._state_file)
            if os.name == "posix":
                os.chmod(self._state_file, 0o600)
        finally:
            with suppress(FileNotFoundError):
                temporary.unlink()

    def _async_lock(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        with self._sync_lock:
            lock = self._async_locks.get(loop)
            if lock is None:
                lock = self._async_locks[loop] = asyncio.Lock()
            return lock

    def _reserve_slot(self) -> float:
        now_mono = time.monotonic()
        if self._state_file is None:
            if self._local_calls >= self.limit:
                raise SafetyError("The live run reached its fixed API-action limit")
            delay = max(self._next_allowed - now_mono, 0.0)
            self._local_calls += 1
            return delay

        gate_lock = self._state_file.with_name(f".{self._state_file.name}.lock")
        with lock_file_guard(gate_lock):
            state = self._read_shared_state()
            if state["calls"] >= self.limit:
                raise SafetyError("The live run reached its fixed API-action limit")
            now_epoch = time.time()
            delay = max(
                self._next_allowed - now_mono,
                float(state["next_allowed_epoch"]) - now_epoch,
                0.0,
            )
            state["calls"] += 1
            state["next_allowed_epoch"] = now_epoch + delay + self.interval
            self._write_shared_state(state)
            self._local_calls += 1
            return delay

    def _complete_slot(self) -> None:
        now_mono = time.monotonic()
        self._next_allowed = now_mono + self.interval
        if self._state_file is not None:
            gate_lock = self._state_file.with_name(f".{self._state_file.name}.lock")
            with lock_file_guard(gate_lock):
                state = self._read_shared_state()
                state["next_allowed_epoch"] = max(
                    float(state["next_allowed_epoch"]),
                    time.time() + self.interval,
                )
                self._write_shared_state(state)

    def _start(self) -> None:
        delay = self._reserve_slot()
        if delay > 0:
            deadline = time.monotonic() + delay
            while (remaining := deadline - time.monotonic()) > 0:
                time.sleep(remaining)

    def call_sync(self, action: Callable[[], T]) -> T:
        with self._sync_lock:
            self._start()
            try:
                return action()
            finally:
                self._complete_slot()

    async def call_async(self, action: Callable[[], Any]) -> Any:
        async with self._async_lock():
            with self._sync_lock:
                delay = self._reserve_slot()
            if delay > 0:
                deadline = time.monotonic() + delay
                while (remaining := deadline - time.monotonic()) > 0:
                    await asyncio.sleep(remaining)
            try:
                return await asyncio.wait_for(action(), timeout=self.timeout)
            finally:
                with self._sync_lock:
                    self._complete_slot()
