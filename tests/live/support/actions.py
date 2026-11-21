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
from typing import Any

from webaiprovider.errors import RateLimited

from .config import lock_file_guard
from .errors import SafetyError

DEFAULT_ACTION_INTERVAL = 0.15
DEFAULT_SINGLE_API_LIMIT = 96
DEFAULT_COMBINED_RUN_LIMIT = 180


class ActionGate:
    """Serialize live calls per account and enforce pacing and a total run cap."""

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
        self._adaptive_penalty = 0.0
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

    def _effective_interval(self) -> float:
        if self._adaptive_penalty > 0:
            return self.interval + self._adaptive_penalty
        return self.interval

    def _record_outcome(self, *, rate_limited: RateLimited | None = None) -> None:
        if rate_limited is not None:
            retry = rate_limited.retry_after
            capped = (
                min(float(retry), 5.0) if isinstance(retry, (int, float)) and retry > 0 else 1.5
            )
            self._adaptive_penalty = max(self.interval, capped)
        elif self._adaptive_penalty > 0:
            self._adaptive_penalty = (
                0.0 if self._adaptive_penalty < 0.05 else self._adaptive_penalty * 0.5
            )

    def _reserve_slot(self, *, allow_over_limit: bool = False) -> float:
        now_mono = time.monotonic()
        interval = self._effective_interval()
        if self._state_file is None:
            if not allow_over_limit and self._local_calls >= self.limit:
                raise SafetyError("The live run reached its fixed API-action limit")
            delay = max(self._next_allowed - now_mono, 0.0)
            self._local_calls += 1
            return delay

        gate_lock = self._state_file.with_name(f".{self._state_file.name}.lock")
        with lock_file_guard(gate_lock):
            state = self._read_shared_state()
            if not allow_over_limit and state["calls"] >= self.limit:
                raise SafetyError("The live run reached its fixed API-action limit")
            now_epoch = time.time()
            delay = max(
                self._next_allowed - now_mono,
                float(state["next_allowed_epoch"]) - now_epoch,
                0.0,
            )
            state["calls"] += 1
            state["next_allowed_epoch"] = now_epoch + delay + interval
            self._write_shared_state(state)
            self._local_calls += 1
            return delay

    def _complete_slot(self) -> None:
        now_mono = time.monotonic()
        interval = self._effective_interval()
        self._next_allowed = now_mono + interval
        if self._state_file is not None:
            gate_lock = self._state_file.with_name(f".{self._state_file.name}.lock")
            with lock_file_guard(gate_lock):
                state = self._read_shared_state()
                state["next_allowed_epoch"] = max(
                    float(state["next_allowed_epoch"]),
                    time.time() + interval,
                )
                self._write_shared_state(state)

    def call(self, action: Callable[[], Any], *, bypass_limit: bool = False) -> Any:
        with self._sync_lock:
            delay = self._reserve_slot(allow_over_limit=bypass_limit)
        if delay > 0:
            time.sleep(delay)
        try:
            result = action()
        except RateLimited as exc:
            with self._sync_lock:
                self._record_outcome(rate_limited=exc)
            raise
        else:
            with self._sync_lock:
                self._record_outcome()
            return result
        finally:
            with self._sync_lock:
                self._complete_slot()

    async def call_async(self, action: Callable[[], Any], *, bypass_limit: bool = False) -> Any:
        async with self._async_lock():
            with self._sync_lock:
                delay = self._reserve_slot(allow_over_limit=bypass_limit)
            if delay > 0:
                deadline = time.monotonic() + delay
                while (remaining := deadline - time.monotonic()) > 0:
                    await asyncio.sleep(remaining)
            try:
                result = await asyncio.wait_for(action(), timeout=self.timeout)
            except RateLimited as exc:
                with self._sync_lock:
                    self._record_outcome(rate_limited=exc)
                raise
            else:
                with self._sync_lock:
                    self._record_outcome()
                return result
            finally:
                with self._sync_lock:
                    self._complete_slot()
