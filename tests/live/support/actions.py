from __future__ import annotations

import asyncio
import threading
import time
import weakref
from collections.abc import Callable
from typing import Any, TypeVar

from .errors import SafetyError

T = TypeVar("T")


class ActionGate:
    """Rate-limits public API calls and bounds one live test run."""

    def __init__(self, interval: float = 1.0, timeout: float = 300.0, limit: int = 20):
        self.interval = interval
        self.timeout = timeout
        self.limit = limit
        self.calls = 0
        self._next_allowed = 0.0
        self._sync_lock = threading.Lock()
        self._async_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
            weakref.WeakKeyDictionary()
        )

    def _async_lock(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        with self._sync_lock:
            lock = self._async_locks.get(loop)
            if lock is None:
                lock = self._async_locks[loop] = asyncio.Lock()
            return lock

    def _start(self) -> None:
        if self.calls >= self.limit:
            raise SafetyError("The live run reached its fixed API-action limit")
        delay = self._next_allowed - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        self.calls += 1

    def call_sync(self, action: Callable[[], T]) -> T:
        with self._sync_lock:
            self._start()
            try:
                return action()
            finally:
                self._next_allowed = time.monotonic() + self.interval

    async def call_async(self, action: Callable[[], Any]) -> Any:
        async with self._async_lock():
            with self._sync_lock:
                if self.calls >= self.limit:
                    raise SafetyError("The live run reached its fixed API-action limit")
                delay = self._next_allowed - time.monotonic()
                self.calls += 1
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                return await asyncio.wait_for(action(), timeout=self.timeout)
            finally:
                with self._sync_lock:
                    self._next_allowed = time.monotonic() + self.interval
