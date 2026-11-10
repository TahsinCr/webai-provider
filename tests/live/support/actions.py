from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from typing import Any, TypeVar

from .errors import SafetyError

T = TypeVar("T")


class ActionGate:
    """Rate-limits public API calls and bounds one live test run."""

    def __init__(self, interval: float = 1.0, timeout: float = 90.0, limit: int = 20):
        self.interval = interval
        self.timeout = timeout
        self.limit = limit
        self.calls = 0
        self._next_allowed = 0.0
        self._async_lock = asyncio.Lock()
        self._sync_lock = threading.Lock()

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
        async with self._async_lock:
            if self.calls >= self.limit:
                raise SafetyError("The live run reached its fixed API-action limit")
            delay = self._next_allowed - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self.calls += 1
            try:
                return await asyncio.wait_for(action(), timeout=self.timeout)
            finally:
                self._next_allowed = time.monotonic() + self.interval
