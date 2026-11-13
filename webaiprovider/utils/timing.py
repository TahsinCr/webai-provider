"""Opt-in, content-free provider send timing collection."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token

_current: ContextVar[SendTiming | None] = ContextVar("webaiprovider_send_timing", default=None)


class SendTiming:
    """Keep only monotonic timestamps and named durations for one send."""

    __slots__ = ("provider_id", "started_ns", "_durations", "_counts", "_lock")

    def __init__(self, provider_id: str = "unknown") -> None:
        self.provider_id = provider_id
        self.started_ns = time.perf_counter_ns()
        self._durations: dict[str, float] = {}
        self._counts: dict[str, int] = {}
        self._lock = threading.Lock()

    def record(self, name: str, elapsed_ns: int) -> None:
        with self._lock:
            self._durations[name] = max(0, elapsed_ns) / 1_000_000

    def increment(self, name: str) -> None:
        with self._lock:
            self._counts[name] = self._counts.get(name, 0) + 1

    def snapshot(self) -> tuple[dict[str, float], dict[str, int]]:
        with self._lock:
            return dict(self._durations), dict(self._counts)


@contextmanager
def use_send_timing(timing: SendTiming | None) -> Iterator[None]:
    token: Token[SendTiming | None] = _current.set(timing)
    try:
        yield
    finally:
        _current.reset(token)


def current_send_timing() -> SendTiming | None:
    return _current.get()
