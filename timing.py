"""Opt-in, content-free provider send timing collection."""

from __future__ import annotations

import math
import os
import statistics
import threading
import time
from collections import deque
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Iterator

_TIMING_ENV = "SIOPECHAT_SEND_TIMINGS"
_current: ContextVar[SendTiming | None] = ContextVar("siopechat_send_timing", default=None)
_MAX_SAMPLES = 256
_samples: deque[SendTiming] = deque(maxlen=_MAX_SAMPLES)
_samples_lock = threading.Lock()


def timing_enabled() -> bool:
    return os.environ.get(_TIMING_ENV) == "1"


class SendTiming:
    """Keep only monotonic timestamps and named durations for one send."""

    def __init__(self, provider_id: str = "unknown") -> None:
        self.provider_id = provider_id
        self.started_ns = time.perf_counter_ns()
        self._durations: dict[str, float] = {}
        self._counts: dict[str, int] = {}
        self._lock = threading.Lock()

    def record_elapsed(self, name: str, start_ns: int) -> None:
        value = max(0, time.perf_counter_ns() - start_ns) / 1_000_000
        with self._lock:
            self._durations[name] = value

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


def record_send_timing(name: str, elapsed_ns: int) -> None:
    timing = current_send_timing()
    if timing is not None:
        timing.record(name, elapsed_ns)


def collect_send_timing(timing: SendTiming | None) -> None:
    if timing is None:
        return
    with _samples_lock:
        _samples.append(timing)


def send_timing_report(*, clear: bool = True) -> str:
    with _samples_lock:
        samples = [(sample.provider_id, sample.snapshot()) for sample in _samples]
        if clear:
            _samples.clear()
    if not samples:
        return ""

    providers = sorted({provider_id for provider_id, _ in samples})
    rows = [f"SiopeChat provider send timings; sends={len(samples)}; milliseconds"]
    rows.append("provider | stage | samples | median | p95")
    for provider_id in providers:
        durations = [sample[0] for name, sample in samples if name == provider_id]
        names = sorted({name for sample in durations for name in sample})
        for name in names:
            values = sorted(sample[name] for sample in durations if name in sample)
            p95 = values[max(0, math.ceil(0.95 * len(values)) - 1)]
            rows.append(
                f"{provider_id} | {name} | {len(values)} | "
                f"{statistics.median(values):.3f} | {p95:.3f}"
            )
    counts_by_provider: dict[str, dict[str, int]] = {}
    for provider_id, (_, counts) in samples:
        totals = counts_by_provider.setdefault(provider_id, {})
        for name, value in counts.items():
            totals[name] = totals.get(name, 0) + value
    if any(counts_by_provider.values()):
        rows.extend(("", "provider | path count | total"))
        for provider_id, counts in sorted(counts_by_provider.items()):
            rows.extend(
                f"{provider_id} | {name} | {value}"
                for name, value in sorted(counts.items())
            )
    return "\n".join(rows)
