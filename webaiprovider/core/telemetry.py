from __future__ import annotations

from collections import deque
from statistics import median
from typing import Any

from eventflow import OperationMetrics, correlation_context, current_correlation_id


class ProviderTelemetry:
    __slots__ = (
        "_calls",
        "_successful",
        "_failed",
        "_cancelled",
        "_total_seconds",
        "_max_seconds",
        "_ttft_records",
        "_sync_event_drops",
    )

    def __init__(self) -> None:
        self._calls: int = 0
        self._successful: int = 0
        self._failed: int = 0
        self._cancelled: int = 0
        self._total_seconds: float = 0.0
        self._max_seconds: float = 0.0
        self._ttft_records: deque[float] = deque(maxlen=256)
        self._sync_event_drops: int = 0

    def record_call(self) -> None:
        self._calls += 1

    def record_success(self, duration_seconds: float) -> None:
        self._successful += 1
        dur = max(0.0, duration_seconds)
        self._total_seconds += dur
        if dur > self._max_seconds:
            self._max_seconds = dur

    def record_failure(self, duration_seconds: float) -> None:
        self._failed += 1
        dur = max(0.0, duration_seconds)
        self._total_seconds += dur
        if dur > self._max_seconds:
            self._max_seconds = dur

    def record_cancelled(self) -> None:
        self._cancelled += 1

    def record_ttft(self, seconds: float) -> None:
        if seconds >= 0.0:
            self._ttft_records.append(seconds)

    def record_sync_event_drop(self) -> None:
        self._sync_event_drops += 1

    @property
    def sync_event_drops(self) -> int:
        return self._sync_event_drops

    @property
    def median_ttft(self) -> float | None:
        if not self._ttft_records:
            return None
        return float(median(self._ttft_records))

    def metrics(self) -> OperationMetrics:
        return OperationMetrics(
            calls=self._calls,
            successful=self._successful,
            failed=self._failed,
            cancelled=self._cancelled,
            total_seconds=self._total_seconds,
            max_seconds=self._max_seconds,
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "calls": self._calls,
            "successful": self._successful,
            "failed": self._failed,
            "cancelled": self._cancelled,
            "total_seconds": self._total_seconds,
            "max_seconds": self._max_seconds,
            "median_ttft": self.median_ttft,
            "sync_event_drops": self._sync_event_drops,
        }


__all__ = [
    "OperationMetrics",
    "ProviderTelemetry",
    "correlation_context",
    "current_correlation_id",
]
