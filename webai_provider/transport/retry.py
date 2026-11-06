import math
import random
import time
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import Any


def parse_retry_after(value: Any, *, now: float | None = None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if math.isfinite(value) and value >= 0:
            return float(value)
        return None
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        seconds = float(stripped)
        if math.isfinite(seconds) and seconds >= 0:
            return seconds
        return None
    except (ValueError, OverflowError):
        pass
    try:
        retry_at = parsedate_to_datetime(stripped)
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        current_time = time.time() if now is None else now
        delay = retry_at.timestamp() - current_time
        if math.isfinite(delay) and delay >= 0:
            return delay
        return 0.0 if math.isfinite(delay) else None
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def calculate_backoff(
    attempt: int,
    base: float = 1.0,
    max_backoff: float = 60.0,
    jitter: bool = True,
) -> float:
    attempt = max(0, attempt)
    delay = min(max_backoff, base * (2**attempt))
    if jitter:
        delay = delay * (0.5 + random.random() * 0.5)
    return max(0.0, delay)


class RetryPolicy:
    def __init__(
        self,
        max_attempts: int = 3,
        base_backoff: float = 1.0,
        max_backoff: float = 60.0,
        retryable_statuses: tuple[int, ...] = (429, 502, 503, 504),
    ):
        self.max_attempts = max_attempts
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self.retryable_statuses = retryable_statuses

    def should_retry(self, status_code: int, attempt: int, method: str = "GET") -> bool:
        if attempt >= self.max_attempts:
            return False
        if method.upper() not in ("GET", "HEAD", "OPTIONS") and status_code != 429:
            return False
        return status_code in self.retryable_statuses

    def backoff_for(self, attempt: int, retry_after: float | None = None) -> float:
        if retry_after is not None and retry_after > 0:
            return min(self.max_backoff, retry_after)
        return calculate_backoff(attempt, self.base_backoff, self.max_backoff)
