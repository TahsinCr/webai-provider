import math
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
