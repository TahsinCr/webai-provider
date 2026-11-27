from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import TypeVar

from ...errors import OperationCancelledError

_T = TypeVar("_T")
_DEFAULT_BACKOFF: tuple[float, ...] = (1.0, 2.0, 3.0, 5.0, 10.0)


def poll_until(
    step: Callable[[], _T | None],
    *,
    timeout: float,
    backoff: tuple[float, ...] = _DEFAULT_BACKOFF,
    cancel_event: threading.Event | None = None,
    is_cancelled: Callable[[], bool] | None = None,
) -> _T:
    """Poll synchronously with adaptive backoff until step returns a non-None value or timeout."""
    deadline = time.monotonic() + timeout
    attempt = 0
    while True:
        if (cancel_event is not None and cancel_event.is_set()) or (
            is_cancelled is not None and is_cancelled()
        ):
            raise OperationCancelledError("Operation cancelled")
        result = step()
        if result is not None:
            return result
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Operation timed out after {timeout:.1f}s")
        delay = backoff[min(attempt, len(backoff) - 1)]
        attempt += 1
        sleep_for = min(delay, remaining)
        if cancel_event is not None:
            if cancel_event.wait(sleep_for):
                raise OperationCancelledError("Operation cancelled")
        else:
            time.sleep(sleep_for)
