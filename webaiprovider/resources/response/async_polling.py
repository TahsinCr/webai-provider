from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

from ...errors import OperationCancelledError

_T = TypeVar("_T")
_DEFAULT_BACKOFF: tuple[float, ...] = (1.0, 2.0, 3.0, 5.0, 10.0)


async def async_poll_until(
    step: Callable[[], Awaitable[_T | None]],
    *,
    timeout: float,
    backoff: tuple[float, ...] = _DEFAULT_BACKOFF,
    is_cancelled: Callable[[], bool] | None = None,
) -> _T:
    """Poll asynchronously with adaptive backoff until step returns a non-None value or timeout."""
    deadline = time.monotonic() + timeout
    attempt = 0
    while True:
        if is_cancelled is not None and is_cancelled():
            raise OperationCancelledError("Operation cancelled")
        result = await step()
        if result is not None:
            return result
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Operation timed out after {timeout:.1f}s")
        delay = backoff[min(attempt, len(backoff) - 1)]
        attempt += 1
        await asyncio.sleep(min(delay, remaining))
