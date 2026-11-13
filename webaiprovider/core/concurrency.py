from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar

from eventflow import KeyedConcurrencyLimiter, SingleFlight

T = TypeVar("T")
R = TypeVar("R")


async def gather_bounded(
    items: Sequence[T],
    worker: Callable[[T], Awaitable[R]],
    *,
    limit: int = 2,
) -> list[R]:
    if not items:
        return []
    if len(items) == 1:
        return [await worker(items[0])]
    semaphore = asyncio.Semaphore(max(1, limit))

    async def run_one(item: T) -> R:
        async with semaphore:
            return await worker(item)

    tasks = [asyncio.create_task(run_one(item)) for item in items]
    try:
        return list(await asyncio.gather(*tasks))
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


class ConcurrencyCoordinator:
    __slots__ = ("_limiter", "_single_flight")

    def __init__(self, max_concurrency_per_key: int = 3) -> None:
        self._limiter = KeyedConcurrencyLimiter(max_concurrency=max_concurrency_per_key)
        self._single_flight: SingleFlight[Any, Any] = SingleFlight()

    def acquire(self, key: Any) -> Any:
        return self._limiter.acquire(key)

    async def deduplicate(self, key: Any, factory: Callable[[], Awaitable[T]]) -> T:
        return await self._single_flight.run(key, factory)

    def stats(self) -> dict[str, Any]:
        return {
            "concurrency": self._limiter.stats(),
            "single_flight": self._single_flight.stats(),
        }

    async def aclose(self) -> None:
        await self._limiter.aclose()
        await self._single_flight.aclose()
