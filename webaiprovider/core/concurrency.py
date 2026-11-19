from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar, cast

from eventflow import KeyedConcurrencyLimiter, SingleFlight

T = TypeVar("T")
R = TypeVar("R")


async def maybe_await(value):
    return await value if inspect.isawaitable(value) else value


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
    pending = iter(enumerate(items))
    results: list[R | None] = [None] * len(items)

    async def run_worker() -> None:
        for index, item in pending:
            results[index] = await worker(item)

    tasks = [asyncio.create_task(run_worker()) for _ in range(min(len(items), max(1, limit)))]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    return cast(list[R], results)


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
