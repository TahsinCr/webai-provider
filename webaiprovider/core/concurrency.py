from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar, cast

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
