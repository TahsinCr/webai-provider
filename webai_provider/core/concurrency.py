from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from eventflow import KeyedConcurrencyLimiter, SingleFlight

T = TypeVar("T")


class ConcurrencyCoordinator:
    __slots__ = ("_limiter", "_single_flight")

    def __init__(self, max_concurrency_per_key: int = 3) -> None:
        self._limiter = KeyedConcurrencyLimiter(max_concurrency=max_concurrency_per_key)
        self._single_flight: SingleFlight[Any, Any] = SingleFlight()

    def acquire(self, key: Any) -> Any:
        return self._limiter.acquire(key)

    async def run_keyed(
        self,
        key: Any,
        operation: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        async with self._limiter.acquire(key):
            return await operation(*args, **kwargs)

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
