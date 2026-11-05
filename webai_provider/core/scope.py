from __future__ import annotations

from collections.abc import Callable, Coroutine
from types import TracebackType
from typing import Any, TypeVar

from eventflow import AsyncResourceScope, AsyncTaskScope

T = TypeVar("T")


class ProviderResourceScope:
    __slots__ = ("_resources", "_tasks")

    def __init__(self) -> None:
        self._resources = AsyncResourceScope()
        self._tasks = AsyncTaskScope()

    def defer(self, cleanup: Callable[[], object]) -> None:
        self._resources.defer(cleanup)

    def spawn(self, coro: Coroutine[Any, Any, T], *, name: str | None = None) -> Any:
        return self._tasks.start(coro, name=name)

    def cancel(self) -> None:
        self._tasks.cancel()

    async def aclose(self) -> None:
        try:
            self._tasks.cancel()
            await self._tasks.aclose()
        finally:
            await self._resources.aclose()

    async def __aenter__(self) -> ProviderResourceScope:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.aclose()
