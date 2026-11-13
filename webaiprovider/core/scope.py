from __future__ import annotations

from collections.abc import Callable
from types import TracebackType

from eventflow import AsyncResourceScope


class ProviderResourceScope:
    __slots__ = ("_resources",)

    def __init__(self) -> None:
        self._resources = AsyncResourceScope()

    def defer(self, cleanup: Callable[[], object]) -> None:
        self._resources.defer(cleanup)

    async def aclose(self) -> None:
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
