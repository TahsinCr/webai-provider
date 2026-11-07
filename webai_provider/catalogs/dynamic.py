from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from ..contracts import EffortRef, ModelRef, ModelSelection
from ..errors import RemoteNotFound
from .policies import Models
from .resolver import SelectionResolver


class ModelsCollection:
    def __init__(self, loader: Callable[[bool], Awaitable[list[ModelRef]]]) -> None:
        self._loader = loader
        self._items: tuple[ModelRef, ...] | None = None
        self._lock = asyncio.Lock()

    async def list(self, *, refresh: bool = False) -> list[ModelRef]:
        """Load once to prewarm the account cache; refresh forces a provider fetch."""
        async with self._lock:
            if self._items is None or refresh:
                loaded = await self._loader(refresh)
                self._items = tuple(loaded)
            return list(self._items)

    async def refresh(self) -> list[ModelRef]:
        """Refresh the account catalog and bypass provider-side cache entries."""
        return await self.list(refresh=True)

    async def get(self, model_id: str) -> ModelRef:
        models = await self.list()
        for model in models:
            if model.id == model_id:
                return model
        raise RemoteNotFound(f"Model ID {model_id!r} is not available for this account")

    async def find(self, query: str) -> ModelRef:
        result = SelectionResolver.resolve(query, await self.list())
        if not isinstance(result, ModelRef):
            raise TypeError("Model query did not resolve to a model")
        return result

    async def search(self, query: str) -> list[ModelRef]:
        query = query.casefold()
        return [
            model
            for model in await self.list()
            if query in model.id.casefold() or query in model.name.casefold()
        ]

    async def resolve(
        self,
        target: Models | EffortRef | ModelRef | ModelSelection | str,
    ) -> ModelSelection:
        result = SelectionResolver.resolve(target, await self.list())
        if isinstance(result, ModelRef):
            return ModelSelection(result)
        if isinstance(result, ModelSelection):
            return result
        raise TypeError("Selection did not resolve to a model")
