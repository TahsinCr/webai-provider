from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from ..collections.base import AsyncQueryableCollection, QueryableCollection
from ..contracts import EffortRef, ModelRef, ModelSelection
from ..errors import RemoteNotFound
from .policies import Models
from .resolver import SelectionResolver


class ModelsCollection(AsyncQueryableCollection[ModelRef]):
    def __init__(self, loader: Callable[[bool], Awaitable[list[ModelRef]]]) -> None:
        self._loader = loader
        self._items_tuple: tuple[ModelRef, ...] | None = None
        self._lock = asyncio.Lock()

    async def list(self, *, refresh: bool = False) -> list[ModelRef]:
        """Load once to prewarm the account cache; refresh forces a provider fetch."""
        async with self._lock:
            if self._items_tuple is None or refresh:
                loaded = await self._loader(refresh)
                self._items_tuple = tuple(loaded)
            return list(self._items_tuple)

    async def refresh(self) -> list[ModelRef]:
        """Refresh the account catalog and bypass provider-side cache entries."""
        return await self.list(refresh=True)

    async def get(self, model_id: str, /) -> ModelRef:
        models = await self.list()
        for model in models:
            if model.id == model_id:
                return model
        raise RemoteNotFound(f"Model ID {model_id!r} is not available for this account")

    async def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> ModelRef:
        models = await self.list()
        if not (startswith or endswith or exact):
            result = SelectionResolver.resolve(query, models)
            if not isinstance(result, ModelRef):
                raise TypeError("Model query did not resolve to a model")
            return result
        return QueryableCollection._find_in(
            models,
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

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
