from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from ...collections import (
    AsyncQueryableCollection,
    BaseCollection,
    _CollectionFacade,
    validate_limit,
    validate_match_flags,
)
from ...errors import RemoteNotFound
from .model import EffortRef, ModelRef, ModelSelection
from .resolver import SelectionResolver
from .types import Models, StaticModelCatalog, _model_refs_from_catalog, effort_ref

if TYPE_CHECKING:
    from ...provider import Provider

__all__ = [
    "AsyncModelsCollection",
    "Models",
    "ModelsCollection",
    "SelectionResolver",
    "StaticModelCatalog",
    "_model_refs_from_catalog",
    "effort_ref",
]


class AsyncModelsCollection(AsyncQueryableCollection[ModelRef]):
    def __init__(self, loader: Callable[[bool], Awaitable[list[ModelRef]]]) -> None:
        self._loader = loader
        self._items_tuple: tuple[ModelRef, ...] | None = None
        self._lock = asyncio.Lock()

    async def list(self, *, refresh: bool = False, limit: int | None = None) -> list[ModelRef]:
        """Load once to prewarm the account cache; refresh forces a provider fetch."""
        validated = validate_limit(limit)
        if validated == 0 and not refresh:
            return []
        async with self._lock:
            if self._items_tuple is None or refresh:
                loaded = await self._loader(refresh)
                self._items_tuple = tuple(loaded)
            items = list(self._items_tuple)
            return items if validated is None else items[:validated]

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
        validate_match_flags(exact=exact, startswith=startswith, endswith=endswith)
        models = await self.list()
        if not (startswith or endswith or exact):
            result = SelectionResolver.resolve(query, models)
            if not isinstance(result, ModelRef):
                raise TypeError("Model query did not resolve to a model")
            return result
        return BaseCollection._find_in(
            models,
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    async def first(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> ModelRef:
        return BaseCollection._first_in(
            await self.list(),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    async def last(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> ModelRef:
        return BaseCollection._last_in(
            await self.list(),
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


class ModelsCollection(_CollectionFacade[ModelRef, ModelRef]):
    def __init__(self, owner: Provider) -> None:
        super().__init__(owner, owner._provider.models)

    def list(self, *, refresh: bool = False, limit: int | None = None) -> list[ModelRef]:
        self._items = self._owner._call(self._collection.list(refresh=refresh, limit=limit))
        return list(self._items)

    def refresh(self) -> list[ModelRef]:
        self._items = self._owner._call(self._collection.refresh())
        return list(self._items)

    def resolve(
        self, target: Models | EffortRef | ModelRef | ModelSelection | str
    ) -> ModelSelection:
        return self._owner._call(self._collection.resolve(target))
