from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from ...collections import (
    AsyncQueryableCollection,
    _CollectionFacade,
    validate_limit,
    validate_predicate,
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

    async def list(
        self,
        predicate: Callable[[ModelRef], bool] | None = None,
        *,
        refresh: bool = False,
        limit: int | None = None,
    ) -> list[ModelRef]:
        """Load once to prewarm the account cache; refresh forces a provider fetch."""
        validate_predicate(predicate)
        validated = validate_limit(limit)
        if validated == 0 and not refresh:
            return []
        async with self._lock:
            if self._items_tuple is None or refresh:
                loaded = await self._loader(refresh)
                self._items_tuple = tuple(loaded)
            return self._filter_items(list(self._items_tuple), predicate, limit=validated)

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
        single: bool = False,
        last: bool = False,
        refresh: bool = False,
    ) -> ModelRef | None:
        if isinstance(query, Models) and not (exact or startswith or endswith or last):
            models = await self.list(refresh=refresh)
            result = SelectionResolver.resolve(query, models)
            if not isinstance(result, ModelRef):
                raise TypeError("Model query did not resolve to a model")
            return result
        return await super().find(
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
            single=single,
            last=last,
            refresh=refresh,
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

    def list(
        self,
        predicate: Callable[[ModelRef], bool] | None = None,
        *,
        refresh: bool = False,
        limit: int | None = None,
    ) -> list[ModelRef]:
        async_pred = self._wrap_predicate(predicate)
        items = self._owner._call(self._collection.list(async_pred, refresh=refresh, limit=limit))
        if predicate is None:
            self._items = items
        return list(items)

    def refresh(self) -> list[ModelRef]:
        self._items = self._owner._call(self._collection.refresh())
        return list(self._items)

    def resolve(
        self, target: Models | EffortRef | ModelRef | ModelSelection | str
    ) -> ModelSelection:
        return self._owner._call(self._collection.resolve(target))
