from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any

from ...collections import (
    AsyncCollection,
    RemoteCollection,
    _report_read,
)
from ...errors import RemoteNotFound
from .model import EffortRef, ModelRef, ModelSelection
from .resolver import SelectionResolver
from .types import Models

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider


def _resolve_model_selection(
    target: Models | EffortRef | ModelRef | ModelSelection | str,
    models: Sequence[ModelRef],
) -> ModelSelection:
    result = SelectionResolver.resolve(target, models)
    if isinstance(result, ModelRef):
        return ModelSelection(result)
    if isinstance(result, ModelSelection):
        return result
    raise TypeError("Selection did not resolve to a model")


class AsyncModelsCollection(AsyncCollection[ModelRef]):
    _collection_name = "models"

    def __init__(
        self,
        loader: Callable[[bool], Awaitable[list[ModelRef]]],
        *,
        provider: AsyncProvider[Any, Any] | None = None,
    ) -> None:
        self._loader = loader
        self._provider = provider
        self._items: tuple[ModelRef, ...] | None = None
        self._lock = asyncio.Lock()

    def _read_context(self):
        return self._provider, None

    def _clear(self) -> None:
        self._items = None

    async def _reload(self) -> tuple[ModelRef, ...]:
        self._mark_remote_read()
        self._items = tuple(await self._loader(True))
        return self._items

    async def _load_items(self) -> tuple[ModelRef, ...]:
        if self._items is None:
            async with self._lock:
                if self._items is None:
                    return await self._reload()
        return self._items

    @_report_read
    async def refresh(self) -> list[ModelRef]:
        """Replace the session catalog after a successful provider load."""
        async with self._lock:
            return list(await self._reload())

    @_report_read
    async def get(self, model_id: str, /) -> ModelRef:
        for model in await self._load_items():
            if model.id == model_id:
                return model
        raise RemoteNotFound(f"Model ID {model_id!r} is not available for this account")

    @_report_read
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
    ) -> ModelRef | None:
        if isinstance(query, Models) and not (exact or startswith or endswith or last):
            models = await self._load_items()
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
        )

    async def resolve(
        self,
        target: Models | EffortRef | ModelRef | ModelSelection | str,
    ) -> ModelSelection:
        return _resolve_model_selection(target, await self.list())


class ModelsCollection(RemoteCollection[ModelRef, ModelRef]):
    def __init__(self, owner: Provider[Any, Any]) -> None:
        super().__init__(owner, owner._provider.models)

    def list(
        self,
        predicate: Callable[[ModelRef], bool] | None = None,
        *,
        limit: int | None = None,
    ) -> list[ModelRef]:
        self._owner._ensure_blocking_call()
        items = self._collection._items
        if items is not None:
            return self._filter_items(items, predicate, limit=limit)
        return self._owner._call(self._collection.list(predicate, limit=limit))

    def refresh(self) -> list[ModelRef]:
        return self._owner._call(self._collection.refresh())

    def resolve(
        self, target: Models | EffortRef | ModelRef | ModelSelection | str
    ) -> ModelSelection:
        self._owner._ensure_blocking_call()
        items = self._collection._items
        if items is not None:
            return _resolve_model_selection(target, items)
        return self._owner._call(self._collection.resolve(target))
