from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable, Iterator, Sequence
from typing import TYPE_CHECKING, Any

from ...collections import (
    AsyncCollection,
    RemoteCollection,
    _async_report_read,
    _report_read,
)
from ...errors import RemoteNotFound
from .model import EffortRef, ModelRef, ModelSelection
from .resolver import SelectionResolver
from .types import Models

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider


def _model_by_id(models: Sequence[ModelRef], model_id: str) -> ModelRef:
    for model in models:
        if model.id == model_id:
            return model
    raise RemoteNotFound(f"Model ID {model_id!r} is not available for this account")


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


def _find_catalog_model(query: Models, models: tuple[ModelRef, ...]) -> ModelRef:
    result = SelectionResolver.resolve(query, models)
    if not isinstance(result, ModelRef):
        raise TypeError("Model query did not resolve to a model")
    return result


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

    @_async_report_read
    async def refresh(self) -> list[ModelRef]:
        """Replace the session catalog after a successful provider load."""
        async with self._lock:
            return list(await self._reload())

    @_async_report_read
    async def get(self, model_id: str, /) -> ModelRef:
        return _model_by_id(await self._load_items(), model_id)

    @_async_report_read
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
        **options: Any,
    ) -> ModelRef | None:
        if isinstance(query, Models) and not (exact or startswith or endswith or last or options):
            return _find_catalog_model(query, await self._load_items())
        return await super().find(
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
            single=single,
            last=last,
            **options,
        )

    async def resolve(
        self,
        target: Models | EffortRef | ModelRef | ModelSelection | str,
    ) -> ModelSelection:
        return _resolve_model_selection(target, await self.list())


class ModelsCollection(RemoteCollection[ModelRef]):
    """Synchronous session model catalog backed by a native blocking loader."""

    _collection_name = "models"

    def __init__(
        self,
        loader: Callable[[bool], list[ModelRef]],
        *,
        provider: Provider[Any, Any] | None = None,
    ) -> None:
        self._loader = loader
        self._provider = provider
        self._items: tuple[ModelRef, ...] | None = None
        self._lock = threading.Lock()

    def _read_context(self):
        return self._provider, None

    def _clear(self) -> None:
        self._items = None

    def _reload(self) -> tuple[ModelRef, ...]:
        self._mark_remote_read()
        items = tuple(self._loader(True))
        self._items = items
        return items

    def _load_items(self) -> tuple[ModelRef, ...]:
        items = self._items
        if items is not None:
            return items
        with self._lock:
            items = self._items
            return self._reload() if items is None else items

    def _iter_batches(
        self, *, limit: int | None = None, **options: Any
    ) -> Iterator[Sequence[ModelRef]]:
        if options:
            raise TypeError(f"Unexpected model list option: {next(iter(options))}")
        items = self._load_items()
        yield items if limit is None else items[:limit]

    @_report_read
    def list(
        self,
        predicate: Callable[[ModelRef], bool] | None = None,
        *,
        limit: int | None = None,
        **options: Any,
    ) -> list[ModelRef]:
        if options:
            raise TypeError(f"Unexpected model list option: {next(iter(options))}")
        return self._collect_items(predicate, limit=limit)

    @_report_read
    def refresh(self) -> list[ModelRef]:
        """Replace the session catalog after a successful provider load."""
        with self._lock:
            return list(self._reload())

    @_report_read
    def get(self, model_id: str, /) -> ModelRef:
        return _model_by_id(self._load_items(), model_id)

    @_report_read
    def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        single: bool = False,
        last: bool = False,
        **options: Any,
    ) -> ModelRef | None:
        if isinstance(query, Models) and not (exact or startswith or endswith or last or options):
            return _find_catalog_model(query, self._load_items())
        return super().find(
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
            single=single,
            last=last,
            **options,
        )

    def resolve(
        self, target: Models | EffortRef | ModelRef | ModelSelection | str
    ) -> ModelSelection:
        return _resolve_model_selection(target, self.list())
