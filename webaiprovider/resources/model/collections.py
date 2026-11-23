from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Awaitable, Callable, Iterable, Iterator, Sequence
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
    from ...async_provider import AsyncWebAIProvider
    from ...provider import WebAIProvider

MODEL_CATALOG_TTL = 300.0


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
        provider: AsyncWebAIProvider[Any, Any] | None = None,
        ttl: float | None = MODEL_CATALOG_TTL,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._loader = loader
        self._provider = provider
        self._ttl = ttl
        self._clock = clock
        self._snapshot: tuple[tuple[ModelRef, ...], float] | None = None
        self._lock = asyncio.Lock()

    def _read_context(self):
        return self._provider, None

    def _prime(self, items: Iterable[ModelRef]) -> None:
        self._snapshot = (tuple(items), self._clock())

    def _clear(self) -> None:
        self._snapshot = None

    def _is_fresh(self, snapshot: tuple[tuple[ModelRef, ...], float] | None) -> bool:
        return snapshot is not None and (
            self._ttl is None or self._clock() - snapshot[1] < self._ttl
        )

    async def _reload(self, *, force: bool) -> tuple[ModelRef, ...]:
        self._mark_remote_read()
        items = tuple(await self._loader(force))
        self._snapshot = (items, self._clock())
        return items

    async def _load_items(self) -> tuple[ModelRef, ...]:
        snapshot = self._snapshot
        if self._is_fresh(snapshot):
            assert snapshot is not None
            return snapshot[0]
        async with self._lock:
            snapshot = self._snapshot
            if self._is_fresh(snapshot):
                assert snapshot is not None
                return snapshot[0]
            return await self._reload(force=False)

    @_async_report_read
    async def refresh(self) -> list[ModelRef]:
        """Replace the session catalog after a successful provider load."""
        async with self._lock:
            return list(await self._reload(force=True))

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
        provider: WebAIProvider[Any, Any] | None = None,
        ttl: float | None = MODEL_CATALOG_TTL,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._loader = loader
        self._provider = provider
        self._ttl = ttl
        self._clock = clock
        self._snapshot: tuple[tuple[ModelRef, ...], float] | None = None
        self._lock = threading.Lock()

    def _read_context(self):
        return self._provider, None

    def _prime(self, items: Iterable[ModelRef]) -> None:
        self._snapshot = (tuple(items), self._clock())

    def _clear(self) -> None:
        self._snapshot = None

    def _is_fresh(self, snapshot: tuple[tuple[ModelRef, ...], float] | None) -> bool:
        return snapshot is not None and (
            self._ttl is None or self._clock() - snapshot[1] < self._ttl
        )

    def _reload(self, *, force: bool) -> tuple[ModelRef, ...]:
        self._mark_remote_read()
        items = tuple(self._loader(force))
        self._snapshot = (items, self._clock())
        return items

    def _load_items(self) -> tuple[ModelRef, ...]:
        snapshot = self._snapshot
        if self._is_fresh(snapshot):
            assert snapshot is not None
            return snapshot[0]
        with self._lock:
            snapshot = self._snapshot
            if self._is_fresh(snapshot):
                assert snapshot is not None
                return snapshot[0]
            return self._reload(force=False)

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
            return list(self._reload(force=True))

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
