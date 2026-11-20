from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable, Iterator, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from time import monotonic_ns
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar, cast

from .errors import AmbiguousMatchError, ProviderError, RemoteNotFound

if TYPE_CHECKING:
    from .async_provider import AsyncProvider
    from .provider import Provider

T = TypeVar("T", covariant=True)
U = TypeVar("U", covariant=True)
_ItemT = TypeVar("_ItemT")
Predicate = Callable[[T], bool]


@dataclass(frozen=True)
class Page(Generic[T]):
    """Internal pagination container; not part of the public user API."""

    items: tuple[T, ...]
    next_cursor: str | None = None


def validate_limit(limit: int | None) -> int | None:
    if limit is None:
        return None
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise TypeError("limit must be a non-negative integer or None")
    if limit < 0:
        raise ValueError("limit must be non-negative")
    return limit


def validate_match_flags(
    *,
    exact: bool = False,
    startswith: bool = False,
    endswith: bool = False,
) -> None:
    if sum((bool(exact), bool(startswith), bool(endswith))) > 1:
        raise ValueError("Specify at most one of exact, startswith, or endswith")


def validate_predicate(predicate: Any) -> None:
    if predicate is not None and not callable(predicate):
        raise TypeError("predicate must be callable or None")


class _QueryRules(Generic[T]):
    """Shared matching and extraction contract across local and remote collections."""

    @staticmethod
    def _extract_id(item: Any) -> str:
        if isinstance(item, dict):
            return str(item.get("id") or "")
        return str(getattr(item, "id", None) or "")

    @staticmethod
    def _extract_name(item: Any) -> str:
        if isinstance(item, dict):
            return str(item.get("name") or item.get("title") or item.get("content") or "")
        return str(
            getattr(item, "name", None)
            or getattr(item, "title", None)
            or getattr(item, "url", None)
            or getattr(item, "content", None)
            or ""
        )

    @classmethod
    def _matches(
        cls,
        text: str,
        query: str,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> bool:
        t = text.casefold()
        q = query.casefold()
        if exact:
            return t == q
        if startswith:
            return t.startswith(q)
        if endswith:
            return t.endswith(q)
        return q in t

    @classmethod
    def _item_matches(
        cls,
        item: Any,
        query: str,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> bool:
        return cls._matches(
            cls._extract_name(item),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        ) or cls._matches(
            cls._extract_id(item),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    @classmethod
    def _item_matches_exact_id(cls, item: Any, item_id: str) -> bool:
        return cls._extract_id(item) == item_id or getattr(item, "remote_id", None) == item_id

    @classmethod
    def _get_from(cls, items: Sequence[T], item_id: str) -> T:
        for it in items:
            if cls._item_matches_exact_id(it, item_id):
                return it
        raise RemoteNotFound(f"Record was not found: {item_id}")

    @classmethod
    def _ambiguous(cls, matches: Sequence[Any]) -> AmbiguousMatchError:
        ids = [cls._extract_id(it) for it in matches]
        return AmbiguousMatchError(
            f"Search is ambiguous; multiple matches were found: {', '.join(ids)}"
        )

    @classmethod
    def _query_predicate(
        cls,
        query: str,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> Callable[[Any], bool]:
        validate_match_flags(exact=exact, startswith=startswith, endswith=endswith)
        if not isinstance(query, str):
            raise TypeError("query must be a string")
        return lambda item: cls._item_matches(
            item,
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    @classmethod
    def _filter_items(
        cls,
        items: Sequence[T],
        predicate: Callable[[T], bool] | None = None,
        *,
        limit: int | None = None,
    ) -> list[T]:
        validate_predicate(predicate)
        validated = validate_limit(limit)
        if validated == 0:
            return []
        if predicate is None:
            return list(items) if validated is None else list(items[:validated])
        matched: list[T] = []
        for it in items:
            if predicate(it):
                matched.append(it)
                if validated is not None and len(matched) >= validated:
                    break
        return matched

    @classmethod
    def _first_matching(
        cls,
        items: Sequence[T],
        predicate: Callable[[T], bool] | None = None,
    ) -> T | None:
        validate_predicate(predicate)
        for it in items:
            if predicate is None or predicate(it):
                return it
        return None

    @classmethod
    def _last_matching(
        cls,
        items: Sequence[T],
        predicate: Callable[[T], bool] | None = None,
    ) -> T | None:
        validate_predicate(predicate)
        for it in reversed(items):
            if predicate is None or predicate(it):
                return it
        return None

    @classmethod
    def _single_matching(
        cls,
        items: Sequence[T],
        predicate: Callable[[T], bool] | None = None,
    ) -> T | None:
        validate_predicate(predicate)
        matched: list[T] = []
        for it in items:
            if predicate is None or predicate(it):
                matched.append(it)
                if len(matched) > 1:
                    raise cls._ambiguous(matched)
        return matched[0] if matched else None


class BaseCollection(_QueryRules[T], ABC):
    """Contract for synchronous collection queries."""

    @abstractmethod
    def list(
        self, predicate: Predicate[T] | None = None, *, limit: int | None = None, **options: Any
    ) -> list[T]: ...

    @abstractmethod
    def get(self, item_id: str, /) -> T: ...

    @abstractmethod
    def first(self, predicate: Predicate[T] | None = None, **options: Any) -> T | None: ...

    @abstractmethod
    def last(self, predicate: Predicate[T] | None = None, **options: Any) -> T | None: ...

    @abstractmethod
    def single(self, predicate: Predicate[T] | None = None, **options: Any) -> T | None: ...

    @abstractmethod
    def find(self, query: str, /, **options: Any) -> T | None: ...

    @abstractmethod
    def search(self, query: str, /, **options: Any) -> list[T]: ...


@dataclass
class _Read:
    collection: object
    started_ns: int | None = None
    exhausted: bool = False


_active_read: ContextVar[_Read | None] = ContextVar("collection_read", default=None)


def _event_item(item: object) -> dict[str, Any]:
    record = {"id": getattr(item, "remote_id", None) or getattr(item, "id", None)}
    if hasattr(item, "remote_id") and not item.remote_id:
        record["remote"] = False
    project = getattr(item, "project", None)
    if project is not None:
        record["project_id"] = getattr(project, "remote_id", None)
    return record


_ReadParams = ParamSpec("_ReadParams")
_ReadResult = TypeVar("_ReadResult")


def _report_read(
    method: Callable[_ReadParams, Coroutine[Any, Any, _ReadResult]],
) -> Callable[_ReadParams, Coroutine[Any, Any, _ReadResult]]:
    """Emit once for the outer query, only if the source was read."""

    invoke: Callable[..., Coroutine[Any, Any, _ReadResult]] = method

    @wraps(method)
    async def report(self, *args, **options):
        provider, scope_id = self._read_context()
        current = _active_read.get()
        if provider is None or (current is not None and current.collection is self):
            return await invoke(self, *args, **options)
        read = _Read(self)
        token = _active_read.set(read)
        missing = None
        try:
            result = await invoke(self, *args, **options)
        except RemoteNotFound as error:
            missing = error
            result = None
        finally:
            _active_read.reset(token)
        runtime = provider.runtime
        if read.started_ns is not None and (
            runtime.events.subscriber_count or runtime.sync_events.subscriber_count
        ):
            if isinstance(result, tuple):
                result_items = [result[0]]
            elif isinstance(result, list):
                result_items = result
            else:
                result_items = [] if result is None else [result]
            complete = (
                read.exhausted
                and method.__name__ == "list"
                and "limit" in options
                and options["limit"] is None
                and (not args or args[0] is None)
                and options.get("predicate") is None
            )
            await runtime.emit_safe(
                "collection.read.completed",
                resource_id=scope_id,
                payload={
                    "collection": self._collection_name,
                    "operation": method.__name__,
                    "items": [_event_item(item) for item in result_items],
                    "complete": complete,
                    "include_projects": bool(options.get("include_projects")),
                    "started_ns": read.started_ns,
                },
            )
        if missing is not None:
            raise missing
        return result

    return cast("Callable[_ReadParams, Coroutine[Any, Any, _ReadResult]]", report)


async def _publish_collection_change(
    runtime, collection, action, item, *, scope_id=None, operation_id=None
):
    if item is None:
        return
    record = {"id": item} if isinstance(item, str) else _event_item(item)
    if record["id"]:
        await runtime.emit_safe(
            "collection.changed",
            operation_id=operation_id,
            resource_id=scope_id,
            payload={
                "collection": collection,
                "action": action,
                "items": [record],
                "confirmed_ns": monotonic_ns(),
            },
        )


class AsyncBaseCollection(_QueryRules[T], ABC):
    """Contract and selection over asynchronous batches."""

    _collection_name = ""

    def _read_context(self):
        return None, None

    def _mark_remote_read(self, started_ns: int | None = None) -> None:
        read = _active_read.get()
        if read is not None and read.collection is self and read.started_ns is None:
            read.started_ns = monotonic_ns() if started_ns is None else started_ns

    @abstractmethod
    def _iter_batches(
        self, *, limit: int | None = None, **options: Any
    ) -> AsyncIterator[Sequence[T]]: ...

    async def _fetch_by_id(self, item_id: str) -> T | None:
        return None

    async def _collect_items(
        self,
        predicate: Callable[[T], bool] | None = None,
        *,
        limit: int | None = None,
        **options: Any,
    ) -> list[T]:
        validate_predicate(predicate)
        validated = validate_limit(limit)
        if validated == 0:
            return []
        if predicate is None:
            items: list[T] = []
            async for batch in self._iter_batches(limit=validated, **options):
                items.extend(batch)
            return items
        matched: list[T] = []
        async for batch in self._iter_batches(limit=None, **options):
            for item in batch:
                if predicate(item):
                    matched.append(item)
                    if validated is not None and len(matched) >= validated:
                        return matched
        return matched

    @_report_read
    async def list(
        self,
        predicate: Callable[[T], bool] | None = None,
        *,
        limit: int | None = 50,
        **options: Any,
    ) -> list[T]:
        return await self._collect_items(predicate, limit=limit, **options)

    @_report_read
    async def get(self, item_id: str, /) -> T:
        direct = await self._fetch_by_id(item_id)
        if direct is not None:
            return direct
        return await self._find_id(item_id)

    async def _find_id(self, item_id: str, **options: Any) -> T:
        item = await self.first(lambda item: self._item_matches_exact_id(item, item_id), **options)
        if item is None:
            raise RemoteNotFound(f"Record was not found: {item_id}")
        return item

    @_report_read
    async def first(
        self,
        predicate: Callable[[T], bool] | None = None,
        /,
        **options: Any,
    ) -> T | None:
        validate_predicate(predicate)
        page_limit = 1 if predicate is None else None
        async for batch in self._iter_batches(limit=page_limit, **options):
            for item in batch:
                if predicate is None or predicate(item):
                    return item
        return None

    @_report_read
    async def last(
        self,
        predicate: Callable[[T], bool] | None = None,
        /,
        **options: Any,
    ) -> T | None:
        validate_predicate(predicate)
        matched: T | None = None
        async for batch in self._iter_batches(limit=None, **options):
            for item in batch:
                if predicate is None or predicate(item):
                    matched = item
        return matched

    @_report_read
    async def single(
        self,
        predicate: Callable[[T], bool] | None = None,
        /,
        **options: Any,
    ) -> T | None:
        validate_predicate(predicate)
        matches: list[T] = []
        async for batch in self._iter_batches(limit=None, **options):
            for item in batch:
                if predicate is None or predicate(item):
                    matches.append(item)
                    if len(matches) > 1:
                        raise self._ambiguous(matches)
        return matches[0] if matches else None

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
        **options: Any,
    ) -> T | None:
        pred = self._query_predicate(query, exact=exact, startswith=startswith, endswith=endswith)
        if single:
            return await self.single(pred, **options)
        if last:
            return await self.last(pred, **options)
        return await self.first(pred, **options)

    @_report_read
    async def search(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        limit: int | None = None,
        **options: Any,
    ) -> list[T]:
        pred = self._query_predicate(query, exact=exact, startswith=startswith, endswith=endswith)
        return await self.list(pred, limit=limit, **options)


class Collection(BaseCollection[_ItemT]):
    """Synchronous collection operating over in-memory objects without network I/O."""

    def __init__(self, items: Iterable[_ItemT] | None = None) -> None:
        self._items: list[_ItemT] = list(items) if items is not None else []

    def list(
        self,
        predicate: Callable[[_ItemT], bool] | None = None,
        *,
        limit: int | None = None,
    ) -> list[_ItemT]:
        return self._filter_items(self._items, predicate, limit=limit)

    def get(self, item_id: str, /) -> _ItemT:
        return self._get_from(self._items, item_id)

    def first(self, predicate: Callable[[_ItemT], bool] | None = None) -> _ItemT | None:
        return self._first_matching(self._items, predicate)

    def last(self, predicate: Callable[[_ItemT], bool] | None = None) -> _ItemT | None:
        return self._last_matching(self._items, predicate)

    def single(self, predicate: Callable[[_ItemT], bool] | None = None) -> _ItemT | None:
        return self._single_matching(self._items, predicate)

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
    ) -> _ItemT | None:
        pred = self._query_predicate(query, exact=exact, startswith=startswith, endswith=endswith)
        if single:
            return self.single(pred)
        if last:
            return self.last(pred)
        return self.first(pred)

    def search(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        limit: int | None = None,
    ) -> list[_ItemT]:
        pred = self._query_predicate(query, exact=exact, startswith=startswith, endswith=endswith)
        return self.list(pred, limit=limit)

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[_ItemT]:
        return iter(self._items)

    def __getitem__(self, index: Any) -> Any:
        return self._items[index]

    def __contains__(self, item: Any) -> bool:
        target_id = self._extract_id(item) if not isinstance(item, str) else item
        return any(
            it is item or (target_id and self._extract_id(it) == target_id) for it in self._items
        )


class AsyncCollection(AsyncBaseCollection[_ItemT]):
    """Asynchronously query a loaded in-memory collection."""

    def __init__(self, items: Iterable[_ItemT] = ()) -> None:
        self._items: tuple[_ItemT, ...] | None = tuple(items)

    async def _load_items(self) -> Sequence[_ItemT]:
        return self._items or ()

    async def _iter_batches(self, *, limit: int | None = None) -> AsyncIterator[Sequence[_ItemT]]:
        items = await self._load_items()
        yield items if limit is None else items[:limit]

    @_report_read
    async def list(
        self, predicate: Predicate[_ItemT] | None = None, *, limit: int | None = None
    ) -> list[_ItemT]:
        return await self._collect_items(predicate, limit=limit)


class AsyncRemoteCollection(AsyncBaseCollection[T]):
    """Asynchronous remote collection with hidden pagination and unified queries."""

    def _read_context(self):
        return self._read_provider, self._scope_id

    @property
    def _read_provider(self) -> AsyncProvider[Any, Any] | None:
        return None

    @property
    def _scope_id(self) -> str | None:
        return None

    _max_remote_pages = 2000
    _default_page_size = 50
    _followup_page_size: int | None = None

    def _item_key(self, item: object) -> str:
        return getattr(item, "remote_id", None) or self._extract_id(item) or str(id(item))

    async def _fetch_page(
        self,
        cursor: str | None,
        page_limit: int | None,
        **options: Any,
    ) -> Page[T]:
        raise NotImplementedError("This collection does not use pagination")

    async def _iter_batches(
        self,
        *,
        limit: int | None = None,
        **options: Any,
    ) -> AsyncIterator[list[T]]:
        validated = validate_limit(limit)
        if validated == 0:
            return
        cursor: str | None = None
        seen_cursors: set[str] = set()
        seen_keys: set[str] = set()
        collected = 0
        try:
            for _ in range(self._max_remote_pages):
                remaining = None if validated is None else validated - collected
                page_size = self._default_page_size
                if cursor is not None and self._followup_page_size:
                    page_size = self._followup_page_size
                page_limit = min(page_size, remaining) if remaining is not None else page_size
                self._mark_remote_read()
                page = await self._fetch_page(cursor, page_limit, **options)
                if not isinstance(page, Page):
                    raise ProviderError("Provider returned an invalid pagination page")
                unique_batch: list[T] = []
                for item in page.items:
                    key = self._item_key(item)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    unique_batch.append(item)
                    collected += 1
                    if validated is not None and collected >= validated:
                        break
                if unique_batch:
                    yield unique_batch
                if validated is not None and collected >= validated:
                    return
                next_cursor = page.next_cursor
                if next_cursor is None:
                    read = _active_read.get()
                    if read is not None and read.collection is self:
                        read.exhausted = True
                    return
                if (
                    not isinstance(next_cursor, str)
                    or not next_cursor
                    or next_cursor in seen_cursors
                ):
                    raise ProviderError(
                        "Provider pagination returned an invalid or repeating cursor"
                    )
                seen_cursors.add(next_cursor)
                cursor = next_cursor
            raise ProviderError(f"Provider pagination exceeded {self._max_remote_pages} pages")
        except Exception as error:
            if not hasattr(error, "partial_count"):
                try:
                    setattr(error, "partial_count", collected)
                except Exception:
                    pass
            raise


class RemoteCollection(BaseCollection[U], Generic[_ItemT, U]):
    """Synchronous view of the provider's remote collection."""

    def __init__(self, owner: Provider[Any, Any], collection: AsyncBaseCollection[_ItemT]) -> None:
        self._owner = owner
        self._collection = collection

    def _wrap(self, item: _ItemT) -> U:
        return item  # type: ignore[return-value]

    def _wrap_optional(self, item: _ItemT | None) -> U | None:
        return self._wrap(item) if item is not None else None

    def _wrap_predicate(
        self, predicate: Callable[[U], bool] | None
    ) -> Callable[[_ItemT], bool] | None:
        if predicate is None:
            return None
        validate_predicate(predicate)
        return lambda item: bool(predicate(self._wrap(item)))

    def list(
        self,
        predicate: Callable[[U], bool] | None = None,
        *,
        limit: int | None = 50,
        **options: Any,
    ) -> list[U]:
        async_pred = self._wrap_predicate(predicate)
        return [
            self._wrap(item)
            for item in self._owner._call(self._collection.list(async_pred, limit=limit, **options))
        ]

    def get(self, item_id: str, /) -> U:
        return self._wrap(self._owner._call(self._collection.get(item_id)))

    def first(
        self,
        predicate: Callable[[U], bool] | None = None,
        /,
        **options: Any,
    ) -> U | None:
        async_pred = self._wrap_predicate(predicate)
        return self._wrap_optional(self._owner._call(self._collection.first(async_pred, **options)))

    def last(
        self,
        predicate: Callable[[U], bool] | None = None,
        /,
        **options: Any,
    ) -> U | None:
        async_pred = self._wrap_predicate(predicate)
        return self._wrap_optional(self._owner._call(self._collection.last(async_pred, **options)))

    def single(
        self,
        predicate: Callable[[U], bool] | None = None,
        /,
        **options: Any,
    ) -> U | None:
        async_pred = self._wrap_predicate(predicate)
        return self._wrap_optional(
            self._owner._call(self._collection.single(async_pred, **options))
        )

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
    ) -> U | None:
        return self._wrap_optional(
            self._owner._call(
                self._collection.find(
                    query,
                    exact=exact,
                    startswith=startswith,
                    endswith=endswith,
                    single=single,
                    last=last,
                    **options,
                )
            )
        )

    def search(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        limit: int | None = None,
        **options: Any,
    ) -> list[U]:
        return [
            self._wrap(item)
            for item in self._owner._call(
                self._collection.search(
                    query,
                    exact=exact,
                    startswith=startswith,
                    endswith=endswith,
                    limit=limit,
                    **options,
                )
            )
        ]
