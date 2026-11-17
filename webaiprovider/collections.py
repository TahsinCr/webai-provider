from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from .errors import AmbiguousMatchError, ProviderError, RemoteNotFound

if TYPE_CHECKING:
    from .provider import Provider

T = TypeVar("T")
U = TypeVar("U")
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


class BaseCollection(Generic[T], ABC):
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


class BaseLocalCollection(BaseCollection[T]):
    """Synchronous collection operating over in-memory objects without network I/O."""

    def __init__(self, items: Iterable[T] | None = None) -> None:
        self._items: list[T] = list(items) if items is not None else []

    def list(
        self,
        predicate: Callable[[T], bool] | None = None,
        *,
        limit: int | None = None,
    ) -> list[T]:
        return self._filter_items(self._items, predicate, limit=limit)

    def get(self, item_id: str, /) -> T:
        return self._get_from(self._items, item_id)

    def first(self, predicate: Callable[[T], bool] | None = None) -> T | None:
        return self._first_matching(self._items, predicate)

    def last(self, predicate: Callable[[T], bool] | None = None) -> T | None:
        return self._last_matching(self._items, predicate)

    def single(self, predicate: Callable[[T], bool] | None = None) -> T | None:
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
    ) -> T | None:
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
    ) -> list[T]:
        pred = self._query_predicate(query, exact=exact, startswith=startswith, endswith=endswith)
        return self.list(pred, limit=limit)

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def __getitem__(self, index: Any) -> Any:
        return self._items[index]

    def __bool__(self) -> bool:
        return bool(self._items)

    def __contains__(self, item: Any) -> bool:
        target_id = self._extract_id(item) if not isinstance(item, str) else item
        return any(
            it is item or (target_id and self._extract_id(it) == target_id) for it in self._items
        )


class ReadOnlyCollection(BaseLocalCollection[T]):
    """Read-only synchronous local collection for Sources and Artifacts."""


class QueryableCollection(BaseLocalCollection[T], ABC):
    """Synchronous queryable collection base."""


class ManageableCollection(QueryableCollection[T], ABC):
    """Synchronous collection base with CRUD lifecycle operations."""

    def refresh(self) -> list[T]:
        return list(self._items)

    @abstractmethod
    def create(self, *args: Any, **kwargs: Any) -> Any:
        """Create a new item in the collection."""

    @abstractmethod
    def delete(self, target: T | str, /, *args: Any, **kwargs: Any) -> Any:
        """Delete an item from the collection and its remote backend."""

    @abstractmethod
    def update(self, target: T | str, /, **fields: Any) -> Any:
        """Update an existing item in the collection."""


class BaseRemoteCollection(BaseCollection[T], ABC):
    """Asynchronous remote collection with hidden pagination and unified queries."""

    _max_remote_pages = 500
    _default_page_size = 50

    def _item_key(self, item: T) -> str:
        return getattr(item, "remote_id", None) or self._extract_id(item) or str(id(item))

    @abstractmethod
    async def _fetch_page(
        self,
        cursor: str | None,
        page_limit: int | None,
        **options: Any,
    ) -> Page[T]:
        """Fetch and normalize a single remote page."""

    async def _fetch_by_id(self, item_id: str) -> T | None:
        del item_id
        return None

    async def _emit_refreshed(self, count: int) -> None:
        del count

    async def _iter_remote_pages(
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
                page_limit = (
                    min(self._default_page_size, remaining)
                    if remaining is not None
                    else self._default_page_size
                )
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

    async def _collect_remote(
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
            async for batch in self._iter_remote_pages(limit=validated, **options):
                items.extend(batch)
            return items
        matched: list[T] = []
        async for batch in self._iter_remote_pages(limit=None, **options):
            for item in batch:
                if predicate(item):
                    matched.append(item)
                    if validated is not None and len(matched) >= validated:
                        return matched
        return matched

    async def list(
        self,
        predicate: Callable[[T], bool] | None = None,
        *,
        limit: int | None = 50,
        **options: Any,
    ) -> list[T]:
        return await self._collect_remote(predicate, limit=limit, **options)

    async def get(self, item_id: str, /) -> T:
        direct = await self._fetch_by_id(item_id)
        if direct is not None:
            return direct
        async for batch in self._iter_remote_pages(limit=None):
            for item in batch:
                if self._item_matches_exact_id(item, item_id):
                    return item
        raise RemoteNotFound(f"Record was not found: {item_id}")

    async def first(
        self,
        predicate: Callable[[T], bool] | None = None,
        /,
        **options: Any,
    ) -> T | None:
        validate_predicate(predicate)
        page_limit = 1 if predicate is None else None
        async for batch in self._iter_remote_pages(limit=page_limit, **options):
            for item in batch:
                if predicate is None or predicate(item):
                    return item
        return None

    async def last(
        self,
        predicate: Callable[[T], bool] | None = None,
        /,
        **options: Any,
    ) -> T | None:
        validate_predicate(predicate)
        matched: T | None = None
        async for batch in self._iter_remote_pages(limit=None, **options):
            for item in batch:
                if predicate is None or predicate(item):
                    matched = item
        return matched

    async def single(
        self,
        predicate: Callable[[T], bool] | None = None,
        /,
        **options: Any,
    ) -> T | None:
        validate_predicate(predicate)
        matches: list[T] = []
        async for batch in self._iter_remote_pages(limit=None, **options):
            for item in batch:
                if predicate is None or predicate(item):
                    matches.append(item)
                    if len(matches) > 1:
                        raise self._ambiguous(matches)
        return matches[0] if matches else None

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

    async def refresh(self, *, limit: int | None = None, **options: Any) -> list[T]:
        items = await self._collect_remote(limit=limit, **options)
        await self._emit_refreshed(len(items))
        return items


class AsyncRemoteCollection(BaseRemoteCollection[T], ABC):
    """Readable remote collection returning materialized lists without caching stale _items."""


class AsyncDerivedCollection(BaseCollection[T], ABC):
    """Derived queryable view over another collection without fake page-fetching."""

    @abstractmethod
    async def list(
        self,
        predicate: Callable[[T], bool] | None = None,
        *,
        limit: int | None = 50,
        **options: Any,
    ) -> list[T]:
        """Return projected items from the underlying collection."""

    @abstractmethod
    async def refresh(self, *, limit: int | None = None, **options: Any) -> list[T]:
        """Refresh the underlying collection and return projected items."""

    async def get(self, item_id: str, /) -> T:
        return self._get_from(await self.list(limit=None), item_id)

    async def first(
        self,
        predicate: Callable[[T], bool] | None = None,
        /,
        **options: Any,
    ) -> T | None:
        items = await self.list(predicate, limit=None, **options)
        return items[0] if items else None

    async def last(
        self,
        predicate: Callable[[T], bool] | None = None,
        /,
        **options: Any,
    ) -> T | None:
        items = await self.list(predicate, limit=None, **options)
        return items[-1] if items else None

    async def single(
        self,
        predicate: Callable[[T], bool] | None = None,
        /,
        **options: Any,
    ) -> T | None:
        items = await self.list(predicate, limit=None, **options)
        if len(items) > 1:
            raise self._ambiguous(items[:2])
        return items[0] if items else None

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


class AsyncMutableRemoteCollection(AsyncRemoteCollection[T], ABC):
    """Mutable remote collection supporting create/update/delete and collection hooks."""

    def __init__(self) -> None:
        from .core.hooks import OperationHooks

        self.hooks = OperationHooks()

    @abstractmethod
    async def create(self, *args: Any, **kwargs: Any) -> Any:
        """Create a resource in the remote backend."""

    @abstractmethod
    async def update(self, target: T | str, /, **fields: Any) -> Any:
        """Update an existing remote resource."""

    @abstractmethod
    async def delete(self, target: T | str, /, *args: Any, **kwargs: Any) -> Any:
        """Delete an existing remote resource."""


class AsyncQueryableCollection(BaseLocalCollection[T], ABC):
    """Generic asynchronous collection base over local or loaded items."""

    async def list(
        self,
        predicate: Callable[[T], bool] | None = None,
        *,
        limit: int | None = None,
        **_kwargs: Any,
    ) -> list[T]:
        return self._filter_items(self._items, predicate, limit=limit)

    async def get(self, item_id: str, /) -> T:
        return self._get_from(await self.list(limit=None), item_id)

    async def first(
        self,
        predicate: Callable[[T], bool] | None = None,
        /,
        **options: Any,
    ) -> T | None:
        items = await self.list(predicate, limit=None, **options)
        return items[0] if items else None

    async def last(
        self,
        predicate: Callable[[T], bool] | None = None,
        /,
        **options: Any,
    ) -> T | None:
        items = await self.list(predicate, limit=None, **options)
        return items[-1] if items else None

    async def single(
        self,
        predicate: Callable[[T], bool] | None = None,
        /,
        **options: Any,
    ) -> T | None:
        items = await self.list(predicate, limit=None, **options)
        if len(items) > 1:
            raise self._ambiguous(items[:2])
        return items[0] if items else None

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


class AsyncManageableCollection(AsyncQueryableCollection[T], ABC):
    """Asynchronous local/loaded collection base with refresh support."""

    async def refresh(self) -> list[T]:
        return await self.list()


class _RemoteCollectionFacade(Generic[T, U]):
    """Stateless synchronous facade over an AsyncRemoteCollection via LoopPortal."""

    def __init__(self, owner: Provider, collection: Any) -> None:
        self._owner = owner
        self._collection = collection

    def _wrap(self, item: T) -> U:
        return item  # type: ignore[return-value]

    def _wrap_optional(self, item: T | None) -> U | None:
        return self._wrap(item) if item is not None else None

    def _wrap_predicate(self, predicate: Callable[[U], bool] | None) -> Callable[[T], bool] | None:
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

    def refresh(self, *, limit: int | None = None, **options: Any) -> list[U]:
        return [
            self._wrap(item)
            for item in self._owner._call(self._collection.refresh(limit=limit, **options))
        ]


class _MutableRemoteCollectionFacade(_RemoteCollectionFacade[T, U]):
    """Synchronous mutable collection facade exposing SyncOperationHooks."""

    def __init__(self, owner: Provider, collection: AsyncMutableRemoteCollection[T]) -> None:
        super().__init__(owner, collection)
        from .core.hooks import SyncOperationHooks

        self.hooks = SyncOperationHooks(collection.hooks)


class _CollectionFacade(QueryableCollection[U], Generic[T, U]):
    """Synchronous view over an asynchronous queryable collection owned by Provider."""

    def __init__(self, owner: Provider, collection: Any) -> None:
        super().__init__()
        self._owner = owner
        self._collection = collection

    def _wrap(self, item: T) -> U:
        return item  # type: ignore[return-value]

    def _wrap_optional(self, item: T | None) -> U | None:
        return self._wrap(item) if item is not None else None

    def _wrap_predicate(self, predicate: Callable[[U], bool] | None) -> Callable[[T], bool] | None:
        if predicate is None:
            return None
        validate_predicate(predicate)
        return lambda item: bool(predicate(self._wrap(item)))

    def list(
        self,
        predicate: Callable[[U], bool] | None = None,
        *,
        limit: int | None = None,
        **options: Any,
    ) -> list[U]:
        async_pred = self._wrap_predicate(predicate)
        items = [
            self._wrap(item)
            for item in self._owner._call(self._collection.list(async_pred, limit=limit, **options))
        ]
        if predicate is None and not options:
            self._items = items
        return list(items)

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

    def refresh(self) -> list[U]:
        items = [self._wrap(item) for item in self._owner._call(self._collection.refresh())]
        self._items = items
        return list(items)
