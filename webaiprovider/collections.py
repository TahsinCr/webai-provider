from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from .errors import AmbiguousMatchError, ProviderError, RemoteNotFound

if TYPE_CHECKING:
    from .provider import Provider

T = TypeVar("T")
U = TypeVar("U")


@dataclass(frozen=True)
class Page(Generic[T]):
    """Internal adapter pagination container; not part of the public user API."""

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
    def _find_in(
        cls,
        items: Sequence[T],
        query: str,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> T:
        """Return the only candidate matching the strategy, never an implicit best match."""
        validate_match_flags(exact=exact, startswith=startswith, endswith=endswith)
        matches = [
            it
            for it in items
            if cls._item_matches(it, query, exact=exact, startswith=startswith, endswith=endswith)
        ]
        if len(matches) == 1:
            return matches[0]
        if matches:
            raise cls._ambiguous(matches)
        raise RemoteNotFound(f"No records matched the search query: {query}")

    @classmethod
    def _ambiguous(cls, matches: Sequence[Any]) -> AmbiguousMatchError:
        ids = [cls._extract_id(it) for it in matches]
        return AmbiguousMatchError(
            f"Search is ambiguous; multiple matches were found: {', '.join(ids)}"
        )

    @classmethod
    def _first_in(
        cls,
        items: Sequence[T],
        query: str,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> T:
        validate_match_flags(exact=exact, startswith=startswith, endswith=endswith)
        for it in items:
            if cls._item_matches(it, query, exact=exact, startswith=startswith, endswith=endswith):
                return it
        raise RemoteNotFound(f"No records matched the search query: {query}")

    @classmethod
    def _last_in(
        cls,
        items: Sequence[T],
        query: str,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> T:
        validate_match_flags(exact=exact, startswith=startswith, endswith=endswith)
        for it in reversed(items):
            if cls._item_matches(it, query, exact=exact, startswith=startswith, endswith=endswith):
                return it
        raise RemoteNotFound(f"No records matched the search query: {query}")

    @classmethod
    def _search_in(
        cls,
        items: Sequence[T],
        query: str,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> list[T]:
        validate_match_flags(exact=exact, startswith=startswith, endswith=endswith)
        return [
            it
            for it in items
            if cls._item_matches(it, query, exact=exact, startswith=startswith, endswith=endswith)
        ]


class BaseLocalCollection(BaseCollection[T]):
    """Synchronous collection operating over in-memory objects without network I/O."""

    def __init__(self, items: Iterable[T] | None = None) -> None:
        self._items: list[T] = list(items) if items is not None else []

    def list(self, *, limit: int | None = None) -> list[T]:
        validated = validate_limit(limit)
        if validated is None:
            return list(self._items)
        if validated == 0:
            return []
        return list(self._items[:validated])

    def get(self, item_id: str, /) -> T:
        return self._get_from(self._items, item_id)

    def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> T:
        return self._find_in(
            self._items,
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    def first(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> T:
        return self._first_in(
            self._items,
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    def last(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> T:
        return self._last_in(
            self._items,
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    def search(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> list[T]:
        return self._search_in(
            self._items,
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

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

    async def _collect_remote(self, *, limit: int | None = None, **options: Any) -> list[T]:
        items: list[T] = []
        async for batch in self._iter_remote_pages(limit=limit, **options):
            items.extend(batch)
        return items

    async def list(self, *, limit: int | None = 50, **options: Any) -> list[T]:
        return await self._collect_remote(limit=limit, **options)

    async def get(self, item_id: str, /) -> T:
        direct = await self._fetch_by_id(item_id)
        if direct is not None:
            return direct
        async for batch in self._iter_remote_pages(limit=None):
            for item in batch:
                if self._item_matches_exact_id(item, item_id):
                    return item
        raise RemoteNotFound(f"Record was not found: {item_id}")

    async def search(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        **options: Any,
    ) -> list[T]:
        validate_match_flags(exact=exact, startswith=startswith, endswith=endswith)
        matches: list[T] = []
        async for batch in self._iter_remote_pages(limit=None, **options):
            for item in batch:
                if self._item_matches(
                    item, query, exact=exact, startswith=startswith, endswith=endswith
                ):
                    matches.append(item)
        return matches

    async def first(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        **options: Any,
    ) -> T:
        validate_match_flags(exact=exact, startswith=startswith, endswith=endswith)
        async for batch in self._iter_remote_pages(limit=None, **options):
            for item in batch:
                if self._item_matches(
                    item, query, exact=exact, startswith=startswith, endswith=endswith
                ):
                    return item
        raise RemoteNotFound(f"No records matched the search query: {query}")

    async def last(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        **options: Any,
    ) -> T:
        validate_match_flags(exact=exact, startswith=startswith, endswith=endswith)
        matched: T | None = None
        async for batch in self._iter_remote_pages(limit=None, **options):
            for item in batch:
                if self._item_matches(
                    item, query, exact=exact, startswith=startswith, endswith=endswith
                ):
                    matched = item
        if matched is None:
            raise RemoteNotFound(f"No records matched the search query: {query}")
        return matched

    async def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        **options: Any,
    ) -> T:
        validate_match_flags(exact=exact, startswith=startswith, endswith=endswith)
        matches: list[T] = []
        async for batch in self._iter_remote_pages(limit=None, **options):
            for item in batch:
                if self._item_matches(
                    item, query, exact=exact, startswith=startswith, endswith=endswith
                ):
                    matches.append(item)
                    if len(matches) > 1:
                        raise self._ambiguous(matches)
        if not matches:
            raise RemoteNotFound(f"No records matched the search query: {query}")
        return matches[0]

    async def refresh(self, *, limit: int | None = None, **options: Any) -> list[T]:
        items = await self._collect_remote(limit=limit, **options)
        await self._emit_refreshed(len(items))
        return items


class AsyncRemoteCollection(BaseRemoteCollection[T], ABC):
    """Readable remote collection returning materialized lists without caching stale _items."""


class AsyncDerivedCollection(BaseCollection[T], ABC):
    """Derived queryable view over another collection without fake page-fetching."""

    @abstractmethod
    async def list(self, *, limit: int | None = 50, **options: Any) -> list[T]:
        """Return projected items from the underlying collection."""

    @abstractmethod
    async def refresh(self, *, limit: int | None = None, **options: Any) -> list[T]:
        """Refresh the underlying collection and return projected items."""

    async def get(self, item_id: str, /) -> T:
        return self._get_from(await self.list(limit=None), item_id)

    async def search(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        **_options: Any,
    ) -> list[T]:
        return self._search_in(
            await self.list(limit=None),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    async def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        **_options: Any,
    ) -> T:
        return self._find_in(
            await self.list(limit=None),
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
        **_options: Any,
    ) -> T:
        return self._first_in(
            await self.list(limit=None),
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
        **_options: Any,
    ) -> T:
        return self._last_in(
            await self.list(limit=None),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )


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

    async def list(self, *, limit: int | None = None, **_kwargs: Any) -> list[T]:
        return super().list(limit=limit)

    async def get(self, item_id: str, /) -> T:
        return BaseCollection._get_from(await self.list(), item_id)

    async def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> T:
        return BaseCollection._find_in(
            await self.list(),
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
    ) -> T:
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
    ) -> T:
        return BaseCollection._last_in(
            await self.list(),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    async def search(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> list[T]:
        return BaseCollection._search_in(
            await self.list(),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )


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

    def list(self, *, limit: int | None = 50, **options: Any) -> list[U]:
        return [
            self._wrap(item)
            for item in self._owner._call(self._collection.list(limit=limit, **options))
        ]

    def get(self, item_id: str, /) -> U:
        return self._wrap(self._owner._call(self._collection.get(item_id)))

    def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        **options: Any,
    ) -> U:
        return self._wrap(
            self._owner._call(
                self._collection.find(
                    query, exact=exact, startswith=startswith, endswith=endswith, **options
                )
            )
        )

    def first(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        **options: Any,
    ) -> U:
        return self._wrap(
            self._owner._call(
                self._collection.first(
                    query, exact=exact, startswith=startswith, endswith=endswith, **options
                )
            )
        )

    def last(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        **options: Any,
    ) -> U:
        return self._wrap(
            self._owner._call(
                self._collection.last(
                    query, exact=exact, startswith=startswith, endswith=endswith, **options
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
        **options: Any,
    ) -> list[U]:
        return [
            self._wrap(item)
            for item in self._owner._call(
                self._collection.search(
                    query, exact=exact, startswith=startswith, endswith=endswith, **options
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

    def list(self, *, limit: int | None = None) -> list[U]:
        items = [self._wrap(item) for item in self._owner._call(self._collection.list(limit=limit))]
        self._items = items
        return list(items)

    def get(self, item_id: str, /) -> U:
        return self._wrap(self._owner._call(self._collection.get(item_id)))

    def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> U:
        return self._wrap(
            self._owner._call(
                self._collection.find(query, exact=exact, startswith=startswith, endswith=endswith)
            )
        )

    def first(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> U:
        return self._wrap(
            self._owner._call(
                self._collection.first(query, exact=exact, startswith=startswith, endswith=endswith)
            )
        )

    def last(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> U:
        return self._wrap(
            self._owner._call(
                self._collection.last(query, exact=exact, startswith=startswith, endswith=endswith)
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
    ) -> list[U]:
        return [
            self._wrap(item)
            for item in self._owner._call(
                self._collection.search(
                    query, exact=exact, startswith=startswith, endswith=endswith
                )
            )
        ]

    def refresh(self) -> list[U]:
        items = [self._wrap(item) for item in self._owner._call(self._collection.refresh())]
        self._items = items
        return list(items)
