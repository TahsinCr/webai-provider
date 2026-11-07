from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from typing import Any, Generic, TypeVar

from ..errors import AmbiguousMatchError, RemoteNotFound

T = TypeVar("T")


class QueryableCollection(Generic[T], ABC):
    """Generic abstract base class for synchronous queryable collections."""

    def __init__(self, items: Sequence[T] | None = None) -> None:
        self._items: list[T] = list(items) if items is not None else []

    @staticmethod
    def _extract_id(item: Any) -> str:
        if isinstance(item, dict):
            return str(item.get("id") or "")
        return str(getattr(item, "id", None) or "")

    @staticmethod
    def _extract_name(item: Any) -> str:
        if isinstance(item, dict):
            return str(item.get("name") or item.get("title") or "")
        return str(getattr(item, "name", None) or getattr(item, "title", None) or "")

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
    def _get_from(cls, items: Sequence[T], item_id: str) -> T:
        for it in items:
            if cls._extract_id(it) == item_id:
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
        if not (startswith or endswith):
            exact_id = [it for it in items if cls._extract_id(it) == query]
            if len(exact_id) == 1:
                return exact_id[0]

            exact_name = [
                it for it in items if cls._matches(cls._extract_name(it), query, exact=True)
            ]
            if len(exact_name) == 1:
                return exact_name[0]
            if len(exact_name) > 1:
                ids = [cls._extract_id(it) for it in exact_name]
                raise AmbiguousMatchError(f"Multiple exact matches were found: {', '.join(ids)}")

        matches = [
            it
            for it in items
            if cls._matches(
                cls._extract_name(it),
                query,
                exact=exact,
                startswith=startswith,
                endswith=endswith,
            )
            or cls._matches(
                cls._extract_id(it),
                query,
                exact=exact,
                startswith=startswith,
                endswith=endswith,
            )
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            ids = [cls._extract_id(it) for it in matches]
            raise AmbiguousMatchError(
                f"Search is ambiguous; multiple matches were found: {', '.join(ids)}"
            )

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
        return [
            it
            for it in items
            if cls._matches(
                cls._extract_name(it),
                query,
                exact=exact,
                startswith=startswith,
                endswith=endswith,
            )
            or cls._matches(
                cls._extract_id(it),
                query,
                exact=exact,
                startswith=startswith,
                endswith=endswith,
            )
        ]

    def list(self) -> list[T]:
        return list(self._items)

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

    def __contains__(self, item: Any) -> bool:
        target_id = self._extract_id(item) if not isinstance(item, str) else item
        return any(self._extract_id(it) == target_id for it in self._items)


class ManageableCollection(QueryableCollection[T], ABC):
    """Generic abstract base class for collections with CRUD lifecycle operations."""

    async def refresh(self) -> list[T]:
        return list(self._items)

    @abstractmethod
    def create(self, *args: Any, **kwargs: Any) -> Any:
        """Create a new item in the collection."""

    @abstractmethod
    async def delete(self, target: T | str, /, *args: Any, **kwargs: Any) -> Any:
        """Delete an item from the collection and its remote backend."""

    @abstractmethod
    def update(self, target: T | str, /, **fields: Any) -> Any:
        """Update an existing item in the collection."""


class AsyncQueryableCollection(Generic[T], ABC):
    """Generic abstract base class for asynchronous queryable collections."""

    @abstractmethod
    async def list(self, *args: Any, **kwargs: Any) -> list[T]:
        """Return all items in the collection asynchronously."""

    async def get(self, item_id: str, /) -> T:
        return QueryableCollection._get_from(await self.list(), item_id)

    async def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> T:
        return QueryableCollection._find_in(
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
        return QueryableCollection._search_in(
            await self.list(),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )
