from __future__ import annotations

from typing import Any, Generic, Sequence, TypeVar

from ..errors import AmbiguousMatchError, RemoteNotFound

T = TypeVar("T")


class QueryableCollection(Generic[T]):
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

    def list(self) -> list[T]:
        return list(self._items)

    def get(self, item_id: str) -> T:
        for it in self._items:
            if self._extract_id(it) == item_id:
                return it
        raise RemoteNotFound(f"Kayıt bulunamadı: {item_id}")

    def find(
        self,
        query: str,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> T:
        if not (startswith or endswith):
            exact_id = [it for it in self._items if self._extract_id(it) == query]
            if len(exact_id) == 1:
                return exact_id[0]

            exact_name = [
                it for it in self._items if self._matches(self._extract_name(it), query, exact=True)
            ]
            if len(exact_name) == 1:
                return exact_name[0]
            if len(exact_name) > 1:
                ids = [self._extract_id(it) for it in exact_name]
                raise AmbiguousMatchError(f"Birden fazla tam eşleşme bulundu: {', '.join(ids)}")

        matches = [
            it
            for it in self._items
            if self._matches(
                self._extract_name(it),
                query,
                exact=exact,
                startswith=startswith,
                endswith=endswith,
            )
            or self._matches(
                self._extract_id(it),
                query,
                exact=exact,
                startswith=startswith,
                endswith=endswith,
            )
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            ids = [self._extract_id(it) for it in matches]
            raise AmbiguousMatchError(
                f"Arama belirsiz, birden çok eşleşme bulundu: {', '.join(ids)}"
            )

        raise RemoteNotFound(f"Aramayla eşleşen kayıt bulunamadı: {query}")

    def search(
        self,
        query: str,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> list[T]:
        return [
            it
            for it in self._items
            if self._matches(
                self._extract_name(it),
                query,
                exact=exact,
                startswith=startswith,
                endswith=endswith,
            )
            or self._matches(
                self._extract_id(it),
                query,
                exact=exact,
                startswith=startswith,
                endswith=endswith,
            )
        ]

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self):
        return iter(self._items)

    def __contains__(self, item: Any) -> bool:
        target_id = self._extract_id(item) if not isinstance(item, str) else item
        return any(self._extract_id(it) == target_id for it in self._items)


class ManageableCollection(QueryableCollection[T]):
    async def refresh(self) -> list[T]:
        return list(self._items)

    async def delete(self, target: T | str) -> None:
        target_id = self._extract_id(target) if not isinstance(target, str) else target
        self._items = [it for it in self._items if self._extract_id(it) != target_id]

    async def update(self, target: T | str, **fields: Any) -> Any:
        raise NotImplementedError


ResourceCollection = QueryableCollection
