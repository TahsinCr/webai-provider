from __future__ import annotations

from typing import Generic, Sequence, TypeVar

from ..errors import AmbiguousMatchError, RemoteNotFound

T = TypeVar("T")


class ResourceCollection(Generic[T]):
    def __init__(self, items: Sequence[T] | None = None) -> None:
        self._items: list[T] = list(items) if items is not None else []

    def list(self) -> list[T]:
        return list(self._items)

    def get(self, item_id: str) -> T:
        for it in self._items:
            if getattr(it, "id", None) == item_id:
                return it
        raise RemoteNotFound(f"Kayıt bulunamadı: {item_id}")

    def find(self, query: str) -> T:
        query_lower = query.lower()
        exact = [
            it
            for it in self._items
            if getattr(it, "id", None) == query
            or str(getattr(it, "title", "")).lower() == query_lower
            or str(getattr(it, "name", "")).lower() == query_lower
        ]
        if len(exact) == 1:
            return exact[0]
        if len(exact) > 1:
            ids = [str(getattr(it, "id", "")) for it in exact]
            raise AmbiguousMatchError(f"Birden fazla kayıt eşleşti: {', '.join(ids)}")

        partial = [
            it
            for it in self._items
            if query_lower in str(getattr(it, "title", "")).lower()
            or query_lower in str(getattr(it, "name", "")).lower()
        ]
        if len(partial) == 1:
            return partial[0]
        if len(partial) > 1:
            ids = [str(getattr(it, "id", "")) for it in partial]
            raise AmbiguousMatchError(
                f"Arama belirsiz, birden çok eşleşme bulundu: {', '.join(ids)}"
            )

        raise RemoteNotFound(f"Aramayla eşleşen kayıt bulunamadı: {query}")

    def search(self, query: str) -> list[T]:
        query_lower = query.lower()
        return [
            it
            for it in self._items
            if query_lower in str(getattr(it, "title", "")).lower()
            or query_lower in str(getattr(it, "name", "")).lower()
            or query == getattr(it, "id", "")
        ]

    def __len__(self) -> int:
        return len(self._items)
