from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from ..resources.attachment import Attachment
from .base import QueryableCollection


class Attachments(QueryableCollection[Attachment]):
    def __init__(self, items: Iterable[Attachment] | None = None) -> None:
        super().__init__(list(items) if items is not None else None)

    def add(
        self,
        item: Attachment | Path | str | bytes,
        *,
        name: str | None = None,
        mime_type: str | None = None,
    ) -> Attachment:
        if isinstance(item, Attachment):
            self._items.append(item)
            return item
        if isinstance(item, bytes):
            if not name:
                raise ValueError("A filename is required when attaching byte data")
            att = Attachment.from_bytes(item, name, mime_type=mime_type)
            self._items.append(att)
            return att
        if isinstance(item, (Path, str)):
            att = Attachment.from_path(item, name=name, mime_type=mime_type)
            self._items.append(att)
            return att
        raise TypeError(f"Unsupported attachment type: {type(item)}")

    def extend(self, items: Iterable[Attachment | Path | str | bytes]) -> list[Attachment]:
        return [self.add(item) for item in items]

    def remove(self, item_or_id: Attachment | str) -> bool:
        target_id = item_or_id.id if isinstance(item_or_id, Attachment) else item_or_id
        for i, it in enumerate(self._items):
            if it.id == target_id or it.name == target_id:
                del self._items[i]
                return True
        return False

    def clear(self) -> None:
        self._items.clear()

    @property
    def total_size(self) -> int:
        return sum(it.size for it in self._items)

    def __getitem__(self, index: int) -> Attachment:
        return self._items[index]

    def __contains__(self, item_or_id: Any) -> bool:
        if isinstance(item_or_id, Attachment):
            return any(item is item_or_id for item in self._items)
        target = str(item_or_id)
        return any(it.id == target or it.name == target for it in self._items)
