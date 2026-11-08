from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from ..resources.attachment import Attachment
from .base import QueryableCollection


class Attachments(QueryableCollection[Attachment]):
    def __init__(self, items: Iterable[Attachment] | None = None) -> None:
        super().__init__(list(items) if items is not None else None)

    def create(
        self,
        item: Attachment | Path | str | bytes,
        *,
        name: str | None = None,
        mime_type: str | None = None,
    ) -> Attachment:
        """Prepare an attachment and add it to this collection."""
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

    def update(
        self,
        target: Attachment | str,
        *,
        name: str | None = None,
        mime_type: str | None = None,
    ) -> Attachment:
        """Update an attachment's display name or media type."""
        item_id = target.id if isinstance(target, Attachment) else target
        attachment = self.get(item_id)
        if name is not None:
            attachment.name = name
        if mime_type is not None:
            attachment.mime_type = mime_type
        return attachment

    def delete(self, target: Attachment | str) -> bool:
        """Detach an item from this collection without deleting its source file."""
        item_id = target.id if isinstance(target, Attachment) else target
        for index, item in enumerate(self._items):
            if item.id == item_id:
                del self._items[index]
                return True
        return False

    @property
    def total_size(self) -> int:
        return sum(it.size for it in self._items)
