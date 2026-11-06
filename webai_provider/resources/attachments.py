from __future__ import annotations

import mimetypes
import os
import uuid
from pathlib import Path
from typing import Any, Iterable, Iterator


class Attachment:
    def __init__(
        self,
        id: str,
        name: str,
        *,
        path: Path | str | None = None,
        content: bytes | None = None,
        mime_type: str | None = None,
        size: int | None = None,
    ) -> None:
        self.id = id
        self.name = name
        self.path = Path(path).resolve() if path is not None else None
        self._content = content

        if mime_type is not None:
            self.mime_type = mime_type
        elif self.path is not None:
            guessed, _ = mimetypes.guess_type(str(self.path))
            self.mime_type = guessed or "application/octet-stream"
        else:
            guessed, _ = mimetypes.guess_type(name)
            self.mime_type = guessed or "application/octet-stream"

        if size is not None:
            self.size = size
        elif self._content is not None:
            self.size = len(self._content)
        elif self.path is not None and self.path.exists():
            self.size = os.path.getsize(self.path)
        else:
            self.size = 0

    @classmethod
    def from_path(
        cls,
        path: Path | str,
        *,
        name: str | None = None,
        mime_type: str | None = None,
    ) -> Attachment:
        p = Path(path).resolve()
        if not p.exists() or not p.is_file():
            raise FileNotFoundError(f"Dosya bulunamadı: {p}")
        return cls(
            id=str(uuid.uuid4()),
            name=name or p.name,
            path=p,
            mime_type=mime_type,
            size=os.path.getsize(p),
        )

    @classmethod
    def from_bytes(
        cls,
        data: bytes,
        name: str,
        *,
        mime_type: str | None = None,
    ) -> Attachment:
        return cls(
            id=str(uuid.uuid4()),
            name=name,
            content=data,
            mime_type=mime_type,
            size=len(data),
        )

    def read_bytes(self) -> bytes:
        if self._content is not None:
            return self._content
        if self.path is not None:
            return self.path.read_bytes()
        return b""


class Attachments:
    def __init__(self, items: Iterable[Attachment] | None = None) -> None:
        self._items: list[Attachment] = list(items) if items is not None else []

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
                raise ValueError("Bayt verisi eklendiğinde dosya adı belirtilmelidir")
            att = Attachment.from_bytes(item, name, mime_type=mime_type)
            self._items.append(att)
            return att
        if isinstance(item, (Path, str)):
            att = Attachment.from_path(item, name=name, mime_type=mime_type)
            self._items.append(att)
            return att
        raise TypeError(f"Desteklenmeyen eklenti türü: {type(item)}")

    def extend(self, items: Iterable[Attachment | Path | str | bytes]) -> list[Attachment]:
        added: list[Attachment] = []
        for it in items:
            added.append(self.add(it))
        return added

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

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[Attachment]:
        return iter(self._items)

    def __getitem__(self, index: int) -> Attachment:
        return self._items[index]

    def __contains__(self, item_or_id: Any) -> bool:
        if isinstance(item_or_id, Attachment):
            return item_or_id in self._items
        target = str(item_or_id)
        return any(it.id == target or it.name == target for it in self._items)
