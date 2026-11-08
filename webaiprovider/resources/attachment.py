from __future__ import annotations

import mimetypes
import os
import uuid
from pathlib import Path


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
        resolved = Path(path).resolve()
        if not resolved.exists() or not resolved.is_file():
            raise FileNotFoundError(f"File was not found: {resolved}")
        return cls(
            id=str(uuid.uuid4()),
            name=name or resolved.name,
            path=resolved,
            mime_type=mime_type,
            size=os.path.getsize(resolved),
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
