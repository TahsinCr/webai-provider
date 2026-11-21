"""Unified Attachment model, collection, and provider upload policy helpers."""

from __future__ import annotations

import mimetypes
import os
import uuid
from collections.abc import Iterable
from copy import copy
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from ...collections import Collection


class Attachment:
    """Local or remote attachment associated with a user message or project."""

    def __init__(
        self,
        id: str,
        name: str,
        media_type: str | None = None,
        remote_url: str | None = None,
        *,
        path: Path | str | None = None,
        content: bytes | str | None = None,
        mime_type: str | None = None,
        size: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.id = id
        self.name = name
        self.path = Path(path).resolve() if path is not None else None
        self._content: bytes | str | None = content
        self.remote_url = remote_url
        self.metadata: dict[str, Any] = dict(metadata) if metadata is not None else {}
        self._explicit_media_type = mime_type if mime_type is not None else media_type

        if self._explicit_media_type is not None:
            self.mime_type = self._explicit_media_type
        elif self.path is not None:
            guessed, _ = mimetypes.guess_type(str(self.path))
            self.mime_type = guessed or "application/octet-stream"
        else:
            guessed, _ = mimetypes.guess_type(name)
            self.mime_type = guessed or "application/octet-stream"

        if size is not None:
            self.size = size
        elif self._content is not None:
            self.size = (
                len(self._content.encode("utf-8"))
                if isinstance(self._content, str)
                else len(self._content)
            )
        elif self.path is not None and self.path.exists():
            self.size = os.path.getsize(self.path)
        else:
            self.size = 0

    @property
    def media_type(self) -> str | None:
        """Explicit remote media type when present, or guessed MIME type for local content."""
        if self._explicit_media_type is not None:
            return self._explicit_media_type
        if self.path is not None or self._content is not None:
            return self.mime_type
        return None

    @property
    def content(self) -> bytes | str | None:
        return self._content

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Attachment):
            return NotImplemented
        return (
            self.id == other.id
            and self.name == other.name
            and self.media_type == other.media_type
            and self.remote_url == other.remote_url
            and self.path == other.path
            and self._content == other._content
            and self.metadata == other.metadata
        )

    def __repr__(self) -> str:
        return f"Attachment(id={self.id!r}, name={self.name!r}, media_type={self.media_type!r})"

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

    @classmethod
    def from_text(
        cls,
        text: str,
        name: str,
        *,
        mime_type: str = "text/plain",
        id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Attachment:
        return cls(
            id=id or str(uuid.uuid4()),
            name=name,
            content=text,
            mime_type=mime_type,
            metadata=metadata,
        )

    def read_bytes(self) -> bytes:
        if self._content is not None:
            return (
                self._content.encode("utf-8") if isinstance(self._content, str) else self._content
            )
        if self.path is not None:
            return self.path.read_bytes()
        return b""


class Attachments(Collection[Attachment]):
    """In-memory collection of attachments bound to a message or draft."""

    _mutable = True

    def __init__(
        self,
        items: Iterable[Attachment] | None = None,
        *,
        attachment_type: type[Attachment] = Attachment,
    ) -> None:
        super().__init__(items)
        self._attachment_type = attachment_type

    def create(
        self,
        item: Attachment | Path | str | bytes,
        *,
        name: str | None = None,
        mime_type: str | None = None,
    ) -> Attachment:
        """Prepare an attachment and add it to this collection."""
        if isinstance(item, Attachment):
            attachment = item
        elif isinstance(item, bytes):
            if not name:
                raise ValueError("A filename is required when attaching byte data")
            attachment = self._attachment_type.from_bytes(item, name, mime_type=mime_type)
        elif isinstance(item, (Path, str)):
            attachment = self._attachment_type.from_path(item, name=name, mime_type=mime_type)
        else:
            raise TypeError(f"Unsupported attachment type: {type(item)}")
        with self._write_lock:
            self._items = (*self._items, attachment)
        return attachment

    def update(
        self,
        target: Attachment | str,
        *,
        name: str | None = None,
        mime_type: str | None = None,
    ) -> Attachment:
        with self._write_lock:
            item_id = target.id if isinstance(target, Attachment) else target
            attachment = self.get(item_id)
            if name is None and mime_type is None:
                return attachment
            updated = copy(attachment)
            if name is not None:
                updated.name = name
            if mime_type is not None:
                updated.mime_type = mime_type
                updated._explicit_media_type = mime_type
            self._items = tuple(updated if item is attachment else item for item in self._items)
            return updated

    def delete(self, target: Attachment | str) -> bool:
        item_id = target.id if isinstance(target, Attachment) else target
        with self._write_lock:
            for index, item in enumerate(self._items):
                if item.id == item_id:
                    self._items = self._items[:index] + self._items[index + 1 :]
                    return True
        return False

    @property
    def total_size(self) -> int:
        return sum(it.size for it in self._items)


@dataclass(frozen=True)
class AttachmentPolicy:
    max_files: int
    max_bytes: int
    max_image_bytes: int | None = None
    max_spreadsheet_bytes: int | None = None
    max_video_bytes: int | None = None
    max_total_bytes: int | None = None
    allowed_extensions: tuple[str, ...] = ()
    max_files_scope: str = "prompt"
    source: str = "provider web transport"

    def as_dict(self):
        return asdict(self)


_MIB = 1024 * 1024
_GIB = 1024 * _MIB

# OpenAI documents 512 MiB per file, 20 MiB for images, and about 50 MiB for
# spreadsheets. Its rolling quota and plan allowance are account-specific.
CHATGPT_ATTACHMENTS = AttachmentPolicy(
    max_files=10,
    max_bytes=512 * _MIB,
    max_image_bytes=20 * _MIB,
    max_spreadsheet_bytes=50 * _MIB,
    source="ChatGPT web upload limits",
)

# Gemini Apps documents 10 files per prompt, 100 MB for non-video files and
# 2 GB per video. Its web client does not publish a cumulative byte limit.
GEMINI_ATTACHMENTS = AttachmentPolicy(
    max_files=10,
    max_bytes=100 * _MIB,
    max_video_bytes=2 * _GIB,
    source="Gemini Apps upload limits",
)

# Claude chat uploads: 30 MB per file and at most 20 files per chat.
CLAUDE_ATTACHMENTS = AttachmentPolicy(
    max_files=20,
    max_bytes=30 * _MIB,
    allowed_extensions=(
        ".pdf",
        ".docx",
        ".csv",
        ".txt",
        ".html",
        ".htm",
        ".odt",
        ".rtf",
        ".epub",
        ".json",
        ".xlsx",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
    ),
    max_files_scope="conversation",
    source="Claude chat upload limits",
)


GROK_ATTACHMENTS = AttachmentPolicy(
    max_files=10,
    max_bytes=3 * _MIB,
    max_total_bytes=10 * _MIB,
    allowed_extensions=(
        ".pdf",
        ".txt",
        ".md",
        ".csv",
        ".json",
        ".doc",
        ".docx",
        ".ppt",
        ".pptx",
        ".xls",
        ".xlsx",
        ".xlsm",
        ".odt",
        ".ods",
        ".odp",
        ".rtf",
    ),
    source="Grok chat file-upload endpoint with a conservative local size cap",
)


POLICIES = {
    "grok": GROK_ATTACHMENTS,
    "chatgpt": CHATGPT_ATTACHMENTS,
    "gemini": GEMINI_ATTACHMENTS,
    "claude": CLAUDE_ATTACHMENTS,
}


def policy_for(provider_id: str) -> AttachmentPolicy | None:
    return POLICIES.get(provider_id)


def fit_attachments(attachments, policy):
    """Keep the earliest attachments that fit a provider's current transport policy."""
    if policy is None:
        return [], list(attachments)
    if hasattr(policy, "as_dict"):
        policy = policy.as_dict()

    allowed = {str(value).lower() for value in policy.get("allowed_extensions") or ()}
    accepted, rejected = [], []
    for item in attachments:
        name = str(item.get("name") or Path(str(item.get("path") or "")).name)
        has_size = item.get("size_bytes") is not None
        size = int(item.get("size_bytes") or 0)
        if size < 0 or (has_size and size == 0):
            rejected.append(item)
            continue
        extension = Path(name).suffix.lower()
        if allowed and extension not in allowed:
            rejected.append(item)
            continue

        mime = item.get("mime_type") or mimetypes.guess_type(name)[0] or ""
        if mime.startswith("video/"):
            limit = policy.get("max_video_bytes")
        elif mime.startswith("image/"):
            limit = policy.get("max_image_bytes")
        elif extension in {".csv", ".tsv", ".xls", ".xlsx"}:
            limit = policy.get("max_spreadsheet_bytes")
        else:
            limit = None
        limit = int(limit or policy.get("max_bytes") or 20 * _MIB)
        if size and size > limit:
            rejected.append(item)
            continue
        accepted.append(item)

    max_files = max(1, int(policy.get("max_files") or 1))
    if len(accepted) > max_files:
        rejected.extend(accepted[max_files:])
        accepted = accepted[:max_files]

    max_total = policy.get("max_total_bytes")
    if max_total:
        max_total_bytes = int(max_total)
        total_bytes = sum(int(item.get("size_bytes") or 0) for item in accepted)
        while accepted and total_bytes > max_total_bytes:
            popped = accepted.pop()
            total_bytes -= int(popped.get("size_bytes") or 0)
            rejected.append(popped)
    return accepted, rejected
