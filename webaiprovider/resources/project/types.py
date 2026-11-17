from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..message.attachment import Attachment


@dataclass(frozen=True)
class ProjectOption:
    id: str
    title: str


@dataclass(frozen=True)
class ProjectData:
    id: str
    name: str
    instructions: str | None = None
    resources: tuple[Attachment, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class ProjectOptions:
    fields: tuple[str, ...] = ()
    visibility: tuple[ProjectOption, ...] = ()
    memory: tuple[ProjectOption, ...] = ()
    sources: tuple[dict, ...] = ()
    remove_sources: bool = False
    source_error: str | None = None


def source_file(path: Any, *, error: str = "Source file must not exceed 20 MiB.") -> Path:
    file = Path(path).expanduser().resolve(strict=True)
    if not file.is_file() or file.stat().st_size > 20 * 1024 * 1024:
        raise ValueError(error)
    return file
