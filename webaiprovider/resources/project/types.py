from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..common import Choice, Origin, ResourceData


@dataclass(frozen=True)
class ProjectData:
    origin: Origin
    name: str
    instructions: str | None = None
    resources: tuple[ResourceData, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class ProjectOptions:
    fields: tuple[str, ...] = ()
    visibility: tuple[Choice, ...] = ()
    memory: tuple[Choice, ...] = ()
    sources: tuple[dict, ...] = ()
    remove_sources: bool = False
    source_error: str | None = None


def source_file(path: Any, *, error: str = "Source file must not exceed 20 MiB.") -> Path:
    file = Path(path).expanduser().resolve(strict=True)
    if not file.is_file() or file.stat().st_size > 20 * 1024 * 1024:
        raise ValueError(error)
    return file
