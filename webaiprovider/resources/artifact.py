from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from ..collections import ReadOnlyCollection


@dataclass(frozen=True)
class Artifact:
    """Generated file or rich output associated with an assistant response."""

    id: str
    kind: str
    name: str
    mime_type: str = "application/octet-stream"
    size: int | None = None
    download_url: str | None = None
    content: bytes | str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


class ArtifactsCollection(ReadOnlyCollection[Artifact]):
    """Synchronous read-only local collection of response artifacts."""
