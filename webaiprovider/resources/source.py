from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from ..collections import ReadOnlyCollection


@dataclass(frozen=True)
class Source:
    """Web search or document citation attached to an assistant response."""

    id: str
    url: str
    title: str = ""
    snippet: str = ""
    kind: str = "web"
    metadata: Mapping[str, Any] = field(default_factory=dict)


class SourcesCollection(ReadOnlyCollection[Source]):
    """Synchronous read-only local collection of response citations."""

    @classmethod
    def _extract_name(cls, item: Any) -> str:
        if isinstance(item, Source):
            return item.title or item.url or item.id
        return super()._extract_name(item)
