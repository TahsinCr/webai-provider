from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Choice:
    id: str
    title: str


@dataclass(frozen=True)
class Origin:
    provider_id: str
    account_id: str
    remote_id: str


@dataclass(frozen=True)
class ResourceData:
    id: str
    name: str
    media_type: str | None = None
    remote_url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)
