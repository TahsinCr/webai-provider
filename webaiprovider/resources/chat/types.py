from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..common import Origin
from ..message.types import MessageData


@dataclass(frozen=True)
class Conversation:
    origin: Origin
    title: str
    messages: tuple[MessageData, ...] = ()
    project_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)
