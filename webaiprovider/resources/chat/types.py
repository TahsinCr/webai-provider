from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..message.types import MessageData
from ..project.types import ProjectData

DEFAULT_CHAT_TITLE = "Yeni Sohbet"


@dataclass(frozen=True)
class Conversation:
    id: str
    title: str
    messages: tuple[MessageData, ...] = ()
    project: ProjectData | None = None
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)
