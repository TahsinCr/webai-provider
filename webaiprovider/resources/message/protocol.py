"""Normalized request and low-level provider events emitted during streaming."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .types import MessageAction

if TYPE_CHECKING:
    from ..response.parts import ResponsePart
    from ..response.research import ResearchPlan, ResearchState, ResearchStatus
    from ..tools.types import Tool

__all__ = [
    "ProviderRequest",
    "ProviderStreamEvent",
    "TextDelta",
    "ThoughtDelta",
    "TextReplacement",
    "ResponseComplete",
    "PartClosed",
    "ResponseVerifying",
    "ConversationBound",
    "MessageMetadata",
    "ResearchPlanReceived",
    "ResearchProgress",
]


@dataclass(frozen=True)
class ProviderRequest:
    prompt: str
    conversation_id: str | None = None
    model: str | None = None
    effort: str | None = None
    project_id: str | None = None
    temporary: bool = False
    attachments: tuple[dict, ...] = ()
    action: MessageAction | None = None
    message_id: str | None = None
    draft_title: str | None = None
    tool: Tool | None = None
    tool_timeout: float | None = None
    tool: Tool | None = None
    tool_timeout: float | None = None


@dataclass(frozen=True)
class TextDelta:
    text: str
    channel: str | None = None
    part_id: str | None = None


@dataclass(frozen=True)
class ThoughtDelta:
    text: str
    part_id: str | None = None


@dataclass(frozen=True)
class TextReplacement:
    text: str
    channel: str | None = None
    part_id: str | None = None


@dataclass(frozen=True)
class PartClosed:
    part_id: str
    channel: str | None = None


@dataclass(frozen=True)
class ResponseVerifying:
    pass


@dataclass(frozen=True)
class ResponseComplete:
    pass


@dataclass(frozen=True)
class ConversationBound:
    conversation_id: str


@dataclass(frozen=True)
class MessageMetadata:
    message_id: str | None = None
    user_message_id: str | None = None
    model: str | None = None
    effort: str | None = None
    sources: tuple[dict, ...] = ()
    files: tuple[dict, ...] = ()
    parts: tuple[ResponsePart, ...] = ()
    research: ResearchState | None = None


@dataclass(frozen=True)
class ResearchPlanReceived:
    plan: ResearchPlan
    state: ResearchState


@dataclass(frozen=True)
class ResearchProgress:
    status: ResearchStatus
    message: str = ""
    state: ResearchState | None = None


ProviderStreamEvent = (
    TextDelta
    | ThoughtDelta
    | TextReplacement
    | ResponseComplete
    | PartClosed
    | ResponseVerifying
    | ConversationBound
    | MessageMetadata
    | ResearchPlanReceived
    | ResearchProgress
)
