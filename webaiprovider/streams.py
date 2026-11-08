"""Typed request and event contracts for provider implementations.

Providers interpret their protocol and yield these normalized values; AsyncProvider
owns the public operation lifecycle and emits the user-facing StreamEvent types.
"""

from __future__ import annotations

from dataclasses import dataclass

from .models import MessageAction

__all__ = [
    "ProviderRequest",
    "ProviderEvent",
    "TextDelta",
    "ThoughtDelta",
    "TextReplacement",
    "ResponseComplete",
    "ConversationBound",
    "MessageMetadata",
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


@dataclass(frozen=True)
class TextDelta:
    text: str
    channel: str | None = None


@dataclass(frozen=True)
class ThoughtDelta:
    text: str


@dataclass(frozen=True)
class TextReplacement:
    text: str
    channel: str | None = None


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


ProviderEvent = (
    TextDelta
    | ThoughtDelta
    | TextReplacement
    | ResponseComplete
    | ConversationBound
    | MessageMetadata
)
