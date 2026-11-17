from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ...errors import UnsupportedOperationError
from .artifact import Artifact, ArtifactsCollection
from .source import Source, SourcesCollection

if TYPE_CHECKING:
    from ..chat.chat import AsyncChat, Chat
    from ..message.message import AsyncMessage, Message


@dataclass(frozen=True)
class TimingMetrics:
    started_at: float = 0.0
    first_token_at: float | None = None
    completed_at: float | None = None
    first_token_ms: float | None = None
    duration_ms: float = 0.0
    metrics: Mapping[str, float] = field(default_factory=dict)


class Response:
    """Assistant response turn pairing generated output with its triggering user message."""

    def __init__(
        self,
        content: str = "",
        *,
        message: AsyncMessage | Message | None = None,
        id: str | None = None,
        remote_id: str | None = None,
        model: Any = None,
        sources: SourcesCollection | Sequence[Source] | None = None,
        artifacts: ArtifactsCollection | Sequence[Artifact] | None = None,
        metadata: Mapping[str, Any] | None = None,
        timing: TimingMetrics | None = None,
        chat: AsyncChat | Chat | None = None,
    ) -> None:
        self.id = id or remote_id or str(uuid.uuid4())
        self.remote_id = remote_id
        self.content = content
        self.message = message
        self.model = (
            model if model is not None else (message.model if message is not None else None)
        )
        self.sources: SourcesCollection = (
            sources if isinstance(sources, SourcesCollection) else SourcesCollection(sources)
        )
        self.artifacts: ArtifactsCollection = (
            artifacts
            if isinstance(artifacts, ArtifactsCollection)
            else ArtifactsCollection(artifacts)
        )
        self.metadata: dict[str, Any] = dict(metadata) if metadata is not None else {}
        self.timing: TimingMetrics | None = timing
        self.chat: AsyncChat | Chat | None = (
            chat if chat is not None else (message.chat if message is not None else None)
        )

    @property
    def text(self) -> str:
        return self.content

    def retry(
        self,
        *,
        model: Any = None,
        **options: Any,
    ) -> Any:
        chat = self.chat
        if chat is None:
            raise UnsupportedOperationError(
                "Cannot retry a response that does not belong to a conversation"
            )
        return chat.responses.retry(self, model=model, **options)

    def branch(self, *, title: str | None = None) -> Any:
        chat = self.chat
        if chat is None:
            raise UnsupportedOperationError(
                "Cannot branch a response that does not belong to a conversation"
            )
        branch_fn = getattr(chat, "branch", None)
        if not callable(branch_fn):
            raise UnsupportedOperationError("This conversation does not support branching")
        target_id = self.remote_id or self.id
        if not target_id and self.message is not None:
            target_id = self.message.remote_id or self.message.id
        if title is None:
            return branch_fn(target_id)
        return branch_fn(target_id, title=title)

    def __repr__(self) -> str:
        preview = self.text[:60] + ("..." if len(self.text) > 60 else "")
        return (
            f"<Response text={preview!r} sources={len(self.sources)} "
            f"artifacts={len(self.artifacts)}>"
        )
