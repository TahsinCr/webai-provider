from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..artifact import Artifact, ArtifactsCollection
from ..source import Source, SourcesCollection

if TYPE_CHECKING:
    from .message import AsyncMessage, Message


@dataclass(frozen=True)
class TimingMetrics:
    started_at: float = 0.0
    first_token_at: float | None = None
    completed_at: float | None = None
    first_token_ms: float | None = None
    duration_ms: float = 0.0
    metrics: Mapping[str, float] = field(default_factory=dict)


class Response:
    """High-level assistant response view carrying message, sources, artifacts, and optional timing."""

    def __init__(
        self,
        message: AsyncMessage | Message,
        *,
        sources: SourcesCollection | Sequence[Source] | None = None,
        artifacts: ArtifactsCollection | Sequence[Artifact] | None = None,
        metadata: Mapping[str, Any] | None = None,
        timing: TimingMetrics | None = None,
    ) -> None:
        self.message = message
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

    @property
    def text(self) -> str:
        return self.message.content

    def __repr__(self) -> str:
        preview = self.text[:60] + ("..." if len(self.text) > 60 else "")
        return (
            f"<Response text={preview!r} sources={len(self.sources)} "
            f"artifacts={len(self.artifacts)}>"
        )
