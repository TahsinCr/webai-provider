from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Sequence

from ..contracts import Artifact, Source, TimingMetrics

if TYPE_CHECKING:
    from .message import Message


class Response:
    def __init__(
        self,
        message: Message,
        *,
        sources: Sequence[Source] | None = None,
        artifacts: Sequence[Artifact] | None = None,
        metadata: Mapping[str, Any] | None = None,
        timing: TimingMetrics | None = None,
    ) -> None:
        self.message = message
        self.sources: tuple[Source, ...] = tuple(sources) if sources is not None else ()
        self.artifacts: tuple[Artifact, ...] = tuple(artifacts) if artifacts is not None else ()
        self.metadata: dict[str, Any] = dict(metadata) if metadata is not None else {}
        self.timing: TimingMetrics = timing or TimingMetrics()

    @property
    def text(self) -> str:
        return self.message.content

    def __repr__(self) -> str:
        preview = self.text[:60] + ("..." if len(self.text) > 60 else "")
        return f"<Response text={preview!r} sources={len(self.sources)} artifacts={len(self.artifacts)}>"
