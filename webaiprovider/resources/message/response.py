from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .message import AsyncMessage, Message


@dataclass(frozen=True)
class Source:
    id: str
    url: str
    title: str = ""
    snippet: str = ""
    kind: str = "web"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Artifact:
    id: str
    kind: str
    name: str
    mime_type: str = "application/octet-stream"
    size: int | None = None
    download_url: str | None = None
    content: bytes | str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TimingMetrics:
    started_at: float = 0.0
    first_token_at: float | None = None
    completed_at: float | None = None
    first_token_ms: float | None = None
    duration_ms: float = 0.0
    metrics: Mapping[str, float] = field(default_factory=dict)


class Response:
    def __init__(
        self,
        message: AsyncMessage | Message,
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
