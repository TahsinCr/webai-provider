from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class ModelRef:
    provider: str
    id: str
    name: str = ""
    capabilities: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EffortRef:
    provider: str
    id: str
    name: str = ""
    value: int | str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Source:
    id: str
    url: str
    title: str = ""
    snippet: str = ""
    kind: str = "web"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Attribution:
    source_id: str
    start_index: int
    end_index: int
    text_segment: str = ""


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


@dataclass(frozen=True)
class StreamEvent:
    event: str = field(init=False, default="")


@dataclass(frozen=True)
class ThinkingBegin(StreamEvent):
    event: str = field(init=False, default="thinking.begin")


@dataclass(frozen=True)
class ThinkingDelta(StreamEvent):
    delta: str = ""
    event: str = field(init=False, default="thinking.delta")


@dataclass(frozen=True)
class ThinkingEnd(StreamEvent):
    text: str = ""
    event: str = field(init=False, default="thinking.end")


@dataclass(frozen=True)
class OutputBegin(StreamEvent):
    event: str = field(init=False, default="output.begin")


@dataclass(frozen=True)
class OutputDelta(StreamEvent):
    delta: str = ""
    event: str = field(init=False, default="output.delta")


@dataclass(frozen=True)
class OutputReplace(StreamEvent):
    text: str = ""
    event: str = field(init=False, default="output.replace")


@dataclass(frozen=True)
class OutputEnd(StreamEvent):
    text: str = ""
    event: str = field(init=False, default="output.end")


@dataclass(frozen=True)
class SourceDiscovered(StreamEvent):
    source: Source = field(default_factory=lambda: Source("", ""))
    event: str = field(init=False, default="source.discovered")


@dataclass(frozen=True)
class ArtifactDiscovered(StreamEvent):
    artifact: Artifact = field(default_factory=lambda: Artifact("", "", ""))
    event: str = field(init=False, default="artifact.discovered")


@dataclass(frozen=True)
class TerminalStatus(StreamEvent):
    status: str = "completed"
    error: Any = None
    event: str = field(init=False, default="status.terminal")
