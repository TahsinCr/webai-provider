from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from .catalogs.policies import Models


@dataclass(frozen=True)
class EffortRef:
    provider: str
    model_id: str
    id: str
    name: str = ""
    rank: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ModelRef:
    provider: str
    id: str
    name: str = ""
    efforts: Sequence[EffortRef] = field(default=(), repr=False)
    default_effort_id: str | None = None
    rank: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        efforts = tuple(self.efforts)
        if any(e.provider != self.provider or e.model_id != self.id for e in efforts):
            raise ValueError("Each effort must belong to its model and provider")
        if self.default_effort_id is not None and not any(
            effort.id == self.default_effort_id for effort in efforts
        ):
            raise ValueError("Default effort must be present in the model's effort catalog")
        object.__setattr__(self, "efforts", EffortChoices(self, efforts))

    def select(self, effort: EffortRef | Models | str | None = None) -> ModelSelection:
        if effort is None or (isinstance(effort, str) and effort.casefold() == "auto"):
            return ModelSelection(self)

        from .catalogs.policies import Models
        from .catalogs.resolver import SelectionResolver

        if isinstance(effort, Models) and effort is Models.AUTO:
            return ModelSelection(self)
        resolved = SelectionResolver.resolve(
            effort,
            self.efforts,
            default_id=self.default_effort_id,
        )
        if not isinstance(resolved, EffortRef):
            raise TypeError("Effort selection did not resolve to an effort")
        return ModelSelection(self, resolved)

    def __getattr__(self, name: str) -> ModelSelection:
        if name.startswith("_"):
            raise AttributeError(name)
        from .errors import RemoteNotFound

        try:
            return self.select(name.replace("_", " "))
        except RemoteNotFound as exc:
            raise AttributeError(name) from exc


class EffortChoices(Sequence[EffortRef]):
    def __init__(self, model: ModelRef, items: Sequence[EffortRef]) -> None:
        self._model = model
        self._items = tuple(items)

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, index: int | slice) -> EffortRef | tuple[EffortRef, ...]:
        return self._items[index]

    def __iter__(self):
        return iter(self._items)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Sequence):
            return self._items == tuple(other)
        return NotImplemented

    def list(self) -> list[EffortRef]:
        return list(self._items)

    def get(self, effort_id: str) -> EffortRef:
        for effort in self._items:
            if effort.id == effort_id:
                return effort
        from .errors import RemoteNotFound

        raise RemoteNotFound(f"Effort ID {effort_id!r} is not available for {self._model.id}")

    def find(self, query: str) -> EffortRef:
        from .catalogs.resolver import SelectionResolver

        result = SelectionResolver.resolve(
            query,
            self._items,
            default_id=self._model.default_effort_id,
        )
        if not isinstance(result, EffortRef):
            raise TypeError("Effort query did not resolve to an effort")
        return result

    def search(self, query: str) -> list[EffortRef]:
        query = query.casefold()
        return [
            effort
            for effort in self._items
            if query in effort.id.casefold() or query in effort.name.casefold()
        ]

    def __getattr__(self, name: str) -> ModelSelection:
        if name.startswith("_"):
            raise AttributeError(name)
        from .errors import RemoteNotFound

        try:
            return self._model.select(name.replace("_", " "))
        except RemoteNotFound as exc:
            raise AttributeError(name) from exc


@dataclass(frozen=True)
class ModelSelection:
    model: ModelRef
    effort: EffortRef | None = None

    def __post_init__(self) -> None:
        if self.effort is not None and (
            self.effort.provider != self.model.provider or self.effort.model_id != self.model.id
        ):
            raise ValueError("Selected effort must belong to the selected model")

    @property
    def provider(self) -> str:
        return self.model.provider

    @property
    def id(self) -> str:
        return self.model.id

    @property
    def name(self) -> str:
        return self.model.name


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
