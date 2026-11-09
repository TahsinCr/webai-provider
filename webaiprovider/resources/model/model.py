from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import KW_ONLY, dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Mapping, TypeVar, cast

from ..common import Choice

if TYPE_CHECKING:
    from .collections import Models


class ModelCatalogState(str, Enum):
    AVAILABLE = "available"
    UNKNOWN = "unknown"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class EffortRef:
    provider: str
    model_id: str
    id: str
    name: str = ""
    rank: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


class EffortSet:
    DEFAULT_EFFORT_ID: ClassVar[str | None] = None

    @classmethod
    def list(cls) -> tuple[EffortRef, ...]:
        return tuple(value for value in vars(cls).values() if isinstance(value, EffortRef))

    @classmethod
    def from_refs(
        cls,
        refs: Iterable[EffortRef],
        *,
        name: str = "DiscoveredEfforts",
        default_id: str | None = None,
    ) -> type[EffortSet]:
        members: dict[str, Any] = {
            "__module__": cls.__module__,
            "DEFAULT_EFFORT_ID": default_id,
        }
        for ref in refs:
            key = re.sub(r"\W+", "_", ref.name or ref.id).strip("_").upper() or "EFFORT"
            if key[0].isdigit():
                key = f"VALUE_{key}"
            base = key
            suffix = 2
            while key in members:
                key = f"{base}_{suffix}"
                suffix += 1
            members[key] = ref

        class_name = re.sub(r"\W+", "_", name)
        if not class_name.isidentifier():
            class_name = "DiscoveredEfforts"
        return type(class_name, (cls,), members)

    @classmethod
    def get(cls, effort_id: str) -> EffortRef:
        for effort in cls.list():
            if effort.id == effort_id:
                return effort
        from ...errors import RemoteNotFound

        raise RemoteNotFound(f"Effort ID {effort_id!r} is not available")

    @classmethod
    def find(cls, query: str) -> EffortRef:
        from .collections import SelectionResolver

        result = SelectionResolver.resolve(
            query,
            cls.list(),
            default_id=cls.DEFAULT_EFFORT_ID,
        )
        if not isinstance(result, EffortRef):
            raise TypeError("Effort query did not resolve to an effort")
        return result

    @classmethod
    def search(cls, query: str) -> list[EffortRef]:
        query = query.casefold()
        return [
            effort
            for effort in cls.list()
            if query in effort.id.casefold() or query in effort.name.casefold()
        ]


E = TypeVar("E", bound=EffortSet)


@dataclass(frozen=True)
class ModelRef(Generic[E]):
    provider: str
    id: str
    name: str = ""
    _: KW_ONLY
    efforts: type[E] = field(default=cast(type[E], EffortSet), repr=False)
    rank: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.efforts, type) or not issubclass(self.efforts, EffortSet):
            raise TypeError("Model efforts must be an EffortSet class")
        efforts = self.efforts.list()
        if any(e.provider != self.provider or e.model_id != self.id for e in efforts):
            raise ValueError("Each effort must belong to its model and provider")
        if self.default_effort_id is not None and not any(
            effort.id == self.default_effort_id for effort in efforts
        ):
            raise ValueError("Default effort must be present in the model's effort catalog")

    @property
    def default_effort_id(self) -> str | None:
        return self.efforts.DEFAULT_EFFORT_ID

    def select(self, effort: EffortRef | Models | str | None = None) -> ModelSelection:
        if effort is None or (isinstance(effort, str) and effort.casefold() == "auto"):
            return ModelSelection(self)

        from .collections import Models, SelectionResolver

        if isinstance(effort, Models) and effort is Models.AUTO:
            return ModelSelection(self)
        resolved = SelectionResolver.resolve(
            effort,
            self.efforts.list(),
            default_id=self.default_effort_id,
        )
        if not isinstance(resolved, EffortRef):
            raise TypeError("Effort selection did not resolve to an effort")
        return ModelSelection(self, resolved)


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
class Model:
    id: str
    title: str
    efforts: tuple[Choice, ...] = ()
    default_effort: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)
    chat_selectable: bool = True
    chat_title: str | None = None


@dataclass(frozen=True)
class ModelCatalog:
    state: ModelCatalogState
    models: tuple[Model, ...] = ()
    default_model: str | None = None
    reason: str | None = None

    @property
    def chat_models(self) -> tuple[Model, ...]:
        return tuple(model for model in self.models if model.chat_selectable)

    def chat_model(self, model_id: str | None) -> Model | None:
        return next(
            (model for model in self.models if model.chat_selectable and model.id == model_id), None
        )

    @property
    def default_chat_model(self) -> Model | None:
        return next(
            (model for model in self.chat_models if model.id == self.default_model),
            next(iter(self.chat_models), None),
        )

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ModelCatalog":
        return cls(
            ModelCatalogState(value["state"]),
            tuple(
                Model(
                    item["id"],
                    item["title"],
                    tuple(Choice(**effort) for effort in item.get("efforts", [])),
                    item.get("default_effort"),
                    item.get("metadata", {}),
                    item.get("chat_selectable", True),
                    item.get("chat_title"),
                )
                for item in value.get("models", [])
            ),
            value.get("default_model"),
            value.get("reason"),
        )

    def effort(self, model_id: str, preferred: str = "medium") -> str | None:
        model = next((m for m in self.models if m.id == model_id), None)
        if model is None:
            return None
        supported = {e.id for e in model.efforts}
        if model.default_effort is None and preferred == "medium":
            return None
        if preferred in supported:
            return preferred
        return model.default_effort if model.default_effort in supported else None
