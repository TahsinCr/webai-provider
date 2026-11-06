from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Generic, TypeVar

from .accounts.status import FeatureState, SessionState, SessionStatus
from .transformers.citations import files_from, sources_from

__all__ = [
    "Capability",
    "FeatureState",
    "SessionState",
    "SessionStatus",
    "Feature",
    "ProviderDescription",
    "Choice",
    "Model",
    "ModelCatalog",
    "Origin",
    "Resource",
    "Content",
    "Message",
    "Conversation",
    "Project",
    "ProjectOptions",
    "source_file",
    "MessageAction",
    "actions_for",
    "target_message",
    "sources_from",
    "files_from",
    "Page",
]


class Capability(str, Enum):
    CHAT = "chat"
    TEMPORARY = "temporary"
    HISTORY = "history"
    RENAME = "rename"
    DELETE = "delete"
    AUTH = "auth"
    ATTACHMENTS = "attachments"
    RESOURCES = "resources"
    PROJECTS = "projects"
    PROJECT_CREATE = "project_create"
    PROJECT_UPDATE = "project_update"
    PROJECT_DELETE = "project_delete"
    PROJECT_MEMBERSHIP = "project_membership"
    MODELS = "models"
    EFFORT = "effort"
    CONNECTORS = "connectors"
    MESSAGE_EDIT = "message_edit"
    MESSAGE_RETRY = "message_retry"
    CHAT_BRANCH = "chat_branch"


class MessageAction(str, Enum):
    EDIT = "edit"
    RETRY = "retry"
    BRANCH = "branch"


def actions_for(
    role: str, *, final: bool = True, latest: bool = True, branch: bool = False
) -> tuple[str, ...]:
    if not final:
        return ()
    if role == "user":
        return (MessageAction.EDIT.value,)
    if role != "assistant":
        return ()
    return (
        *((MessageAction.RETRY.value,) if latest else ()),
        *((MessageAction.BRANCH.value,) if branch else ()),
    )


def target_message(
    messages: tuple[Message, ...] | list[Message], message_id: str, role: str
) -> tuple[int, Message]:
    for index, message in enumerate(messages):
        if message.id == message_id:
            if message.role != role or message.channel == "analysis":
                break
            return index, message
    from .errors import ProviderError

    raise ProviderError("The selected message is no longer on the active conversation path")


@dataclass(frozen=True)
class Feature:
    capability: Capability
    state: FeatureState
    reason: str | None = None


@dataclass(frozen=True)
class ProviderDescription:
    id: str
    features: tuple[Feature, ...]

    def feature(self, capability: Capability) -> Feature:
        return next(
            (item for item in self.features if item.capability == capability),
            Feature(capability, FeatureState.UNKNOWN),
        )


@dataclass(frozen=True)
class Choice:
    id: str
    title: str


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
    state: FeatureState
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
            FeatureState(value["state"]),
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
        if preferred in supported:
            return preferred
        return model.default_effort if model.default_effort in supported else None


@dataclass(frozen=True)
class Origin:
    provider_id: str
    account_id: str
    remote_id: str


@dataclass(frozen=True)
class Resource:
    id: str
    name: str
    media_type: str | None = None
    remote_url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class Content:
    kind: str
    text: str | None = None
    resource: Resource | None = None
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class Message:
    id: str
    role: str
    content: tuple[Content, ...]
    channel: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)
    actions: tuple[str, ...] = ()


@dataclass(frozen=True)
class Conversation:
    origin: Origin
    title: str
    messages: tuple[Message, ...] = ()
    project_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class Project:
    origin: Origin
    name: str
    instructions: str | None = None
    resources: tuple[Resource, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class ProjectOptions:
    fields: tuple[str, ...] = ()
    visibility: tuple[Choice, ...] = ()
    memory: tuple[Choice, ...] = ()
    sources: tuple[dict, ...] = ()
    remove_sources: bool = False
    source_error: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ProjectOptions:
        return cls(
            tuple(value.get("fields", ())),
            tuple(Choice(**v) for v in value.get("visibility", ())),
            tuple(Choice(**v) for v in value.get("memory", ())),
            tuple(value.get("sources", ())),
            bool(value.get("remove_sources")),
            value.get("source_error"),
        )


def source_file(path: Any, *, error: str = "Kaynak dosyası en fazla 20 MiB olabilir") -> Path:
    file = Path(path).expanduser().resolve(strict=True)
    if not file.is_file() or file.stat().st_size > 20 * 1024 * 1024:
        raise ValueError(error)
    return file


T = TypeVar("T")


@dataclass(frozen=True)
class Page(Generic[T]):
    items: tuple[T, ...]
    next_cursor: str | None = None
