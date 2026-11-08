from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Generic, TypeVar

from .accounts.status import FeatureState, SessionState, SessionStatus

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
    "ResourceData",
    "Content",
    "MessageData",
    "Conversation",
    "ProjectData",
    "ProjectOptions",
    "source_file",
    "MessageAction",
    "actions_for",
    "target_message",
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

    @property
    def capability(self) -> Capability:
        return {
            self.EDIT: Capability.MESSAGE_EDIT,
            self.RETRY: Capability.MESSAGE_RETRY,
            self.BRANCH: Capability.CHAT_BRANCH,
        }[self]


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
    messages: tuple[MessageData, ...] | list[MessageData], message_id: str, role: str
) -> tuple[int, MessageData]:
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
        if model.default_effort is None and preferred == "medium":
            return None
        if preferred in supported:
            return preferred
        return model.default_effort if model.default_effort in supported else None


@dataclass(frozen=True)
class Origin:
    provider_id: str
    account_id: str
    remote_id: str


@dataclass(frozen=True)
class ResourceData:
    id: str
    name: str
    media_type: str | None = None
    remote_url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class Content:
    kind: str
    text: str | None = None
    resource: ResourceData | None = None
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class MessageData:
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
    messages: tuple[MessageData, ...] = ()
    project_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class ProjectData:
    origin: Origin
    name: str
    instructions: str | None = None
    resources: tuple[ResourceData, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class ProjectOptions:
    fields: tuple[str, ...] = ()
    visibility: tuple[Choice, ...] = ()
    memory: tuple[Choice, ...] = ()
    sources: tuple[dict, ...] = ()
    remove_sources: bool = False
    source_error: str | None = None


def source_file(path: Any, *, error: str = "Source file must not exceed 20 MiB.") -> Path:
    file = Path(path).expanduser().resolve(strict=True)
    if not file.is_file() or file.stat().st_size > 20 * 1024 * 1024:
        raise ValueError(error)
    return file


T = TypeVar("T")


@dataclass(frozen=True)
class Page(Generic[T]):
    items: tuple[T, ...]
    next_cursor: str | None = None
