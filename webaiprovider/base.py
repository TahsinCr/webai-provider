from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from collections.abc import Awaitable
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum
from importlib import import_module
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

from .accounts.status import SessionState, SessionStatus
from .core.state import LifecycleState
from .core.types import OperationContext
from .errors import ProviderError

if TYPE_CHECKING:
    from .accounts.account import Account
    from .resources.model.model import ModelCatalog
    from .resources.response.operation import StreamEvent


@dataclass(slots=True, eq=False)
class _ActiveOperation:
    provider_id: int
    send_lock_id: int | None = None
    active: bool = True


_ACTIVE_OPERATIONS: ContextVar[tuple[_ActiveOperation, ...]] = ContextVar(
    "webai_active_operations", default=()
)

_OPENING_PROVIDERS: ContextVar[tuple[int, ...]] = ContextVar("webai_opening_providers", default=())


class ProviderType(str, Enum):
    CHATGPT = "chatgpt"
    CLAUDE = "claude"
    GEMINI = "gemini"
    GROK = "grok"

    @classmethod
    def parse(cls, value: ProviderType | str) -> ProviderType:
        try:
            return value if isinstance(value, cls) else cls(value.casefold())
        except (AttributeError, ValueError):
            supported = ", ".join(item.value for item in cls)
            raise ValueError(f"Unsupported provider type; expected one of: {supported}") from None


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
    MESSAGE_EDIT = "message_edit"
    MESSAGE_RETRY = "message_retry"
    CHAT_BRANCH = "chat_branch"


_ChatsT = TypeVar("_ChatsT", covariant=True)
_ProjectsT = TypeVar("_ProjectsT", covariant=True)
_ModelsT = TypeVar("_ModelsT")


class BaseProvider(ABC, Generic[_ChatsT, _ProjectsT, _ModelsT]):
    """Account identity and typed resource collections common to both provider APIs."""

    provider_type: ClassVar[ProviderType | None] = None
    _capabilities: ClassVar[frozenset[Capability]] = frozenset()
    _chats: _ChatsT
    _projects: _ProjectsT
    models: _ModelsT

    @property
    def chats(self) -> _ChatsT:
        return self._chats

    @property
    def projects(self) -> _ProjectsT:
        return self._projects

    @property
    def capabilities(self) -> frozenset[Capability]:
        return self._capabilities

    @abstractmethod
    def close(self) -> None | Awaitable[None]:
        """Close the provider and release its owned resources."""
        raise NotImplementedError

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
    ) -> None:
        account_type = ProviderType.parse(account.provider)
        expected_type = self.provider_type
        requested_type = ProviderType.parse(type) if type is not None else expected_type
        selected_type = requested_type or account_type
        if expected_type is not None and selected_type is not expected_type:
            raise ValueError(
                f"{self.__class__.__name__} requires provider type {expected_type.value!r}"
            )
        if selected_type is not account_type:
            raise ValueError("Provider type must match the account provider")
        self.account = account
        self.type = selected_type

    @property
    def id(self) -> str:
        return self.type.value

    @property
    def account_id(self) -> str:
        return self.account.id

    @property
    def provider(self) -> str:
        return self.type.value

    def attachment_policy(self) -> dict | None:
        from .resources.message.attachment import policy_for

        policy = policy_for(self.id)
        return policy.as_dict() if policy else None

    def _new_operation_id(self) -> str:
        return str(uuid.uuid4())

    def _new_context(
        self,
        name: str,
        *,
        resource_id: str | None = None,
        operation_id: str | None = None,
        **fields: Any,
    ) -> OperationContext:
        return OperationContext(
            name=name,
            operation_id=operation_id or self._new_operation_id(),
            provider_id=self.id,
            account_id=self.account_id,
            resource_id=resource_id,
            **fields,
        )

    def __repr__(self) -> str:
        return f"<{type(self).__name__} type={self.type.value!r} account={self.account.id!r}>"

    def _has_active_operation(self, *, send_lock_id: int | None = None) -> bool:
        pid = id(self)
        return any(
            entry.active
            and entry.provider_id == pid
            and (send_lock_id is None or entry.send_lock_id == send_lock_id)
            for entry in _ACTIVE_OPERATIONS.get()
        )

    @contextmanager
    def _operation_marker(self, *, send_lock_id: int | None = None):
        marker = _ActiveOperation(id(self), send_lock_id)
        token = _ACTIVE_OPERATIONS.set((*_ACTIVE_OPERATIONS.get(), marker))
        try:
            yield
        finally:
            marker.active = False
            _ACTIVE_OPERATIONS.reset(token)

    def _active_transport(self) -> Any | None:
        return None

    def _session_status(self) -> SessionStatus:
        return SessionStatus(SessionState.UNKNOWN, reason="not_implemented")

    def _invalidate_project_cache(self, project_id: str) -> None:
        del project_id

    @property
    def opened(self) -> bool:
        return self._opened

    @property
    def closed(self) -> bool:
        return self.lifecycle.state == LifecycleState.CLOSED

    @property
    def closing(self) -> bool:
        return self.lifecycle.state == LifecycleState.CLOSING

    @property
    def lifecycle_state(self) -> LifecycleState:
        return self.lifecycle.state

    @property
    def _has_event_subscribers(self) -> bool:
        return bool(self.events.subscriber_count)

    def _require_provider(self) -> Any:
        if not self._opened:
            raise ProviderError("Open the provider before using its resources")
        return self

    def _prime_models(self, catalog: ModelCatalog) -> None:
        from .resources.model.types import _model_refs_from_catalog

        self.models._items = tuple(_model_refs_from_catalog(self.id, catalog))


class _StreamEventTranslator:
    """Shared pure provider-stream-to-public-StreamEvent state machine (zero asyncio)."""

    def __init__(self, provider: Any, operation: Any) -> None:
        self._provider = provider
        self._operation = operation
        self.output_started = False
        self.thinking_started = False
        self.thinking_text = ""

    def feed(self, event: Any) -> list[StreamEvent]:
        from .resources.message.protocol import (
            MessageMetadata,
            TextDelta,
            TextReplacement,
            ThoughtDelta,
        )
        from .resources.response.collections import _build_artifacts, _build_sources
        from .resources.response.operation import (
            ArtifactDiscovered,
            OutputBegin,
            OutputDelta,
            OutputReplace,
            SourceDiscovered,
            ThinkingBegin,
            ThinkingDelta,
            ThinkingEnd,
            ThinkingReplace,
        )

        emitted: list[StreamEvent] = []
        if isinstance(event, ThoughtDelta) or (
            isinstance(event, (TextDelta, TextReplacement)) and event.channel == "analysis"
        ):
            delta = event.text
            replaced = False
            if isinstance(event, TextReplacement):
                replaced = not event.text.startswith(self.thinking_text)
                delta = event.text[len(self.thinking_text) :] if not replaced else ""
                self.thinking_text = event.text
            else:
                self.thinking_text += delta
            if not self.thinking_started:
                self.thinking_started = True
                emitted.append(ThinkingBegin())
            if isinstance(event, TextReplacement) and replaced:
                emitted.append(ThinkingReplace(text=self.thinking_text))
            elif delta:
                emitted.append(ThinkingDelta(delta=delta))
            return emitted

        if isinstance(event, TextReplacement):
            if not event.text and not self.output_started:
                return emitted
            if self.thinking_started:
                emitted.append(ThinkingEnd(text=self.thinking_text))
                self.thinking_text = ""
                self.thinking_started = False
            if not self.output_started:
                self.output_started = True
                emitted.append(OutputBegin())
            emitted.append(OutputReplace(text=event.text))
            return emitted

        if isinstance(event, TextDelta) and event.text:
            if self.thinking_started:
                emitted.append(ThinkingEnd(text=self.thinking_text))
                self.thinking_text = ""
                self.thinking_started = False
            if not self.output_started:
                self.output_started = True
                emitted.append(OutputBegin())
            emitted.append(OutputDelta(delta=event.text))
            return emitted

        if isinstance(event, MessageMetadata):
            if event.user_message_id:
                self._operation.message.id = event.user_message_id
                self._operation.message.remote_id = event.user_message_id
            if event.message_id:
                self._operation._assistant_message_id = event.message_id
            for source in _build_sources(event.sources, self._provider._source_type):
                emitted.append(SourceDiscovered(source))
            for artifact in _build_artifacts(None, event.files, self._provider._artifact_type):
                emitted.append(ArtifactDiscovered(artifact))
        return emitted

    def finish(self) -> list[StreamEvent]:
        from .resources.response.operation import OutputEnd, ThinkingEnd

        emitted: list[StreamEvent] = []
        if self.thinking_started:
            emitted.append(ThinkingEnd(text=self.thinking_text))
        if self.output_started:
            emitted.append(OutputEnd())
        return emitted


def _provider_class(provider_type: ProviderType, *, asynchronous: bool):
    names = {
        ProviderType.CHATGPT: "ChatGPT",
        ProviderType.CLAUDE: "Claude",
        ProviderType.GEMINI: "Gemini",
        ProviderType.GROK: "Grok",
    }
    module_name = "async_provider" if asynchronous else "provider"
    module = import_module(f".providers.{provider_type.value}.{module_name}", __package__)
    return getattr(module, ("Async" if asynchronous else "") + names[provider_type] + "Provider")


def _construct_provider(cls, base_cls, args, options, *, asynchronous: bool):
    if cls is base_cls:
        if not args and "account" not in options:
            raise TypeError("A provider account is required")
        account = args[0] if args else options["account"]
        requested = options.get("type")
        selected = ProviderType.parse(account.provider if requested is None else requested)
        return object.__new__(_provider_class(selected, asynchronous=asynchronous))
    return object.__new__(cls)
