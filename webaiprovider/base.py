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
from .errors import WebAIProviderError

if TYPE_CHECKING:
    from .accounts.account import WebAIAccount
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


class WebAIProviderType(str, Enum):
    CHATGPT = "chatgpt"
    CLAUDE = "claude"
    GEMINI = "gemini"
    GROK = "grok"

    @classmethod
    def parse(cls, value: WebAIProviderType | str) -> WebAIProviderType:
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
    WEB_SEARCH = "web_search"
    IMAGE_GENERATION = "image_generation"
    DEEP_RESEARCH = "deep_research"


_ChatsT = TypeVar("_ChatsT", covariant=True)
_ProjectsT = TypeVar("_ProjectsT", covariant=True)
_ModelsT = TypeVar("_ModelsT")


class BaseWebAIProvider(ABC, Generic[_ChatsT, _ProjectsT, _ModelsT]):
    """Account identity and typed resource collections common to both provider APIs."""

    provider_type: ClassVar[WebAIProviderType | None] = None
    _capabilities: ClassVar[frozenset[Capability]] = frozenset()
    _constructed_via_generic: bool = False
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
        account: WebAIAccount,
        *,
        type: WebAIProviderType | str | None = None,
    ) -> None:
        account_type = WebAIProviderType.parse(account.provider)
        expected_type = self.provider_type
        requested_type = WebAIProviderType.parse(type) if type is not None else expected_type
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
            raise WebAIProviderError("Open the provider before using its resources")
        return self

    def _prime_models(self, catalog: ModelCatalog) -> None:
        from .resources.model.types import _model_refs_from_catalog

        self.models._prime(_model_refs_from_catalog(self.id, catalog))

    def _validate_tool(self, tool: Any, *, tool_timeout: float | None = None) -> None:
        import math

        from .errors import FeatureUnavailable, UnsupportedOperationError
        from .resources.tools.types import Tool

        if tool_timeout is not None:
            if (
                isinstance(tool_timeout, bool)
                or not isinstance(tool_timeout, (int, float))
                or not math.isfinite(tool_timeout)
                or tool_timeout <= 0
            ):
                raise ValueError("tool_timeout must be a positive finite number")
        if tool is None:
            return
        if not isinstance(tool, Tool):
            raise TypeError("tool must be a Tool instance or None")
        provider_scope = getattr(tool, "provider_type", None)
        if provider_scope is not None:
            if self._constructed_via_generic:
                raise UnsupportedOperationError(
                    f"{type(tool).__name__} is not supported on generic WebAIProvider; "
                    f"use the {provider_scope} provider directly"
                )
            if provider_scope != self.id:
                raise UnsupportedOperationError(
                    f"{type(tool).__name__} is only supported on {provider_scope} providers"
                )
        capability = getattr(tool, "capability", None)
        if capability is not None and capability not in self.capabilities:
            raise FeatureUnavailable(capability)


class _StreamEventTranslator:
    """Normalize provider blocks and generation milestones for both execution modes."""

    def __init__(self, provider: Any, operation: Any) -> None:
        self._provider = provider
        self._operation = operation
        self._open: dict[str, tuple[bool, list[str]]] = {}
        self._implicit: tuple[bool, str] | None = None
        self._next_id = 0
        self._generated = False
        self._verifying = False

    def _close(self, part_id: str) -> list[StreamEvent]:
        from .resources.response.operation import OutputEnd, ThinkingEnd

        block = self._open.pop(part_id, None)
        if block is None:
            return []
        thinking, chunks = block
        return [
            ThinkingEnd("".join(chunks), part_id=part_id)
            if thinking
            else OutputEnd(part_id=part_id)
        ]

    def feed(self, event: Any) -> list[StreamEvent]:
        from .resources.message.protocol import (
            MessageMetadata,
            PartClosed,
            ResponseComplete,
            ResponseVerifying,
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
            ResponsePartsResolved,
            SourceDiscovered,
            ThinkingBegin,
            ThinkingDelta,
            ThinkingReplace,
            VerificationBegin,
        )

        if isinstance(event, ResponseComplete):
            return self.finish()
        if isinstance(event, ResponseVerifying):
            if self._verifying:
                return []
            self._verifying = True
            return [*self.finish(), VerificationBegin()]
        if isinstance(event, PartClosed):
            return self._close(self._native_id(event.part_id, event.channel == "analysis"))

        emitted: list[StreamEvent] = []
        if isinstance(event, (TextDelta, TextReplacement, ThoughtDelta)):
            if self._generated:
                raise WebAIProviderError("Provider sent content after generation ended")
            thinking = isinstance(event, ThoughtDelta) or event.channel == "analysis"
            replacement = isinstance(event, TextReplacement)
            if (
                not event.text
                and event.part_id is None
                and (self._implicit is None or self._implicit[0] != thinking)
            ):
                return emitted
            if event.part_id is not None:
                part_id = self._native_id(event.part_id, thinking)
                if self._implicit is not None:
                    emitted.extend(self._close(self._implicit[1]))
                    self._implicit = None
            else:
                if self._implicit is None or self._implicit[0] != thinking:
                    if self._implicit is not None:
                        emitted.extend(self._close(self._implicit[1]))
                    self._next_id += 1
                    self._implicit = thinking, f"local:{self._next_id}"
                part_id = self._implicit[1]
            if part_id not in self._open:
                if not event.text:
                    return emitted
                self._open[part_id] = thinking, []
                begin = ThinkingBegin if thinking else OutputBegin
                emitted.append(begin(part_id=part_id))
            if thinking:
                chunks = self._open[part_id][1]
                if replacement:
                    # Keep append-only snapshots as deltas for stream consumers.
                    previous = "".join(chunks)
                    if event.text.startswith(previous):
                        delta = event.text[len(previous) :]
                        if delta:
                            chunks.append(delta)
                            emitted.append(ThinkingDelta(delta, part_id=part_id))
                    else:
                        chunks[:] = [event.text]
                        emitted.append(ThinkingReplace(event.text, part_id=part_id))
                elif event.text:
                    chunks.append(event.text)
                    emitted.append(ThinkingDelta(event.text, part_id=part_id))
            elif replacement:
                emitted.append(OutputReplace(event.text, part_id=part_id))
            elif event.text:
                emitted.append(OutputDelta(event.text, part_id=part_id))
            return emitted

        if isinstance(event, MessageMetadata):
            if event.user_message_id:
                self._operation.message.id = event.user_message_id
                self._operation.message.remote_id = event.user_message_id
            if event.message_id:
                self._operation._assistant_message_id = event.message_id
            if event.parts:
                emitted.append(ResponsePartsResolved(event.parts))
            for source in _build_sources(event.sources, self._provider._source_type):
                emitted.append(SourceDiscovered(source))
            for artifact in _build_artifacts(None, event.files, self._provider._artifact_type):
                emitted.append(ArtifactDiscovered(artifact))
        return emitted

    @staticmethod
    def _native_id(part_id: str, thinking: bool) -> str:
        return f"{'thinking' if thinking else 'text'}:{part_id}"

    def finish(self) -> list[StreamEvent]:
        from .resources.response.operation import GenerationEnd

        if self._generated:
            return []
        self._generated = True
        emitted = [event for part_id in tuple(self._open) for event in self._close(part_id)]
        emitted.append(GenerationEnd())
        return emitted


def _provider_class(provider_type: WebAIProviderType, *, asynchronous: bool):
    names = {
        WebAIProviderType.CHATGPT: "ChatGPT",
        WebAIProviderType.CLAUDE: "Claude",
        WebAIProviderType.GEMINI: "Gemini",
        WebAIProviderType.GROK: "Grok",
    }
    module_name = "async_provider" if asynchronous else "provider"
    module = import_module(f".providers.{provider_type.value}.{module_name}", __package__)
    return getattr(
        module, ("Async" if asynchronous else "") + names[provider_type] + "WebAIProvider"
    )


def _construct_provider(cls, base_cls, args, options, *, asynchronous: bool):
    if cls is base_cls:
        if not args and "account" not in options:
            raise TypeError("A provider account is required")
        account = args[0] if args else options["account"]
        requested = options.get("type")
        selected = WebAIProviderType.parse(account.provider if requested is None else requested)
        instance = object.__new__(_provider_class(selected, asynchronous=asynchronous))
        instance._constructed_via_generic = True
        return instance
    return object.__new__(cls)
