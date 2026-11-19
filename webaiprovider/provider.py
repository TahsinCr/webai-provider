from __future__ import annotations

import asyncio
from importlib import import_module
from typing import TYPE_CHECKING, Any

from eventflow import EventBus, EventEnvelope

from .accounts.status import SessionStatus
from .base import BaseProvider, Capability, ProviderType
from .core.bridges import PortalBridge
from .core.hooks import SyncOperationHooks
from .core.state import LifecycleState
from .core.telemetry import ProviderTelemetry
from .resources.chat.chat import AsyncChat, Chat
from .resources.chat.collections import ChatsCollection
from .resources.message.message import AsyncMessage, Message
from .resources.model.collections import ModelsCollection
from .resources.model.model import (
    EffortRef,
    ModelRef,
    ModelSelection,
)
from .resources.model.types import Models
from .resources.project.collections import ProjectsCollection
from .resources.project.project import Project
from .resources.response.response import Response
from .transport import AsyncTransport, PortalTransport, Transport

if TYPE_CHECKING:
    from .accounts.account import Account
    from .async_provider import AsyncProvider
    from .resources.project.project import AsyncProject


def _provider_class(provider_type: ProviderType, *, asynchronous: bool):
    names = {
        ProviderType.CHATGPT: "ChatGPT",
        ProviderType.CLAUDE: "Claude",
        ProviderType.GEMINI: "Gemini",
        ProviderType.GROK: "Grok",
    }
    module = import_module(f".providers.{provider_type.value}.provider", __package__)
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


class Provider(BaseProvider):
    """Synchronous provider API backed by one persistent async ProviderRuntime."""

    def __new__(cls, *args: Any, **options: Any):
        return _construct_provider(cls, Provider, args, options, asynchronous=False)

    async_provider_type: type[AsyncProvider] | None = None
    chat_wrapper_type = None
    project_wrapper_type = None
    message_wrapper_type = Message
    projects_collection_type = None
    models_collection_type = None

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        transport: AsyncTransport | None = None,
    ) -> None:
        super().__init__(account, type=type)
        provider_type = self.async_provider_type
        if provider_type is None:
            from .async_provider import AsyncProvider

            provider_type = AsyncProvider
        options: dict[str, Any] = {"type": self.type}
        if transport is not None:
            options["transport"] = transport
        self._provider = provider_type(account, **options)
        self._portal = PortalBridge()
        self.transport: Transport = PortalTransport(self._call, lambda: self._provider.transport)
        self._portal_started = False
        self._opened = False
        self._closed = False
        self.events: EventBus[EventEnvelope[str]] = self._provider.runtime.sync_events
        self.hooks: SyncOperationHooks = SyncOperationHooks(self._provider.runtime.hooks)
        self.chats = ChatsCollection(self)
        self.projects = (self.projects_collection_type or ProjectsCollection)(self)
        self.models = (self.models_collection_type or ModelsCollection)(self)

    def _wrap_chat(self, resource: AsyncChat) -> Chat:
        return (self.chat_wrapper_type or Chat)(self, resource)

    def _wrap_project(self, resource: AsyncProject) -> Project:
        return (self.project_wrapper_type or Project)(self, resource)

    def _wrap_response(self, item: Response, *, chat_view: Chat | None = None) -> Response:
        message = item.message
        if isinstance(message, AsyncMessage):
            message = self.message_wrapper_type._from_async(self, message)
        chat = item.chat
        if isinstance(chat, AsyncChat):
            chat = chat_view if chat_view is not None else self._wrap_chat(chat)
        return type(item)(
            item.content,
            message=message,
            id=item.id,
            remote_id=item.remote_id,
            model=item.model,
            sources=item.sources,
            artifacts=item.artifacts,
            metadata=item.metadata,
            timing=item.timing,
            chat=chat,
        )

    @property
    def capabilities(self) -> frozenset[Capability]:
        return self._provider.capabilities

    @property
    def lifecycle_state(self) -> LifecycleState:
        return self._provider.lifecycle_state

    @property
    def telemetry(self) -> ProviderTelemetry:
        return self._provider.telemetry

    def _call(self, coroutine: Any) -> Any:
        if self._closed:
            if hasattr(coroutine, "close"):
                coroutine.close()
            raise RuntimeError("The provider is closed")
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            if hasattr(coroutine, "close"):
                coroutine.close()
            raise RuntimeError(
                "Use AsyncProvider in async code; Provider cannot block an event loop"
            )
        self._portal_started = True
        return self._portal.call(coroutine)

    def open(self) -> Provider:
        if self._closed:
            raise RuntimeError("A closed provider cannot be reopened")
        if not self._opened:
            self._call(self._provider.open())
            self._opened = True
        return self

    def close(self) -> None:
        if self._closed:
            return
        try:
            if self._portal_started:
                self._call(self._provider.close())
            else:
                self._provider.runtime.sync_events.close()
        finally:
            self._portal.close()
            self._opened = False
            self._closed = True

    def authenticate(self) -> SessionStatus:
        return self._call(self._provider.authenticate())

    def session_status(self) -> SessionStatus:
        return self._call(self._provider.session_status())

    def forget_session(self) -> SessionStatus:
        return self._call(self._provider.forget_session())

    def ask(
        self,
        prompt: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Any = None,
    ) -> Response:
        return self._call(self._provider.ask(prompt, model=model, attachments=attachments))

    def __enter__(self) -> Provider:
        return self.open()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
