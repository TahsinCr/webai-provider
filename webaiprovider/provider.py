from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any, TypeVar, cast

from eventflow import EventBus, EventEnvelope

from .accounts.status import SessionStatus
from .base import BaseProvider, Capability, ProviderType, _construct_provider
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
    from typing_extensions import TypeVar as DefaultTypeVar

    from .accounts.account import Account
    from .async_provider import AsyncProvider
    from .resources.project.project import AsyncProject


if TYPE_CHECKING:
    _ChatsT = DefaultTypeVar(
        "_ChatsT", bound=ChatsCollection[Any], covariant=True, default=ChatsCollection[Chat]
    )
    _ProjectsT = DefaultTypeVar(
        "_ProjectsT",
        bound=ProjectsCollection[Any],
        covariant=True,
        default=ProjectsCollection[Project],
    )
else:
    _ChatsT = TypeVar("_ChatsT", covariant=True)
    _ProjectsT = TypeVar("_ProjectsT", covariant=True)

_ResultT = TypeVar("_ResultT")


_ProviderT = TypeVar("_ProviderT", bound="Provider[Any, Any]")


class Provider(BaseProvider[_ChatsT, _ProjectsT, ModelsCollection]):
    """Synchronous provider API backed by one persistent async ProviderRuntime."""

    def __new__(cls: type[_ProviderT], *args: Any, **options: Any) -> _ProviderT:
        return _construct_provider(cls, Provider, args, options, asynchronous=False)

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        transport: AsyncTransport | None = None,
    ) -> None:
        super().__init__(account, type=type)
        options: dict[str, Any] = {"type": self.type}
        if transport is not None:
            options["transport"] = transport
        self._provider = self._make_async_provider(account, **options)
        self._portal = PortalBridge()
        self.transport: Transport = PortalTransport(self._call, lambda: self._provider.transport)
        self._portal_started = False
        self._closed = False
        self.events: EventBus[EventEnvelope[str]] = self._provider.runtime.sync_events
        self.hooks: SyncOperationHooks = SyncOperationHooks(self._provider.runtime.hooks)
        self._chats = self._make_chats()
        self._projects = self._make_projects()
        self.models = ModelsCollection(self)

    def _make_async_provider(self, account: Account, **options: Any) -> AsyncProvider[Any, Any]:
        from .async_provider import AsyncProvider

        return AsyncProvider(account, **options)

    def _make_chats(self) -> _ChatsT:
        return cast("_ChatsT", ChatsCollection(self, Chat))

    def _make_projects(self) -> _ProjectsT:
        return cast("_ProjectsT", ProjectsCollection(self, Project))

    def _wrap_chat(self, resource: AsyncChat) -> Chat:
        return self.chats._wrap(resource)

    def _wrap_project(self, resource: AsyncProject) -> Project:
        return self.projects._wrap(resource)

    def _wrap_message(self, resource: AsyncMessage) -> Message:
        return Message._from_async(self, resource)

    def _wrap_response(self, item: Response, *, chat_view: Chat | None = None) -> Response:
        message = item.message
        if isinstance(message, AsyncMessage):
            message = self._wrap_message(message)
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

    def _call(self, coroutine: Awaitable[_ResultT]) -> _ResultT:
        if self._closed:
            close = getattr(coroutine, "close", None)
            if close is not None:
                close()
            raise RuntimeError("The provider is closed")
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            close = getattr(coroutine, "close", None)
            if close is not None:
                close()
            raise RuntimeError(
                "Use AsyncProvider in async code; Provider cannot block an event loop"
            )
        self._portal_started = True
        return self._portal.call(coroutine)

    def open(self: _ProviderT) -> _ProviderT:
        if self._closed:
            raise RuntimeError("A closed provider cannot be reopened")
        if not self._provider.runtime.opened:
            self._call(self._provider.open())
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
            if not self._portal_started or self._provider.runtime.closed:
                self._closed = True
                self._portal.close()

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

    def __enter__(self: _ProviderT) -> _ProviderT:
        return self.open()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
