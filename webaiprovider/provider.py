from __future__ import annotations

import asyncio
from enum import Enum
from importlib import import_module
from typing import TYPE_CHECKING, Any

from .base import BaseProvider
from .core.bridges import PortalBridge
from .core.telemetry import ProviderTelemetry
from .resources.chat.chat import Chat
from .resources.chat.collections import ChatsCollection
from .resources.message.response import Response
from .resources.model.collections import (
    Models,
    ModelsCollection,
)
from .resources.model.model import (
    EffortRef,
    ModelRef,
    ModelSelection,
)
from .resources.project.collections import ProjectsCollection
from .resources.project.project import Project
from .transport import AsyncTransport, PortalTransport, Transport


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


def _provider_class(provider_type: ProviderType, *, asynchronous: bool):
    names = {
        ProviderType.CHATGPT: "ChatGPT",
        ProviderType.CLAUDE: "Claude",
        ProviderType.GEMINI: "Gemini",
        ProviderType.GROK: "Grok",
    }
    module = import_module(f".providers.{provider_type.value}.provider", __package__)
    return getattr(module, ("Async" if asynchronous else "") + names[provider_type] + "Provider")


if TYPE_CHECKING:
    from .accounts.account import Account
    from .async_provider import AsyncProvider
    from .resources.chat.chat import AsyncChat
    from .resources.project.project import AsyncProject


class Provider(BaseProvider):
    """Synchronous provider API backed by one persistent async runtime."""

    def __new__(cls, *args, **options):
        if cls is Provider:
            if not args and "account" not in options:
                raise TypeError("A provider account is required")
            account = args[0] if args else options["account"]
            requested = options.get("type")
            selected = ProviderType.parse(account.provider if requested is None else requested)
            return object.__new__(_provider_class(selected, asynchronous=False))
        return object.__new__(cls)

    async_provider_type: type[AsyncProvider] | None = None
    chat_wrapper_type = None
    project_wrapper_type = None
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
        self.chats = ChatsCollection(self)
        self.projects = (self.projects_collection_type or ProjectsCollection)(self)
        self.models = (self.models_collection_type or ModelsCollection)(self)

    def _wrap_chat(self, resource: AsyncChat) -> Chat:
        return (self.chat_wrapper_type or Chat)(self, resource)

    def _wrap_project(self, resource: AsyncProject) -> Project:
        return (self.project_wrapper_type or Project)(self, resource)

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
        finally:
            self._portal.close()
            self._opened = False
            self._closed = True

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

    def __repr__(self) -> str:
        return super().__repr__()
