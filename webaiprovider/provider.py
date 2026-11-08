from __future__ import annotations

import asyncio
from collections.abc import Iterator
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from .base import BaseProvider
from .catalogs import Models
from .contracts import EffortRef, ModelRef, ModelSelection, StreamEvent
from .core.bridges import SyncPortalBridge
from .core.state import OperationState
from .core.telemetry import ProviderTelemetry
from .models import Page
from .resources.chat import Chat
from .resources.message import Message
from .resources.operation import MessageOperation
from .resources.project import Project
from .resources.response import Response

T = TypeVar("T")
U = TypeVar("U")


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


if TYPE_CHECKING:
    from .accounts.account import Account
    from .async_provider import AsyncProvider


class Provider(BaseProvider):
    """Synchronous provider API backed by one persistent async runtime."""

    async_provider_type: type[AsyncProvider] | None = None
    chat_wrapper_type = None
    project_wrapper_type = None
    projects_collection_type = None
    models_collection_type = None

    def __init__(self, account: Account, *, type: ProviderType | str | None = None) -> None:
        super().__init__(account, type=type)
        provider_type = self.async_provider_type
        if provider_type is None:
            from .async_provider import AsyncProvider

            provider_type = AsyncProvider
        self._provider = provider_type(account, type=self.type)
        self._portal = SyncPortalBridge()
        self._portal_started = False
        self._opened = False
        self._closed = False
        self.chats = SyncChatsCollection(self)
        self.projects = (self.projects_collection_type or SyncProjectsCollection)(self)
        self.models = (self.models_collection_type or SyncModelsCollection)(self)

    def _wrap_chat(self, resource: Chat) -> SyncChat:
        return (self.chat_wrapper_type or SyncChat)(self, resource)

    def _wrap_project(self, resource: Project) -> SyncProject:
        return (self.project_wrapper_type or SyncProject)(self, resource)

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


class _SyncCollection(Generic[T, U]):
    def __init__(self, owner: Provider, collection: Any) -> None:
        self._owner = owner
        self._collection = collection

    def _wrap(self, item: T) -> U:
        return item  # type: ignore[return-value]

    def list(self) -> list[U]:
        return [self._wrap(item) for item in self._collection.list()]

    def get(self, item_id: str, /) -> U:
        return self._wrap(self._collection.get(item_id))

    def find(self, query: str, /, **match: bool) -> U:
        return self._wrap(self._collection.find(query, **match))

    def search(self, query: str, /, **match: bool) -> list[U]:
        return [self._wrap(item) for item in self._collection.search(query, **match)]

    def __len__(self) -> int:
        return len(self._collection)

    def __iter__(self) -> Iterator[U]:
        return iter(self.list())


class SyncChat:
    messages_collection_type = None

    def __init__(self, owner: Provider, resource: Chat) -> None:
        self._owner = owner
        self._resource = resource
        self.messages = (self.messages_collection_type or SyncMessagesCollection)(owner, resource)

    @property
    def id(self) -> str:
        return self._resource.id

    @property
    def remote_id(self) -> str | None:
        return self._resource.remote_id

    @property
    def title(self) -> str:
        return self._resource.title

    @property
    def project_id(self) -> str | None:
        return self._resource.project_id

    def get(self, message_id: str) -> Message:
        return self._resource.get(message_id)

    def download(self, target: Any) -> bytes:
        return self._owner._call(self._resource.download(target))

    def rename(self, title: str) -> SyncChat:
        return self._owner._wrap_chat(self._owner._call(self._resource.rename(title)))

    def delete(self) -> None:
        self._owner._call(self._resource.delete())

    def __repr__(self) -> str:
        return repr(self._resource)


class SyncChatsCollection(_SyncCollection[Chat, SyncChat]):
    def __init__(self, owner: Provider) -> None:
        super().__init__(owner, owner._provider.chats)

    def _wrap(self, item: Chat) -> SyncChat:
        return self._owner._wrap_chat(item)

    def refresh(self) -> list[SyncChat]:
        return [self._wrap(item) for item in self._owner._call(self._collection.refresh())]

    def create(
        self, title: str = "New chat", *, project: SyncProject | Project | str | None = None
    ) -> SyncChat:
        target = project._resource if isinstance(project, SyncProject) else project
        chat = self._owner._call(self._collection.create(title, project=target))
        return self._wrap(chat)

    def update(self, chat: SyncChat | Chat | str, *, title: str | None = None) -> SyncChat:
        target = chat._resource if isinstance(chat, SyncChat) else chat
        return self._wrap(self._owner._call(self._collection.update(target, title=title)))

    def delete(self, chat: SyncChat | Chat | str) -> None:
        target = chat._resource if isinstance(chat, SyncChat) else chat
        self._owner._call(self._collection.delete(target))


class SyncProject:
    def __init__(self, owner: Provider, resource: Project) -> None:
        self._owner = owner
        self._resource = resource
        self.instruction = SyncProjectInstruction(owner, resource)
        self.resources = SyncProjectResources(owner, resource)
        self.chats = SyncProjectChatsCollection(owner, resource)

    @property
    def id(self) -> str:
        return self._resource.id

    @property
    def remote_id(self) -> str | None:
        return self._resource.remote_id

    @property
    def title(self) -> str:
        return self._resource.title

    def rename(self, title: str) -> None:
        self._owner._call(self._resource.rename(title))

    def delete(self) -> None:
        self._owner._call(self._resource.delete())

    def __repr__(self) -> str:
        return repr(self._resource)


class SyncProjectsCollection(_SyncCollection[Project, SyncProject]):
    def __init__(self, owner: Provider) -> None:
        super().__init__(owner, owner._provider.projects)

    def _wrap(self, item: Project) -> SyncProject:
        return self._owner._wrap_project(item)

    def refresh(self) -> list[SyncProject]:
        return [self._wrap(item) for item in self._owner._call(self._collection.refresh())]

    def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
    ) -> SyncProject:
        project = self._owner._call(self._collection.create(title, instruction=instruction))
        return self._wrap(project)

    def update(
        self,
        project: SyncProject | Project | str,
        *,
        title: str | None = None,
        instruction: str | None = None,
    ) -> SyncProject:
        target = project._resource if isinstance(project, SyncProject) else project
        return self._wrap(
            self._owner._call(self._collection.update(target, title=title, instruction=instruction))
        )

    def delete(self, project: SyncProject | Project | str) -> None:
        target = project._resource if isinstance(project, SyncProject) else project
        self._owner._call(self._collection.delete(target))


class SyncModelsCollection:
    def __init__(self, owner: Provider) -> None:
        self._owner = owner
        self._collection = owner._provider.models

    def list(self, *, refresh: bool = False) -> list[ModelRef]:
        return self._owner._call(self._collection.list(refresh=refresh))

    def refresh(self) -> list[ModelRef]:
        return self._owner._call(self._collection.refresh())

    def get(self, model_id: str, /) -> ModelRef:
        return self._owner._call(self._collection.get(model_id))

    def find(self, query: str, /, **match: bool) -> ModelRef:
        return self._owner._call(self._collection.find(query, **match))

    def search(self, query: str, /, **match: bool) -> list[ModelRef]:
        return self._owner._call(self._collection.search(query, **match))

    def resolve(
        self, target: Models | EffortRef | ModelRef | ModelSelection | str
    ) -> ModelSelection:
        return self._owner._call(self._collection.resolve(target))


class SyncMessagesCollection(_SyncCollection[Message, Message]):
    def __init__(self, owner: Provider, chat: Chat) -> None:
        super().__init__(owner, chat.messages)

    def refresh(self) -> list[Message]:
        return self._owner._call(self._collection.refresh())

    def create(
        self,
        message: Message | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Any = None,
        **metadata: Any,
    ) -> SyncMessageOperation:
        operation = self._collection.create(
            message, model=model, attachments=attachments, **metadata
        )
        return SyncMessageOperation(self._owner, operation)

    def update(
        self,
        message: Message | str,
        *,
        content: str | None = None,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> SyncMessageOperation:
        return SyncMessageOperation(
            self._owner,
            self._collection.update(message, content=content, model=model, **options),
        )

    def edit(
        self,
        message_id: str,
        new_content: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> SyncMessageOperation:
        return SyncMessageOperation(
            self._owner,
            self._collection.edit(message_id, new_content, model=model, **options),
        )

    def retry(
        self,
        message_id: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> SyncMessageOperation:
        return SyncMessageOperation(
            self._owner,
            self._collection.retry(message_id, model=model, **options),
        )

    def delete(self, message: Message | str) -> None:
        self._owner._call(self._collection.delete(message))


class SyncMessageOperation:
    def __init__(self, owner: Provider, operation: MessageOperation) -> None:
        self._owner = owner
        self._operation = operation
        self.hooks = operation.hooks

    @property
    def message(self) -> Message:
        return self._operation.message

    @property
    def chat(self) -> SyncChat:
        return self._owner._wrap_chat(self._operation.chat)

    @property
    def response(self) -> Response | None:
        return self._operation._response

    @property
    def state(self) -> OperationState:
        return self._operation.state

    def on(self, event_name: str, handler: Any = None) -> Any:
        return self.hooks.on(event_name, handler)

    def cancel(self) -> None:
        self._operation.cancel()

    def wait_for(self, stage_name: str, *, timeout: float | None = None) -> StreamEvent | None:
        return self._owner._call(self._operation.wait_for(stage_name, timeout=timeout))

    def stream(self) -> Iterator[StreamEvent]:
        events = self._operation.stream()
        while True:
            try:
                yield self._owner._call(events.__anext__())
            except StopAsyncIteration:
                return

    def start(self) -> Response:
        return self._owner._call(self._operation.start())

    def wait(self) -> Response:
        return self._owner._call(self._operation.wait())


class SyncProjectInstruction:
    def __init__(self, owner: Provider, project: Project) -> None:
        self._owner = owner
        self._instruction = project.instruction

    def get(self) -> str:
        return self._owner._call(self._instruction.get())

    def edit(self, text: str) -> str:
        return self._owner._call(self._instruction.edit(text))

    def clear(self) -> None:
        self._owner._call(self._instruction.clear())


class SyncProjectResources(_SyncCollection[dict[str, Any], dict[str, Any]]):
    def __init__(self, owner: Provider, project: Project) -> None:
        super().__init__(owner, project.resources)

    def create(self, data: Any, resource_type: str = "custom", **options: Any) -> dict[str, Any]:
        return self._owner._call(
            self._collection.create(data, resource_type=resource_type, **options)
        )

    def add(self, data: Any, resource_type: str = "custom", **options: Any) -> dict[str, Any]:
        return self._owner._call(self._collection.add(data, resource_type=resource_type, **options))

    def remove(self, resource_id: str) -> bool:
        return self._owner._call(self._collection.remove(resource_id))

    def delete(self, target: dict[str, Any] | str) -> None:
        self._owner._call(self._collection.delete(target))

    def update(
        self, resource: dict[str, Any] | str, *, data: Any = None, **options: Any
    ) -> dict[str, Any]:
        return self._owner._call(self._collection.update(resource, data=data, **options))

    def download(self, resource_id: str) -> Any:
        return self._owner._call(self._collection.download(resource_id))


class SyncProjectChatsCollection(_SyncCollection[Chat, SyncChat]):
    def __init__(self, owner: Provider, project: Project) -> None:
        super().__init__(owner, project.chats)
        self.next_cursor: str | None = None

    def _wrap(self, item: Chat) -> SyncChat:
        return self._owner._wrap_chat(item)

    def refresh(self, cursor: str | None = None) -> Page[SyncChat]:
        page = self._owner._call(self._collection.refresh(cursor))
        self.next_cursor = page.next_cursor
        return Page(tuple(self._wrap(item) for item in page.items), page.next_cursor)
