from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any, Generic, TypeVar, cast

from .accounts.account import Account
from .catalogs.policies import Models
from .contracts import EffortRef, ModelRef, ModelSelection, StreamEvent
from .core.bridges import SyncPortalBridge
from .models import Page
from .provider_type import ProviderType
from .providers.chatgpt.collections import ChatGPTProjectsCollection
from .resources.chat import Chat
from .resources.message import Message
from .resources.operation import MessageOperation
from .resources.project import Project
from .resources.response import Response
from .session import Provider

T = TypeVar("T")
U = TypeVar("U")


class SyncProvider:
    """Blocking façade over the async provider, backed by one persistent event loop."""

    def __init__(self, account: Account, *, type: ProviderType | str | None = None) -> None:
        self.account = account
        self.type = ProviderType.parse(type if type is not None else account.provider)
        self._provider = Provider(account, type=self.type)
        self._portal = SyncPortalBridge()
        self._portal_started = False
        self._opened = False
        self._closed = False
        self.chats = SyncChatsCollection(self)
        self.projects = SyncProjectsCollection(self)
        self.models = SyncModelsCollection(self)

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
                "Use Provider in async code; SyncProvider cannot block an event loop"
            )
        self._portal_started = True
        return self._portal.call(coroutine)

    def open(self) -> SyncProvider:
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

    def __enter__(self) -> SyncProvider:
        return self.open()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<SyncProvider type={self.type.value!r} account={self.account.id!r}>"


class _SyncCollection(Generic[T, U]):
    def __init__(self, owner: SyncProvider, collection: Any) -> None:
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
    def __init__(self, owner: SyncProvider, resource: Chat) -> None:
        self._owner = owner
        self._resource = resource
        self.messages = SyncMessagesCollection(owner, resource)

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
        return SyncChat(self._owner, self._owner._call(self._resource.rename(title)))

    def delete(self) -> None:
        self._owner._call(self._resource.delete())

    def branch(self, message_id: str, *, title: str | None = None) -> SyncChat:
        chat = self._owner._call(self._resource.branch(message_id, title=title))
        return SyncChat(self._owner, chat)

    def move_to(self, project: SyncProject | Project | str | None) -> SyncChat:
        target = project._resource if isinstance(project, SyncProject) else project
        chat = self._owner._call(self._resource.move_to(target))
        return SyncChat(self._owner, chat)

    def __repr__(self) -> str:
        return repr(self._resource)


class SyncChatsCollection(_SyncCollection[Chat, SyncChat]):
    def __init__(self, owner: SyncProvider) -> None:
        super().__init__(owner, owner._provider.chats)

    def _wrap(self, item: Chat) -> SyncChat:
        return SyncChat(self._owner, item)

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

    def branch(
        self, chat: SyncChat | Chat | str, message_id: str, *, title: str | None = None
    ) -> SyncChat:
        target = chat._resource if isinstance(chat, SyncChat) else chat
        return self._wrap(
            self._owner._call(self._collection.branch(target, message_id, title=title))
        )

    def move(
        self,
        chat: SyncChat | Chat | str,
        project: SyncProject | Project | str | None,
    ) -> SyncChat:
        target_chat = chat._resource if isinstance(chat, SyncChat) else chat
        target_project = project._resource if isinstance(project, SyncProject) else project
        moved = self._owner._call(self._collection.move(target_chat, target_project))
        return self._wrap(moved)


class SyncProject:
    def __init__(self, owner: SyncProvider, resource: Project) -> None:
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
    def __init__(self, owner: SyncProvider) -> None:
        super().__init__(owner, owner._provider.projects)

    def _wrap(self, item: Project) -> SyncProject:
        return SyncProject(self._owner, item)

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
    def __init__(self, owner: SyncProvider) -> None:
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
    def __init__(self, owner: SyncProvider, chat: Chat) -> None:
        super().__init__(owner, chat.messages)

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

    def branch(self, message_id: str, *, title: str | None = None) -> SyncChat:
        return SyncChat(
            self._owner,
            self._owner._call(self._collection.branch(message_id, title=title)),
        )


class SyncMessageOperation:
    def __init__(self, owner: SyncProvider, operation: MessageOperation) -> None:
        self._owner = owner
        self._operation = operation
        self.hooks = operation.hooks

    @property
    def message(self) -> Message:
        return self._operation.message

    @property
    def chat(self) -> SyncChat:
        return SyncChat(self._owner, self._operation.chat)

    @property
    def response(self) -> Response | None:
        return self._operation._response

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
    def __init__(self, owner: SyncProvider, project: Project) -> None:
        self._owner = owner
        self._instruction = project.instruction

    def get(self) -> str:
        return self._owner._call(self._instruction.get())

    def edit(self, text: str) -> str:
        return self._owner._call(self._instruction.edit(text))

    def clear(self) -> None:
        self._owner._call(self._instruction.clear())


class SyncProjectResources(_SyncCollection[dict[str, Any], dict[str, Any]]):
    def __init__(self, owner: SyncProvider, project: Project) -> None:
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
    def __init__(self, owner: SyncProvider, project: Project) -> None:
        super().__init__(owner, project.chats)
        self.next_cursor: str | None = None

    def _wrap(self, item: Chat) -> SyncChat:
        return SyncChat(self._owner, item)

    def refresh(self, cursor: str | None = None) -> Page[SyncChat]:
        page = self._owner._call(self._collection.refresh(cursor))
        self.next_cursor = page.next_cursor
        return Page(tuple(self._wrap(item) for item in page.items), page.next_cursor)

    def add(self, chat: SyncChat | Chat | str) -> SyncChat:
        target = chat._resource if isinstance(chat, SyncChat) else chat
        return self._wrap(self._owner._call(self._collection.add(target)))

    def remove(self, chat: SyncChat | Chat | str) -> SyncChat:
        target = chat._resource if isinstance(chat, SyncChat) else chat
        return self._wrap(self._owner._call(self._collection.remove(target)))


class SyncChatGPTProjectsCollection(SyncProjectsCollection):
    def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
        memory_share: bool = True,
    ) -> SyncProject:
        collection = cast(ChatGPTProjectsCollection, self._collection)
        project = self._owner._call(
            collection.create(title, instruction=instruction, memory_share=memory_share)
        )
        return self._wrap(project)


class SyncChatGPTProvider(SyncProvider):
    def __init__(self, account: Account) -> None:
        super().__init__(account, type=ProviderType.CHATGPT)
        self._provider.projects = ChatGPTProjectsCollection(self._provider)
        self.projects = SyncChatGPTProjectsCollection(self)


class SyncClaudeProvider(SyncProvider):
    def __init__(self, account: Account) -> None:
        super().__init__(account, type=ProviderType.CLAUDE)


class SyncGeminiProvider(SyncProvider):
    def __init__(self, account: Account) -> None:
        super().__init__(account, type=ProviderType.GEMINI)


class SyncGrokProvider(SyncProvider):
    def __init__(self, account: Account) -> None:
        super().__init__(account, type=ProviderType.GROK)
