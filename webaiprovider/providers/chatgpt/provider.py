from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from ...accounts.account import Account
from ...accounts.status import SessionStatus
from ...async_provider import AsyncProvider
from ...base import BaseProviderAdapter, Capability
from ...core.operations import execute_with_hooks
from ...errors import UnsupportedOperationError
from ...provider import Provider, ProviderType
from ...resources.chat.chat import (
    AsyncChat,
    BranchableChat,
    Chat,
    MovableChat,
    _resolve_project_arg,
)
from ...resources.project.collections import ProjectsCollection
from ...resources.project.project import AsyncProject, Project
from ...transport import AsyncTransport
from .adapter import ChatGPTAdapter
from .collections import AsyncChatGPTProjectsCollection
from .resources import AsyncChatGPTChat
from .session import BrowserSession


class AsyncChatGPTProvider(AsyncProvider):
    provider_type = ProviderType.CHATGPT

    _chat_type = AsyncChatGPTChat
    _projects_collection_type = AsyncChatGPTProjectsCollection

    capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.DELETE,
            Capability.PROJECT_DELETE,
            Capability.ATTACHMENTS,
            Capability.TEMPORARY,
            Capability.HISTORY,
            Capability.RENAME,
            Capability.AUTH,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.RESOURCES,
            Capability.PROJECT_MEMBERSHIP,
            Capability.MODELS,
            Capability.EFFORT,
            Capability.MESSAGE_EDIT,
            Capability.MESSAGE_RETRY,
            Capability.CHAT_BRANCH,
        }
    )

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        session: BrowserSession | None = None,
        client_factory: Callable[..., Any] | None = None,
        transport: AsyncTransport | None = None,
        adapter: BaseProviderAdapter | None = None,
    ) -> None:
        self._initial_session = session
        self._client_factory = client_factory
        super().__init__(account, type=type, transport=transport, adapter=adapter)

    def _create_adapter(self) -> ChatGPTAdapter:
        return ChatGPTAdapter(
            account=self.account,
            capabilities=self.capabilities,
            transport=self._transport,
            session=self._initial_session,
            client_factory=self._client_factory,
        )

    @property
    def session(self) -> BrowserSession:
        return cast(ChatGPTAdapter, self.adapter).session_state

    async def authenticate(self, *, capture_browser: bool = True) -> SessionStatus:
        async with self.runtime.activity():
            auth = self.adapter.session.authenticate
            if isinstance(self.adapter, ChatGPTAdapter):
                return await self.adapter.session.authenticate(capture_browser=capture_browser)
            return await auth()

    async def move_project_chat(
        self, chat: AsyncChat | str, project: AsyncProject | str | None
    ) -> AsyncChat:
        """Move a ChatGPT chat that already belongs to a project into another project or detach it."""
        target_chat = (
            chat
            if isinstance(chat, AsyncChat)
            else await self.chats.get(chat, include_projects=True)
        )
        if target_chat.provider is not self:
            raise ValueError("Chat must belong to this ChatGPT provider session")
        if target_chat.project is None:
            raise UnsupportedOperationError(
                "Chat does not belong to a project; use chat.move_to(project) instead"
            )
        target_project = _resolve_project_arg(self, project)
        if target_project is not None and target_chat.remote_id and not target_project.remote_id:
            raise UnsupportedOperationError(
                "A remote chat cannot be moved into a project that is not persisted remotely"
            )
        target_id = (
            (target_project.remote_id or target_project.id) if target_project is not None else None
        )
        runtime = self.runtime
        context = runtime.new_context(
            "chat.move",
            resource_id=target_chat.remote_id or target_chat.id,
            parameters={"project_id": target_id},
        )

        async def work(ctx) -> AsyncChat:
            if target_chat.remote_id:
                async with runtime.activity():
                    self._require_provider()
                    await self._move_chat_data(target_chat.remote_id, ctx.parameters["project_id"])
            target_chat.project = target_project
            return target_chat

        async def on_success(ctx) -> None:
            await runtime.emit(
                "chat.moved",
                operation_id=ctx.operation_id,
                resource_id=target_chat.remote_id or target_chat.id,
                payload={"project_id": target_id},
            )

        return await execute_with_hooks(
            operation_name="chat.move",
            context=context,
            hooks_chain=(runtime.hooks, self.chats.hooks),
            work=work,
            on_success_event=on_success,
        )


class ChatGPTChat(BranchableChat, MovableChat, Chat):
    pass


class ChatGPTProjectsCollection(ProjectsCollection):
    def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
        memory_share: bool = True,
    ) -> Project:
        collection = cast(AsyncChatGPTProjectsCollection, self._collection)
        project = self._owner._call(
            collection.create(title, instruction=instruction, memory_share=memory_share)
        )
        return self._wrap(project)


class ChatGPTProvider(Provider):
    provider_type = ProviderType.CHATGPT
    async_provider_type = AsyncChatGPTProvider
    chat_wrapper_type = ChatGPTChat
    projects_collection_type = ChatGPTProjectsCollection

    def move_project_chat(
        self,
        chat: Chat | AsyncChat | str,
        project: Project | AsyncProject | str | None,
    ) -> Chat:
        """Move a ChatGPT chat that already belongs to a project into another project or detach it."""
        raw_chat = chat._resource if isinstance(chat, Chat) else chat
        raw_project = project._resource if isinstance(project, Project) else project
        updated = self._call(
            cast(AsyncChatGPTProvider, self._provider).move_project_chat(raw_chat, raw_project)
        )
        return self._wrap_chat(updated)
