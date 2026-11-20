from __future__ import annotations

from typing import Any, cast

from ...accounts.account import Account
from ...provider import Provider
from ...resources.chat.chat import (
    AsyncChat,
    Chat,
)
from ...resources.chat.collections import ChatsCollection
from ...resources.message.message import AsyncMessage
from ...resources.project.collections import ProjectsCollection
from ...resources.project.project import AsyncProject, Project
from .async_provider import AsyncChatGPTProvider
from .base import BaseChatGPTProvider
from .collections import AsyncChatGPTProjectsCollection
from .resources import (
    ChatGPTChat,
    ChatGPTMessage,
    ChatGPTProject,
)


class ChatGPTProjectsCollection(ProjectsCollection[ChatGPTProject]):
    def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
        memory_share: bool = True,
    ) -> ChatGPTProject:
        collection = cast(AsyncChatGPTProjectsCollection, self._collection)
        project = self._owner._call(
            collection.create(title, instruction=instruction, memory_share=memory_share)
        )
        return self._wrap(project)


class ChatGPTProvider(
    Provider[ChatsCollection[ChatGPTChat], ChatGPTProjectsCollection], BaseChatGPTProvider
):
    def _make_async_provider(self, account: Account, **options: Any) -> AsyncChatGPTProvider:
        return AsyncChatGPTProvider(account, **options)

    def _make_chats(self) -> ChatsCollection[ChatGPTChat]:
        return ChatsCollection(self, ChatGPTChat)

    def _make_projects(self) -> ChatGPTProjectsCollection:
        return ChatGPTProjectsCollection(self, ChatGPTProject)

    def _wrap_message(self, resource: AsyncMessage) -> ChatGPTMessage:
        return ChatGPTMessage._from_async(self, resource)

    def move_project_chat(
        self,
        chat: Chat | AsyncChat | str,
        project: Project | AsyncProject | str | None,
    ) -> ChatGPTChat:
        """Move a ChatGPT chat that already belongs to a project into another project or detach it."""
        raw_chat = chat._resource if isinstance(chat, Chat) else chat
        raw_project = project._resource if isinstance(project, Project) else project
        updated = self._call(
            cast(AsyncChatGPTProvider, self._provider).move_project_chat(raw_chat, raw_project)
        )
        return self.chats._wrap(updated)
