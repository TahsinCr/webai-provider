from __future__ import annotations

from typing import cast

from ...provider import Provider
from ...resources.chat.chat import (
    AsyncChat,
    Chat,
)
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


class ChatGPTProvider(BaseChatGPTProvider, Provider):
    async_provider_type = AsyncChatGPTProvider
    chat_wrapper_type = ChatGPTChat
    project_wrapper_type = ChatGPTProject
    message_wrapper_type = ChatGPTMessage
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
