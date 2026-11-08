from __future__ import annotations

from typing import cast

from ...async_provider import AsyncProvider
from ...provider import (
    Provider,
    SyncChat,
    SyncProject,
    SyncProjectsCollection,
)
from ...provider_type import ProviderType
from ...resources.chat import (
    BranchableChat,
    MovableChat,
)
from .collections import AsyncChatGPTProjectsCollection
from .resources import AsyncChatGPTChat


class AsyncChatGPTProvider(AsyncProvider):
    provider_type = ProviderType.CHATGPT
    _chat_type = AsyncChatGPTChat
    _projects_collection_type = AsyncChatGPTProjectsCollection


class ChatGPTChat(BranchableChat, MovableChat, SyncChat):
    pass


class ChatGPTProjectsCollection(SyncProjectsCollection):
    def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
        memory_share: bool = True,
    ) -> SyncProject:
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
