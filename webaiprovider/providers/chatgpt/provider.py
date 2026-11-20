from __future__ import annotations

from typing import Any

from ...accounts.account import Account
from ...provider import Provider
from ...resources.chat.collections import ChatsCollection
from ...resources.message.message import AsyncMessage
from .async_provider import AsyncChatGPTProvider
from .base import BaseChatGPTProvider
from .collections import ChatGPTProjectsCollection
from .resources import (
    ChatGPTChat,
    ChatGPTMessage,
    ChatGPTProject,
)


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
