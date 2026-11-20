from __future__ import annotations

from typing import Any

from ...accounts.account import Account
from ...provider import Provider
from ...resources.chat.collections import ChatsCollection
from ...resources.message.message import AsyncMessage
from ...resources.project.collections import ProjectsCollection
from .async_provider import AsyncGeminiProvider
from .base import BaseGeminiProvider
from .resources import (
    GeminiChat,
    GeminiMessage,
    GeminiProject,
)


class GeminiProvider(
    Provider[ChatsCollection[GeminiChat], ProjectsCollection[GeminiProject]], BaseGeminiProvider
):
    def _make_async_provider(self, account: Account, **options: Any) -> AsyncGeminiProvider:
        return AsyncGeminiProvider(account, **options)

    def _make_chats(self) -> ChatsCollection[GeminiChat]:
        return ChatsCollection(self, GeminiChat)

    def _make_projects(self) -> ProjectsCollection[GeminiProject]:
        return ProjectsCollection(self, GeminiProject)

    def _wrap_message(self, resource: AsyncMessage) -> GeminiMessage:
        return GeminiMessage._from_async(self, resource)
