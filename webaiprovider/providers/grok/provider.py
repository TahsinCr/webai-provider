from __future__ import annotations

from typing import Any

from ...accounts.account import Account
from ...provider import Provider
from ...resources.chat.collections import ChatsCollection
from ...resources.message.message import AsyncMessage
from ...resources.project.collections import ProjectsCollection
from .async_provider import AsyncGrokProvider
from .base import BaseGrokProvider
from .resources import (
    GrokChat,
    GrokMessage,
    GrokProject,
)


class GrokProvider(
    Provider[ChatsCollection[GrokChat], ProjectsCollection[GrokProject]], BaseGrokProvider
):
    def _make_async_provider(self, account: Account, **options: Any) -> AsyncGrokProvider:
        return AsyncGrokProvider(account, **options)

    def _make_chats(self) -> ChatsCollection[GrokChat]:
        return ChatsCollection(self, GrokChat)

    def _make_projects(self) -> ProjectsCollection[GrokProject]:
        return ProjectsCollection(self, GrokProject)

    def _wrap_message(self, resource: AsyncMessage) -> GrokMessage:
        return GrokMessage._from_async(self, resource)
