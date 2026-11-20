from __future__ import annotations

from typing import Any

from ...accounts.account import Account
from ...provider import Provider
from ...resources.chat.collections import ChatsCollection
from ...resources.message.message import AsyncMessage
from ...resources.project.collections import ProjectsCollection
from .async_provider import AsyncClaudeProvider
from .base import BaseClaudeProvider
from .resources import (
    ClaudeChat,
    ClaudeMessage,
    ClaudeProject,
)


class ClaudeProvider(
    Provider[ChatsCollection[ClaudeChat], ProjectsCollection[ClaudeProject]], BaseClaudeProvider
):
    def _make_async_provider(self, account: Account, **options: Any) -> AsyncClaudeProvider:
        return AsyncClaudeProvider(account, **options)

    def _make_chats(self) -> ChatsCollection[ClaudeChat]:
        return ChatsCollection(self, ClaudeChat)

    def _make_projects(self) -> ProjectsCollection[ClaudeProject]:
        return ProjectsCollection(self, ClaudeProject)

    def _wrap_message(self, resource: AsyncMessage) -> ClaudeMessage:
        return ClaudeMessage._from_async(self, resource)
