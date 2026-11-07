from __future__ import annotations

from typing import TYPE_CHECKING

from ..errors import ProviderError
from ..models import Page
from ..resources.chat import Chat
from .base import QueryableCollection

if TYPE_CHECKING:
    from ..resources.project import Project


class ProjectChatsCollection(QueryableCollection[Chat]):
    def __init__(self, project: Project) -> None:
        super().__init__()
        self.project = project
        self.next_cursor: str | None = None

    async def refresh(self, cursor: str | None = None) -> Page[Chat]:
        session = self.project.session
        if session is None:
            raise ProviderError("Project is not attached to an open provider")
        page = await session._require_bridge().list_project_chats(self.project, cursor)
        if cursor is None:
            self._items = list(page.items)
        else:
            known = {chat.id for chat in self._items}
            self._items.extend(chat for chat in page.items if chat.id not in known)
        self.next_cursor = page.next_cursor
        return page

    async def add(self, chat: Chat | str) -> Chat:
        session = self.project.session
        if session is None:
            raise ProviderError("Project is not attached to an open provider")
        target = chat if isinstance(chat, Chat) else session.chats.get(chat)
        return await session.chats.move(target, self.project)

    async def remove(self, chat: Chat | str) -> Chat:
        session = self.project.session
        if session is None:
            raise ProviderError("Project is not attached to an open provider")
        target = chat if isinstance(chat, Chat) else self.get(chat)
        if target.project_id != (self.project.remote_id or self.project.id):
            raise ValueError("Chat does not belong to this project")
        return await session.chats.move(target, None)
