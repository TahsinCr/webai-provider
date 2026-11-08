from __future__ import annotations

from typing import TYPE_CHECKING

from ..resources.chat import Chat
from ..resources.project import Project
from .base import ManageableCollection

if TYPE_CHECKING:
    from ..async_provider import AsyncProvider
    from ..resources.project import Project


class ChatsCollection(ManageableCollection[Chat]):
    def __init__(self, session: AsyncProvider) -> None:
        super().__init__()
        self.session = session

    async def create(
        self,
        title: str = "Yeni Sohbet",
        *,
        project: Project | str | None = None,
    ) -> Chat:
        project_id = (
            (project.remote_id or project.id)
            if isinstance(project, Project)
            else str(project)
            if project is not None
            else None
        )
        chat = self.session._chat_type(
            title=title,
            session=self.session,
            project_id=project_id,
        )
        self._items.insert(0, chat)
        return chat

    async def refresh(self) -> list[Chat]:
        chats = await self.session._require_bridge().list_chats(self.session)
        self._items = list(chats)
        return list(self._items)

    async def update(
        self,
        chat: Chat | str,
        *,
        title: str | None = None,
    ) -> Chat:
        target = chat if isinstance(chat, Chat) else self.get(chat)
        if title is not None and title != target.title:
            if target.remote_id:
                await self.session._require_bridge().rename_chat(target, title)
            target.title = title
        return target

    async def delete(self, chat: Chat | str) -> None:
        target = chat if isinstance(chat, Chat) else self.get(chat)
        await target.delete()
        self._items = [c for c in self._items if c.id != target.id]
