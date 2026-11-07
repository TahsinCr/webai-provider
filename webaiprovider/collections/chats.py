from __future__ import annotations

from typing import TYPE_CHECKING

from ..errors import UnsupportedOperationError
from ..resources.chat import Chat
from ..resources.project import Project
from .base import ManageableCollection

if TYPE_CHECKING:
    from ..resources.project import Project
    from ..session import Provider


class ChatsCollection(ManageableCollection[Chat]):
    def __init__(self, session: Provider) -> None:
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
        chat = Chat(
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

    async def move(self, chat: Chat | str, project: Project | str | None) -> Chat:
        target = chat if isinstance(chat, Chat) else self.get(chat)
        if target.session is not self.session:
            raise ValueError("Chat and provider session must match")
        if isinstance(project, Project):
            if project.session is not self.session:
                raise ValueError("Project and chat must belong to the same provider session")
            if target.remote_id and not project.remote_id:
                raise UnsupportedOperationError(
                    "A remote chat cannot be moved into a project that is not persisted remotely"
                )
            project_id = project.remote_id or project.id
        else:
            project_id = str(project) if project is not None else None
        if target.remote_id:
            await self.session._require_bridge().move_chat(target, project_id)
        target.project_id = project_id
        return target

    async def branch(
        self,
        chat: Chat | str,
        message_id: str,
        *,
        title: str | None = None,
    ) -> Chat:
        target = chat if isinstance(chat, Chat) else self.get(chat)
        branched = await target.branch(message_id, title=title)
        self._items.insert(0, branched)
        return branched
