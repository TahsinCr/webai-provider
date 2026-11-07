from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..errors import UnsupportedOperationError
from ..resources.chat import Chat
from ..resources.project import Project
from .base import ManageableCollection

if TYPE_CHECKING:
    from ..resources.project import Project
    from ..session import ProviderSession


class ChatsCollection(ManageableCollection[Chat]):
    def __init__(self, session: ProviderSession) -> None:
        super().__init__()
        self.session = session

    async def create(
        self,
        title: str = "Yeni Sohbet",
        *,
        project: Project | str | None = None,
        **options: Any,
    ) -> Chat:
        if options:
            raise UnsupportedOperationError("Provider-specific chat options are not supported")
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
        if hasattr(self.session, "_bridge") and self.session._bridge:
            chats = await self.session._bridge.list_chats(self.session)
            self._items = list(chats)
        return list(self._items)

    async def update(
        self,
        chat: Chat | str,
        *,
        title: str | None = None,
        **options: Any,
    ) -> Chat:
        target = chat if isinstance(chat, Chat) else self.get(chat)
        new_title = title if title is not None else target.title
        if new_title != target.title or options:
            target.title = new_title
            if (
                self.session is not None
                and hasattr(self.session, "_bridge")
                and self.session._bridge
            ):
                await self.session._bridge.rename_chat(target, new_title)
        return target

    async def delete(self, chat: Chat | str) -> None:
        target = chat if isinstance(chat, Chat) else self.get(chat)
        await target.delete()
        self._items = [c for c in self._items if c.id != target.id]

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
