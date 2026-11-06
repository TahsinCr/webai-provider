from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..resources.chat import Chat
from .base import ResourceCollection

if TYPE_CHECKING:
    from ..resources.session import ProviderSession


class ChatsCollection(ResourceCollection[Chat]):
    def __init__(self, session: ProviderSession) -> None:
        super().__init__()
        self.session = session

    async def create(
        self,
        title: str = "Yeni Sohbet",
        *,
        project: Any = None,
        **options: Any,
    ) -> Chat:
        project_id = getattr(project, "id", str(project)) if project is not None else None
        chat = Chat(
            title=title,
            session=self.session,
            project_id=project_id,
        )
        self._items.insert(0, chat)
        if hasattr(self.session, "_bridge") and self.session._bridge:
            await self.session._bridge.create_chat(chat, **options)
        return chat

    async def refresh(self) -> list[Chat]:
        if hasattr(self.session, "_bridge") and self.session._bridge:
            chats = await self.session._bridge.list_chats(self.session)
            self._items = list(chats)
        return list(self._items)

    async def delete(self, chat_or_id: Chat | str) -> None:
        chat_id = chat_or_id.id if isinstance(chat_or_id, Chat) else chat_or_id
        target = self.get(chat_id)
        await target.delete()
        self._items = [c for c in self._items if c.id != chat_id]
