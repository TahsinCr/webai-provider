from __future__ import annotations

from typing import TYPE_CHECKING

from ..errors import ProviderError
from ..models import Conversation, Page
from ..resources.chat import Chat
from .base import QueryableCollection
from .chats import _chat_from_conversation

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
        if not self.project.remote_id:
            page = Page(())
        else:
            page = await session._require_provider().list_project_chats(
                self.project.remote_id, cursor
            )
        if not isinstance(page, Page) or any(
            not isinstance(item, Conversation) for item in page.items
        ):
            raise ProviderError("Provider returned an invalid project conversation page")
        chats = tuple(_chat_from_conversation(item, session) for item in page.items)
        if cursor is None:
            self._items = list(chats)
        else:
            known = {chat.id for chat in self._items}
            self._items.extend(chat for chat in chats if chat.id not in known)
        self.next_cursor = page.next_cursor
        return Page(chats, page.next_cursor)
