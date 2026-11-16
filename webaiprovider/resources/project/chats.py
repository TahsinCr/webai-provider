from __future__ import annotations

from typing import TYPE_CHECKING

from ...collections import AsyncQueryableCollection, Page, QueryableCollection, _CollectionFacade
from ...errors import ProviderError
from ..chat.chat import AsyncChat, Chat
from ..chat.collections import _chat_from_conversation
from ..chat.types import Conversation
from .project import AsyncProject

if TYPE_CHECKING:
    from ...provider import Provider


class AsyncProjectChatsCollection(AsyncQueryableCollection[AsyncChat]):
    def __init__(self, project: AsyncProject) -> None:
        super().__init__()
        self.project = project
        self.next_cursor: str | None = None

    async def refresh(self, cursor: str | None = None) -> Page[AsyncChat]:
        session = self.project.session
        if session is None:
            raise ProviderError("Project is not attached to an open provider")
        if not self.project.remote_id:
            page = Page(())
        else:
            async with session._activity():
                page = await session._require_provider().list_project_chats(
                    self.project.remote_id, cursor
                )
        if not isinstance(page, Page) or any(
            not isinstance(item, Conversation) for item in page.items
        ):
            raise ProviderError("Provider returned an invalid project conversation page")
        project_map = {self.project.remote_id: self.project} if self.project.remote_id else {}
        existing = {
            chat.remote_id: chat for chat in (*session.chats._items, *self._items) if chat.remote_id
        }
        chats = []
        for item in page.items:
            chat = existing.get(item.origin.remote_id)
            if chat is None:
                chat = _chat_from_conversation(item, session, project_map)
            else:
                chat.title = item.title
                chat.metadata = dict(item.metadata)
            chats.append(chat)
        chats = tuple(chats)
        for chat in chats:
            if chat.project is None:
                chat.project = self.project
        if cursor is None:
            self._items = list(chats)
        else:
            known = {chat.id for chat in self._items}
            self._items.extend(chat for chat in chats if chat.id not in known)
        self.next_cursor = page.next_cursor
        return Page(chats, page.next_cursor)


class ProjectChatsCollection(_CollectionFacade[AsyncChat, Chat], QueryableCollection[Chat]):
    def __init__(self, owner: Provider, project: AsyncProject) -> None:
        super().__init__(owner, project.chats)
        self.next_cursor: str | None = None

    def _wrap(self, item: AsyncChat) -> Chat:
        return self._owner._wrap_chat(item)

    def refresh(self, cursor: str | None = None) -> Page[Chat]:
        page = self._owner._call(self._collection.refresh(cursor))
        self.next_cursor = page.next_cursor
        return Page(tuple(self._wrap(item) for item in page.items), page.next_cursor)
