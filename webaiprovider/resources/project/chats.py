from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ...collections import AsyncRemoteCollection, Page, _RemoteCollectionFacade
from ...errors import ProviderError
from ..chat.chat import AsyncChat, Chat
from ..chat.collections import _chat_from_conversation
from ..chat.types import Conversation
from .project import AsyncProject

if TYPE_CHECKING:
    from ...provider import Provider


class AsyncProjectChatsCollection(AsyncRemoteCollection[AsyncChat]):
    """Remote collection of conversations scoped to one project."""

    def __init__(self, project: AsyncProject) -> None:
        self.project = project

    async def _emit_refreshed(self, count: int) -> None:
        session = self.project.session
        runtime = getattr(session, "runtime", None) if session is not None else None
        if runtime is not None:
            await runtime.emit(
                "collection.refreshed",
                resource_id=self.project.remote_id or self.project.id,
                payload={"collection": "project.chats", "count": count},
            )

    async def _fetch_page(
        self,
        cursor: str | None,
        page_limit: int | None,
        **options: Any,
    ) -> Page[AsyncChat]:
        del page_limit, options
        session = self.project.session
        if session is None:
            raise ProviderError("Project is not attached to an open provider")
        if not self.project.remote_id:
            return Page(())
        async with session.runtime.activity():
            session._require_provider()
            page = await session._list_project_chats_data(self.project.remote_id, cursor=cursor)
        if not isinstance(page, Page) or any(
            not isinstance(item, Conversation) for item in page.items
        ):
            raise ProviderError("Provider returned an invalid project conversation page")
        project_map = {self.project.remote_id: self.project}
        chats = []
        for item in page.items:
            chat = _chat_from_conversation(item, session, project_map)
            if chat.project is None:
                chat.project = self.project
            chats.append(chat)
        return Page(tuple(chats), page.next_cursor)


class ProjectChatsCollection(_RemoteCollectionFacade[AsyncChat, Chat]):
    def __init__(self, owner: Provider, project: AsyncProject) -> None:
        super().__init__(owner, project.chats)

    def _wrap(self, item: AsyncChat) -> Chat:
        return self._owner._wrap_chat(item)
