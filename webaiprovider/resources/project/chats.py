from __future__ import annotations

from typing import Any

from ...collections import AsyncRemoteCollection, Page, RemoteCollection
from ...errors import ProviderError
from ..chat.chat import AsyncChat, Chat
from ..chat.types import Conversation
from .project import AsyncProject, Project


def _build_project_chat_page(project: Any, provider: Any, page: Any) -> Page[Any]:
    if not isinstance(page, Page) or any(not isinstance(item, Conversation) for item in page.items):
        raise ProviderError("Provider returned an invalid project conversation page")
    project_map = {project.remote_id: project}
    chats = []
    for item in page.items:
        chat = provider.chats._from_data(item, project_map)
        if chat.project is None:
            chat.project = project
        chats.append(chat)
    return Page(tuple(chats), page.next_cursor)


class AsyncProjectChatsCollection(AsyncRemoteCollection[AsyncChat]):
    """Remote collection of conversations scoped to one project."""

    _collection_name = "project.chats"

    @property
    def _read_provider(self):
        return self.project.provider

    @property
    def _scope_id(self):
        return self.project.remote_id

    def __init__(self, project: AsyncProject) -> None:
        self.project = project

    async def _fetch_page(
        self,
        cursor: str | None,
        page_limit: int | None,
        **options: Any,
    ) -> Page[AsyncChat]:
        del options
        provider = self.project.provider
        if provider is None:
            raise ProviderError("Project is not attached to an open provider")
        if not self.project.remote_id:
            return Page(())
        async with provider._activity():
            provider._require_provider()
            page = await provider._list_project_chats_data(
                self.project.remote_id, cursor=cursor, limit=page_limit
            )
        return _build_project_chat_page(self.project, provider, page)


class ProjectChatsCollection(RemoteCollection[Chat]):
    """Blocking remote collection of conversations scoped to one project."""

    _collection_name = "project.chats"

    @property
    def _read_provider(self):
        return self.project.provider

    @property
    def _scope_id(self):
        return self.project.remote_id

    def __init__(self, project: Project) -> None:
        self.project = project

    def _fetch_page(
        self,
        cursor: str | None,
        page_limit: int | None,
        **options: Any,
    ) -> Page[Chat]:
        del options
        provider = self.project.provider
        if provider is None:
            raise ProviderError("Project is not attached to an open provider")
        if not self.project.remote_id:
            return Page(())
        with provider._activity():
            provider._require_provider()
            page = provider._list_project_chats_data(
                self.project.remote_id, cursor=cursor, limit=page_limit
            )
        return _build_project_chat_page(self.project, provider, page)
