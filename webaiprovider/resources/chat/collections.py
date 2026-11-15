from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from ...base import Capability
from ...collections import (
    AsyncManageableCollection,
    ManageableCollection,
    Page,
    QueryableCollection,
    _CollectionFacade,
)
from ...errors import ProviderError, RemoteNotFound
from ..project.project import AsyncProject, Project
from .chat import AsyncChat, Chat, _project_from_data, _resolve_project_arg
from .types import Conversation

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider

_PROJECT_CHAT_CONCURRENCY = 4
_MAX_PROJECT_CHAT_PAGES = 100


def _chat_from_conversation(
    item: Conversation,
    session: AsyncProvider,
    projects: Mapping[str, AsyncProject] | None = None,
) -> AsyncChat:
    project = _project_from_data(session, item.project, projects)
    return session._chat_type(
        title=item.title,
        remote_id=item.origin.remote_id,
        project=project,
        temporary=bool(item.metadata.get("temporary", False)),
        metadata=item.metadata,
        session=session,
    )


def _require_conversation_page(page: Any, *, label: str = "conversation") -> Page[Conversation]:
    if not isinstance(page, Page) or any(not isinstance(item, Conversation) for item in page.items):
        raise ProviderError(f"Provider returned an invalid {label} page")
    return page


def _merge_chat_lists(
    normal_chats: list[AsyncChat],
    project_chats: list[AsyncChat],
) -> list[AsyncChat]:
    by_key: dict[str, AsyncChat] = {}
    order: list[str] = []
    for chat in normal_chats:
        key = chat.remote_id or chat.id
        if key not in by_key:
            order.append(key)
        by_key[key] = chat
    for chat in project_chats:
        key = chat.remote_id or chat.id
        existing = by_key.get(key)
        if existing is None:
            order.append(key)
            by_key[key] = chat
        elif existing.project is None and chat.project is not None:
            by_key[key] = chat
    return [by_key[key] for key in order]


class AsyncChatsCollection(AsyncManageableCollection[AsyncChat]):
    def __init__(self, session: AsyncProvider) -> None:
        super().__init__()
        self.session = session
        self._projects_loaded = False

    async def create(
        self,
        title: str = "Yeni Sohbet",
        *,
        project: AsyncProject | str | None = None,
        temporary: bool = False,
    ) -> AsyncChat:
        resolved_project = (
            _resolve_project_arg(self.session, project) if project is not None else None
        )
        chat = self.session._chat_type(
            title=title,
            session=self.session,
            project=resolved_project,
            temporary=temporary,
        )
        self._items.insert(0, chat)
        return chat

    async def _fetch_project_chats(
        self, projects: list[AsyncProject]
    ) -> tuple[dict[str, AsyncProject], list[AsyncChat]]:
        project_map = {p.remote_id: p for p in projects if p.remote_id}
        if not project_map:
            return project_map, []
        provider = self.session._require_provider()
        semaphore = asyncio.Semaphore(_PROJECT_CHAT_CONCURRENCY)

        async def fetch_one(project: AsyncProject) -> list[AsyncChat]:
            assert project.remote_id is not None
            chats: list[AsyncChat] = []
            cursor: str | None = None
            seen_cursors: set[str] = set()
            async with semaphore:
                for _ in range(_MAX_PROJECT_CHAT_PAGES):
                    raw_page = (
                        await provider.list_project_chats(project.remote_id)
                        if cursor is None
                        else await provider.list_project_chats(project.remote_id, cursor=cursor)
                    )
                    page = _require_conversation_page(raw_page, label="project conversation")
                    for item in page.items:
                        chat = _chat_from_conversation(item, self.session, project_map)
                        if chat.project is None:
                            chat.project = project
                        chats.append(chat)
                    next_cursor = page.next_cursor
                    if next_cursor is None:
                        return chats
                    if (
                        not isinstance(next_cursor, str)
                        or not next_cursor
                        or next_cursor in seen_cursors
                    ):
                        raise ProviderError(
                            "Provider project chat pagination returned an invalid or repeating cursor"
                        )
                    seen_cursors.add(next_cursor)
                    cursor = next_cursor
            raise ProviderError(
                f"Provider project chat pagination exceeded {_MAX_PROJECT_CHAT_PAGES} pages"
            )

        tasks = [asyncio.create_task(fetch_one(project)) for project in project_map.values()]
        try:
            batches = await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        return project_map, [chat for batch in batches for chat in batch]

    async def refresh(self, *, include_projects: bool = False) -> list[AsyncChat]:
        async with self.session._activity():
            provider = self.session._require_provider()
            if include_projects and Capability.PROJECTS in self.session.capabilities:
                raw_page, projects = await asyncio.gather(
                    provider.list_chats(),
                    self.session.projects.refresh(),
                )
                page = _require_conversation_page(raw_page)
                project_map, project_chats = await self._fetch_project_chats(projects)
                normal_chats = [
                    _chat_from_conversation(item, self.session, project_map) for item in page.items
                ]
                self._items = _merge_chat_lists(normal_chats, project_chats)
                self._projects_loaded = True
                return list(self._items)

            page = _require_conversation_page(await provider.list_chats())
            project_map = {p.remote_id: p for p in self.session.projects._items if p.remote_id}
            self._items = [
                _chat_from_conversation(item, self.session, project_map) for item in page.items
            ]
            self._projects_loaded = (
                include_projects and Capability.PROJECTS not in self.session.capabilities
            )
            if include_projects:
                return list(self._items)
            return [chat for chat in self._items if chat.project is None]

    async def list(self, *, include_projects: bool = False) -> list[AsyncChat]:
        if (
            include_projects
            and not self._projects_loaded
            and getattr(self.session, "_opened", False)
            and Capability.PROJECTS in self.session.capabilities
        ):
            async with self.session._activity():
                if not self._items:
                    return await self.refresh(include_projects=True)
                projects = (
                    list(self.session.projects._items) or await self.session.projects.refresh()
                )
                _, project_chats = await self._fetch_project_chats(projects)
                self._items = _merge_chat_lists(self._items, project_chats)
                self._projects_loaded = True
        if include_projects:
            return list(self._items)
        return [chat for chat in self._items if chat.project is None]

    async def get(self, item_id: str, /, *, include_projects: bool = False) -> AsyncChat:
        for item in self._items:
            if (QueryableCollection._extract_id(item) == item_id or item.remote_id == item_id) and (
                include_projects or item.project is None
            ):
                return item
        return QueryableCollection._get_from(
            await self.list(include_projects=include_projects), item_id
        )

    def _get_cached_or_fetch(self, chat: AsyncChat | str):
        if isinstance(chat, AsyncChat):
            if chat.session is not self.session:
                raise ValueError("Chat belongs to a different provider session")
            return chat
        try:
            return QueryableCollection._get_from(self._items, chat)
        except RemoteNotFound:
            return None

    async def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        include_projects: bool = False,
    ) -> AsyncChat:
        return QueryableCollection._find_in(
            await self.list(include_projects=include_projects),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    async def search(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        include_projects: bool = False,
    ) -> list[AsyncChat]:
        return QueryableCollection._search_in(
            await self.list(include_projects=include_projects),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    async def update(
        self,
        chat: AsyncChat | str,
        *,
        title: str | None = None,
    ) -> AsyncChat:
        target = self._get_cached_or_fetch(chat)
        if target is None:
            target = await self.get(str(chat), include_projects=True)
        if title is not None and title != target.title:
            if target.remote_id:
                async with self.session._activity():
                    await self.session._require_provider().rename_chat(target.remote_id, title)
            target.title = title
        return target

    async def delete(self, chat: AsyncChat | str) -> None:
        target = self._get_cached_or_fetch(chat)
        if target is None:
            target = await self.get(str(chat), include_projects=True)
        if target.remote_id:
            async with self.session._activity():
                await self.session._require_provider().delete_chat(target.remote_id)
        self._items = [c for c in self._items if c.id != target.id]


class ChatsCollection(_CollectionFacade[AsyncChat, Chat], ManageableCollection[Chat]):
    def __init__(self, owner: Provider) -> None:
        super().__init__(owner, owner._provider.chats)

    def _wrap(self, item: AsyncChat) -> Chat:
        return self._owner._wrap_chat(item)

    def refresh(self, *, include_projects: bool = False) -> list[Chat]:
        return [
            self._wrap(item)
            for item in self._owner._call(
                self._collection.refresh(include_projects=include_projects)
            )
        ]

    def list(self, *, include_projects: bool = False) -> list[Chat]:
        self._items = [
            self._wrap(item)
            for item in self._owner._call(self._collection.list(include_projects=include_projects))
        ]
        return list(self._items)

    def get(self, item_id: str, /, *, include_projects: bool = False) -> Chat:
        return self._wrap(
            self._owner._call(self._collection.get(item_id, include_projects=include_projects))
        )

    def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        include_projects: bool = False,
    ) -> Chat:
        return self._wrap(
            self._owner._call(
                self._collection.find(
                    query,
                    exact=exact,
                    startswith=startswith,
                    endswith=endswith,
                    include_projects=include_projects,
                )
            )
        )

    def search(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        include_projects: bool = False,
    ) -> list[Chat]:
        return [
            self._wrap(item)
            for item in self._owner._call(
                self._collection.search(
                    query,
                    exact=exact,
                    startswith=startswith,
                    endswith=endswith,
                    include_projects=include_projects,
                )
            )
        ]

    def create(
        self,
        title: str = "New chat",
        *,
        project: Project | AsyncProject | str | None = None,
        temporary: bool = False,
    ) -> Chat:
        target = project._resource if isinstance(project, Project) else project
        chat = self._owner._call(
            self._collection.create(title, project=target, temporary=temporary)
        )
        return self._wrap(chat)

    def update(
        self,
        chat: Chat | AsyncChat | str,
        *,
        title: str | None = None,
        **fields: Any,
    ) -> Chat:
        if fields:
            raise TypeError(f"Unsupported chat fields: {', '.join(fields)}")
        target = chat._resource if isinstance(chat, Chat) else chat
        return self._wrap(self._owner._call(self._collection.update(target, title=title)))

    def delete(self, chat: Chat | AsyncChat | str) -> None:
        target = chat._resource if isinstance(chat, Chat) else chat
        self._owner._call(self._collection.delete(target))
