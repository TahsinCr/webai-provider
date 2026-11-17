from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from ...base import Capability
from ...collections import (
    AsyncMutableRemoteCollection,
    Page,
    _MutableRemoteCollectionFacade,
    validate_limit,
)
from ...core.concurrency import gather_bounded
from ...core.operations import execute_with_hooks
from ...errors import FeatureUnavailable, ProviderError, RemoteNotFound
from ..project.project import AsyncProject, Project
from .chat import AsyncChat, Chat, _project_from_data, _resolve_project_arg
from .types import Conversation

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider
    from ..response.response import Response

_PROJECT_CHAT_CONCURRENCY = 4
_MAX_PROJECT_CHAT_PAGES = 100


def _chat_from_conversation(
    item: Conversation,
    provider: AsyncProvider,
    projects: Mapping[str, AsyncProject] | None = None,
) -> AsyncChat:
    project = _project_from_data(provider, item.project, projects)
    return provider._chat_type(
        title=item.title,
        remote_id=item.id,
        project=project,
        temporary=bool(item.metadata.get("temporary", False)),
        metadata=item.metadata,
        provider=provider,
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
            existing.project = chat.project
            existing.title = chat.title
            existing.temporary = chat.temporary
            existing.metadata.update(chat.metadata)
    return [by_key[key] for key in order]


class AsyncChatsCollection(AsyncMutableRemoteCollection[AsyncChat]):
    def __init__(self, provider: AsyncProvider) -> None:
        super().__init__()
        self.provider = provider

    async def _emit_refreshed(self, count: int) -> None:
        runtime = getattr(self.provider, "runtime", None)
        if runtime is not None:
            await runtime.emit(
                "collection.refreshed",
                payload={"collection": "chats", "count": count},
            )

    async def _fetch_page(
        self,
        cursor: str | None,
        page_limit: int | None,
        **options: Any,
    ) -> Page[AsyncChat]:
        include_projects = bool(options.get("include_projects", False))
        session = self.provider
        async with session.runtime.activity():
            session._require_provider()
            limit_arg = page_limit or self._default_page_size
            raw_page = await session._list_chats_data(cursor=cursor, limit=limit_arg)
            page = _require_conversation_page(raw_page)
            chats = tuple(
                _chat_from_conversation(item, session)
                for item in page.items
                if include_projects or item.project is None
            )
            return Page(chats, page.next_cursor)

    async def _fetch_project_chats(
        self, projects: list[AsyncProject], slots: asyncio.Semaphore
    ) -> list[AsyncChat]:
        project_map = {p.remote_id: p for p in projects if p.remote_id}
        session = self.provider

        async def fetch_one(project: AsyncProject) -> list[AsyncChat]:
            async with slots:
                return await fetch_pages(project)

        async def fetch_pages(project: AsyncProject) -> list[AsyncChat]:
            assert project.remote_id is not None
            chats: list[AsyncChat] = []
            cursor: str | None = None
            seen_cursors: set[str] = set()
            for _ in range(_MAX_PROJECT_CHAT_PAGES):
                raw_page = await session._list_project_chats_data(project.remote_id, cursor=cursor)
                page = _require_conversation_page(raw_page, label="project conversation")
                for item in page.items:
                    chat = _chat_from_conversation(item, session, project_map)
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

        batches = await gather_bounded(
            tuple(project_map.values()), fetch_one, limit=_PROJECT_CHAT_CONCURRENCY
        )
        return [chat for batch in batches for chat in batch]

    async def _collect_with_projects(self, *, limit: int | None) -> list[AsyncChat]:
        validated = validate_limit(limit)
        if validated == 0:
            return []
        session = self.provider
        slots = asyncio.Semaphore(_PROJECT_CHAT_CONCURRENCY)
        collect_normal = super()._collect_remote

        async def normal_chats():
            async with slots:
                return await collect_normal(limit=None)

        async def project_chats():
            projects = await session.projects.list(limit=None)
            return await self._fetch_project_chats(projects, slots)

        async with session.runtime.activity():
            session._require_provider()
            normal_task = asyncio.create_task(normal_chats())
            projects_task = asyncio.create_task(project_chats())
            try:
                normal, scoped = await asyncio.gather(normal_task, projects_task)
            except BaseException:
                normal_task.cancel()
                projects_task.cancel()
                await asyncio.gather(normal_task, projects_task, return_exceptions=True)
                raise
            merged = _merge_chat_lists(normal, scoped)
            return merged if validated is None else merged[:validated]

    async def _iter_remote_pages(self, *, limit: int | None = None, **options: Any):
        include_projects = bool(options.get("include_projects", False))
        if include_projects and Capability.PROJECTS in self.provider.capabilities:
            items = await self._collect_with_projects(limit=limit)
            if items:
                yield items
            return
        async for batch in super()._iter_remote_pages(limit=limit, **options):
            yield batch

    async def list(
        self,
        predicate: Callable[[AsyncChat], bool] | None = None,
        *,
        limit: int | None = 50,
        include_projects: bool = False,
    ) -> list[AsyncChat]:
        return await self._collect_remote(
            predicate,
            limit=limit,
            include_projects=include_projects,
        )

    async def refresh(
        self,
        *,
        limit: int | None = None,
        include_projects: bool = False,
    ) -> list[AsyncChat]:
        items = await self._collect_remote(limit=limit, include_projects=include_projects)
        await self._emit_refreshed(len(items))
        return items

    async def _fetch_conversation(
        self, item_id: str, *, metadata_only: bool = False
    ) -> Conversation:
        session = self.provider
        async with session.runtime.activity():
            session._require_provider()
            load = session._get_chat_metadata if metadata_only else session._get_chat_data
            return await load(item_id)

    async def _fetch_by_id(self, item_id: str, *, metadata_only: bool = False) -> AsyncChat | None:
        try:
            conv = await self._fetch_conversation(item_id, metadata_only=metadata_only)
        except FeatureUnavailable:
            return None
        if not isinstance(conv, Conversation):
            return None
        return _chat_from_conversation(conv, self.provider)

    async def get(
        self, item_id: str, /, *, include_projects: bool = False, metadata_only: bool = False
    ) -> AsyncChat:
        """Get a chat, preferring a metadata-only lookup when requested and supported."""
        direct = await self._fetch_by_id(item_id, metadata_only=metadata_only)
        if direct is not None:
            if not include_projects and direct.project is not None:
                raise RemoteNotFound(f"Record was not found: {item_id}")
            return direct
        async for batch in self._iter_remote_pages(limit=None, include_projects=include_projects):
            for item in batch:
                if self._item_matches_exact_id(item, item_id):
                    return item
        raise RemoteNotFound(f"Record was not found: {item_id}")

    async def get_with_responses(
        self, item_id: str, /, *, limit: int | None = 50, include_projects: bool = False
    ) -> tuple[AsyncChat, list[Response]]:
        """Read a chat and its latest responses from the same remote history snapshot."""
        validated = validate_limit(limit)
        conversation = await self._fetch_conversation(item_id, metadata_only=validated == 0)
        if not isinstance(conversation, Conversation):
            raise ProviderError("Provider returned an invalid conversation")
        if not include_projects and conversation.project is not None:
            raise RemoteNotFound(f"Record was not found: {item_id}")
        chat = _chat_from_conversation(conversation, self.provider)
        if validated == 0:
            return chat, []
        responses = chat.responses._apply_history(conversation, self.provider)
        return chat, responses if validated is None else responses[-validated:]

    async def first(
        self,
        predicate: Callable[[AsyncChat], bool] | None = None,
        /,
        *,
        include_projects: bool = False,
    ) -> AsyncChat | None:
        return await super().first(predicate, include_projects=include_projects)

    async def last(
        self,
        predicate: Callable[[AsyncChat], bool] | None = None,
        /,
        *,
        include_projects: bool = False,
    ) -> AsyncChat | None:
        return await super().last(predicate, include_projects=include_projects)

    async def single(
        self,
        predicate: Callable[[AsyncChat], bool] | None = None,
        /,
        *,
        include_projects: bool = False,
    ) -> AsyncChat | None:
        return await super().single(predicate, include_projects=include_projects)

    async def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        single: bool = False,
        last: bool = False,
        include_projects: bool = False,
    ) -> AsyncChat | None:
        return await super().find(
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
            single=single,
            last=last,
            include_projects=include_projects,
        )

    async def search(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        limit: int | None = None,
        include_projects: bool = False,
    ) -> list[AsyncChat]:
        return await super().search(
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
            limit=limit,
            include_projects=include_projects,
        )

    async def create(
        self,
        title: str = "Yeni Sohbet",
        *,
        project: AsyncProject | str | None = None,
        temporary: bool = False,
    ) -> AsyncChat:
        resolved_project = (
            _resolve_project_arg(self.provider, project) if project is not None else None
        )
        runtime = self.provider.runtime
        context = runtime.new_context(
            "chat.create",
            parameters={"title": title, "project": resolved_project, "temporary": temporary},
        )

        async def work(ctx) -> AsyncChat:
            return self.provider._chat_type(
                title=ctx.parameters["title"],
                provider=self.provider,
                project=ctx.parameters["project"],
                temporary=ctx.parameters["temporary"],
            )

        async def on_success(ctx) -> None:
            chat: AsyncChat = ctx.result
            await runtime.emit(
                "chat.prepared",
                operation_id=ctx.operation_id,
                resource_id=chat.id,
                payload={"title": chat.title},
            )

        return await execute_with_hooks(
            operation_name="chat.create",
            context=context,
            hooks_chain=(runtime.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )

    async def update(
        self,
        chat: AsyncChat | str,
        *,
        title: str | None = None,
    ) -> AsyncChat:
        if isinstance(chat, AsyncChat):
            if chat.provider is not self.provider:
                raise ValueError("Chat belongs to a different provider session")
            target = chat
        else:
            target = await self.get(str(chat), include_projects=True)

        runtime = self.provider.runtime
        context = runtime.new_context(
            "chat.update",
            resource_id=target.remote_id or target.id,
            parameters={"target": target, "title": title},
        )

        async def work(ctx) -> AsyncChat:
            new_title = ctx.parameters["title"]
            if new_title is not None and new_title != target.title:
                if target.remote_id:
                    async with runtime.activity():
                        self.provider._require_provider()
                        await self.provider._rename_chat_data(target.remote_id, new_title)
                target.title = new_title
            return target

        async def on_success(ctx) -> None:
            await runtime.emit(
                "chat.updated",
                operation_id=ctx.operation_id,
                resource_id=target.remote_id or target.id,
                payload={"title": target.title},
            )

        return await execute_with_hooks(
            operation_name="chat.update",
            context=context,
            hooks_chain=(runtime.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )

    async def delete(self, chat: AsyncChat | str) -> None:
        if isinstance(chat, AsyncChat):
            if chat.provider is not self.provider:
                raise ValueError("Chat belongs to a different provider session")
            target = chat
        else:
            target = await self.get(str(chat), include_projects=True)

        runtime = self.provider.runtime
        rid = target.remote_id or target.id
        context = runtime.new_context(
            "chat.delete",
            resource_id=rid,
            parameters={"target": target},
        )

        async def work(_ctx) -> None:
            if target.remote_id:
                async with runtime.activity():
                    self.provider._require_provider()
                    await self.provider._delete_chat_data(target.remote_id)

        async def on_success(ctx) -> None:
            await runtime.emit(
                "chat.deleted",
                operation_id=ctx.operation_id,
                resource_id=rid,
            )

        await execute_with_hooks(
            operation_name="chat.delete",
            context=context,
            hooks_chain=(runtime.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )


class ChatsCollection(_MutableRemoteCollectionFacade[AsyncChat, Chat]):
    def __init__(self, owner: Provider) -> None:
        super().__init__(owner, owner._provider.chats)

    def _wrap(self, item: AsyncChat) -> Chat:
        return self._owner._wrap_chat(item)

    def list(
        self,
        predicate: Callable[[Chat], bool] | None = None,
        *,
        limit: int | None = 50,
        include_projects: bool = False,
    ) -> list[Chat]:
        async_pred = self._wrap_predicate(predicate)
        return [
            self._wrap(item)
            for item in self._owner._call(
                self._collection.list(async_pred, limit=limit, include_projects=include_projects)
            )
        ]

    def refresh(self, *, limit: int | None = None, include_projects: bool = False) -> list[Chat]:
        return [
            self._wrap(item)
            for item in self._owner._call(
                self._collection.refresh(limit=limit, include_projects=include_projects)
            )
        ]

    def get(
        self, item_id: str, /, *, include_projects: bool = False, metadata_only: bool = False
    ) -> Chat:
        return self._wrap(
            self._owner._call(
                self._collection.get(
                    item_id, include_projects=include_projects, metadata_only=metadata_only
                )
            )
        )

    def get_with_responses(
        self, item_id: str, /, *, limit: int | None = 50, include_projects: bool = False
    ) -> tuple[Chat, list[Response]]:
        resource, responses = self._owner._call(
            self._collection.get_with_responses(
                item_id, limit=limit, include_projects=include_projects
            )
        )
        chat = self._wrap(resource)
        return chat, [chat.responses._wrap(item, chat_view=chat) for item in responses]

    def first(
        self,
        predicate: Callable[[Chat], bool] | None = None,
        /,
        *,
        include_projects: bool = False,
    ) -> Chat | None:
        async_pred = self._wrap_predicate(predicate)
        return self._wrap_optional(
            self._owner._call(self._collection.first(async_pred, include_projects=include_projects))
        )

    def last(
        self,
        predicate: Callable[[Chat], bool] | None = None,
        /,
        *,
        include_projects: bool = False,
    ) -> Chat | None:
        async_pred = self._wrap_predicate(predicate)
        return self._wrap_optional(
            self._owner._call(self._collection.last(async_pred, include_projects=include_projects))
        )

    def single(
        self,
        predicate: Callable[[Chat], bool] | None = None,
        /,
        *,
        include_projects: bool = False,
    ) -> Chat | None:
        async_pred = self._wrap_predicate(predicate)
        return self._wrap_optional(
            self._owner._call(
                self._collection.single(async_pred, include_projects=include_projects)
            )
        )

    def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        single: bool = False,
        last: bool = False,
        include_projects: bool = False,
    ) -> Chat | None:
        return self._wrap_optional(
            self._owner._call(
                self._collection.find(
                    query,
                    exact=exact,
                    startswith=startswith,
                    endswith=endswith,
                    single=single,
                    last=last,
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
        limit: int | None = None,
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
                    limit=limit,
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
