from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from ...base import Capability
from ...collections import (
    AsyncRemoteCollection,
    Page,
    RemoteCollection,
    _async_publish_collection_change,
    _async_report_read,
    _publish_collection_change,
    _report_read,
    validate_limit,
)
from ...core.async_hooks import AsyncOperationHooks
from ...core.concurrency import gather_bounded
from ...core.hooks import OperationHooks
from ...errors import FeatureUnavailable, ProviderError, RemoteNotFound
from ..operations import async_execute_with_hooks, execute_with_hooks
from ..project.project import AsyncProject, Project
from .chat import AsyncChat, Chat, _project_from_data, _resolve_project_arg
from .types import DEFAULT_CHAT_TITLE, Conversation

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider
    from ..response.response import Response

_ChatT = TypeVar("_ChatT", bound=Chat, covariant=True)
_AsyncChatT = TypeVar("_AsyncChatT", bound=AsyncChat, covariant=True)

_PROJECT_CHAT_CONCURRENCY = 4
_MAX_PROJECT_CHAT_PAGES = 500


def _require_conversation_page(page: Any, *, label: str = "conversation") -> Page[Conversation]:
    if not isinstance(page, Page) or any(not isinstance(item, Conversation) for item in page.items):
        raise ProviderError(f"Provider returned an invalid {label} page")
    return page


def _merge_chat_lists(
    normal_chats: list[_AsyncChatT],
    project_chats: list[_AsyncChatT],
) -> list[_AsyncChatT]:
    by_key = {chat.remote_id or chat.id: chat for chat in normal_chats}
    for chat in project_chats:
        key = chat.remote_id or chat.id
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = chat
        elif existing.project is None and chat.project is not None:
            existing.project = chat.project
            existing.title = chat.title
            existing.temporary = chat.temporary
            existing.metadata.update(chat.metadata)
    return list(by_key.values())


class _ProjectChatPageState:
    """Shared per-project chat pagination, deduplication, and cursor-guard state."""

    def __init__(
        self,
        collection: _BaseChatsCollection,
        project: Any,
        project_map: Mapping[str, Any],
        per_project_limit: int | None,
    ) -> None:
        self.collection = collection
        self.project = project
        self.project_map = project_map
        self.per_project_limit = per_project_limit
        self.chats: list[Any] = []
        self.seen_ids: set[str] = set()
        self.seen_cursors: set[str] = set()
        self.cursor: str | None = None

    def remaining(self) -> int | None:
        return None if self.per_project_limit is None else self.per_project_limit - len(self.chats)

    def consume(self, raw_page: Any) -> bool:
        page = _require_conversation_page(raw_page, label="project conversation")
        for item in page.items:
            if item.id in self.seen_ids:
                continue
            self.seen_ids.add(item.id)
            chat = self.collection._from_data(item, self.project_map)
            if chat.project is None:
                chat.project = self.project
            self.chats.append(chat)
            if self.per_project_limit is not None and len(self.chats) >= self.per_project_limit:
                return True
        next_cursor = page.next_cursor
        if next_cursor is None:
            return True
        if not isinstance(next_cursor, str) or not next_cursor or next_cursor in self.seen_cursors:
            raise ProviderError(
                "Provider project chat pagination returned an invalid or repeating cursor"
            )
        self.seen_cursors.add(next_cursor)
        self.cursor = next_cursor
        return False


class _BaseChatsCollection:
    """Shared chat construction, pagination math, and validation for sync/async collections."""

    _collection_name = "chats"
    provider: Any
    _build: Callable[..., Any]
    _default_page_size: int
    _followup_page_size: int | None

    @property
    def _read_provider(self):
        return self.provider

    def _init_chats_collection(self, provider: Any, build: Callable[..., Any]) -> None:
        self.provider = provider
        self._build = build
        self._default_page_size = provider._chat_page_size
        self._followup_page_size = provider._chat_followup_page_size

    def _new(self, **fields: Any) -> Any:
        return self._build(provider=self.provider, **fields)

    def _from_data(self, item: Conversation, projects: Mapping[str, Any] | None = None) -> Any:
        project = _project_from_data(self.provider, item.project, projects)
        return self._new(
            title=item.title,
            remote_id=item.id,
            project=project,
            temporary=bool(item.metadata.get("temporary", False)),
            metadata=item.metadata,
        )

    def _compute_page_limit(
        self, cursor: str | None, page_limit: int | None, *, include_projects: bool
    ) -> int:
        limit_arg = page_limit or self._default_page_size
        if not include_projects and limit_arg < 25:
            # Leave space for filtered project chats on the first read.
            # Only fetch a full page if results are still missing.
            limit_arg = (
                max(limit_arg, self.provider._chat_initial_page_floor)
                if cursor is None
                else self._default_page_size
            )
        return limit_arg

    def _build_chats_page(self, raw_page: Any, *, include_projects: bool) -> Page[Any]:
        page = _require_conversation_page(raw_page)
        chats = tuple(
            self._from_data(item) for item in page.items if include_projects or item.project is None
        )
        return Page(chats, page.next_cursor)

    def _should_collect_with_projects(self, options: Mapping[str, Any]) -> bool:
        return (
            bool(options.get("include_projects", False))
            and Capability.PROJECTS in self.provider.capabilities
        )

    def _finalize_get_with_responses(
        self,
        item_id: str,
        conversation: Any,
        *,
        validated: int | None,
        include_projects: bool,
    ) -> tuple[Any, list[Response]]:
        if not isinstance(conversation, Conversation):
            raise ProviderError("Provider returned an invalid conversation")
        if not include_projects and conversation.project is not None:
            raise RemoteNotFound(f"Record was not found: {item_id}")
        chat = self._from_data(conversation)
        if validated == 0:
            return chat, []
        responses = chat.responses._apply_history(conversation, self.provider, limit=validated)
        return chat, responses if validated is None else responses[-validated:]

    def _prepare_create_context(self, title: str, project: Any, temporary: bool) -> Any:
        resolved_project = (
            _resolve_project_arg(self.provider, project) if project is not None else None
        )
        return self.provider._new_context(
            "chat.create",
            parameters={"title": title, "project": resolved_project, "temporary": temporary},
        )

    def _build_created_chat(self, ctx: Any) -> Any:
        return self._new(
            title=ctx.parameters["title"],
            project=ctx.parameters["project"],
            temporary=ctx.parameters["temporary"],
        )

    def _check_owned_chat(self, chat: Any) -> Any:
        if chat.provider is not self.provider:
            raise ValueError("Chat belongs to a different provider session")
        return chat


class AsyncChatsCollection(_BaseChatsCollection, AsyncRemoteCollection[_AsyncChatT]):
    def __init__(
        self, provider: AsyncProvider[Any, Any], build: Callable[..., _AsyncChatT]
    ) -> None:
        self._init_chats_collection(provider, build)
        self.hooks = AsyncOperationHooks()

    async def _fetch_page(
        self,
        cursor: str | None,
        page_limit: int | None,
        **options: Any,
    ) -> Page[_AsyncChatT]:
        include_projects = bool(options.get("include_projects", False))
        session = self.provider
        async with session._activity():
            session._require_provider()
            limit_arg = self._compute_page_limit(
                cursor, page_limit, include_projects=include_projects
            )
            raw_page = await session._list_chats_data(cursor=cursor, limit=limit_arg)
            return self._build_chats_page(raw_page, include_projects=include_projects)

    async def _fetch_project_chats(
        self,
        projects: list[AsyncProject],
        slots: asyncio.Semaphore,
        *,
        per_project_limit: int | None = None,
    ) -> list[_AsyncChatT]:
        project_map = {p.remote_id: p for p in projects if p.remote_id}
        session = self.provider

        async def fetch_one(project: AsyncProject) -> list[_AsyncChatT]:
            async with slots:
                assert project.remote_id is not None
                state = _ProjectChatPageState(self, project, project_map, per_project_limit)
                for _ in range(_MAX_PROJECT_CHAT_PAGES):
                    raw_page = await session._list_project_chats_data(
                        project.remote_id, cursor=state.cursor, limit=state.remaining()
                    )
                    if state.consume(raw_page):
                        return state.chats
                raise ProviderError(
                    f"Provider project chat pagination exceeded {_MAX_PROJECT_CHAT_PAGES} pages"
                )

        batches = await gather_bounded(
            tuple(project_map.values()), fetch_one, limit=_PROJECT_CHAT_CONCURRENCY
        )
        return [chat for batch in batches for chat in batch]

    async def _collect_with_projects(self, *, limit: int | None) -> list[_AsyncChatT]:
        validated = validate_limit(limit)
        if validated == 0:
            return []
        session = self.provider
        slots = asyncio.Semaphore(_PROJECT_CHAT_CONCURRENCY)
        collect_normal = super()._collect_items

        async def normal_chats():
            async with slots:
                return await collect_normal(limit=validated)

        async def project_chats():
            project_limit = None if validated is None else max(validated, _PROJECT_CHAT_CONCURRENCY)
            projects = await session.projects._collect_items(limit=project_limit)
            return await self._fetch_project_chats(projects, slots, per_project_limit=validated)

        async with session._activity():
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

    async def _iter_batches(self, *, limit: int | None = None, **options: Any):
        if self._should_collect_with_projects(options):
            items = await self._collect_with_projects(limit=limit)
            if items:
                yield items
            return
        async for batch in super()._iter_batches(limit=limit, **options):
            yield batch

    async def _fetch_conversation(
        self, item_id: str, *, metadata_only: bool = False
    ) -> Conversation:
        session = self.provider
        async with session._activity():
            session._require_provider()
            load = session._get_chat_metadata if metadata_only else session._get_chat_data
            self._mark_remote_read()
            return await load(item_id)

    async def _fetch_by_id(
        self, item_id: str, *, metadata_only: bool = False
    ) -> _AsyncChatT | None:
        try:
            conv = await self._fetch_conversation(item_id, metadata_only=metadata_only)
        except FeatureUnavailable:
            return None
        if not isinstance(conv, Conversation):
            return None
        return self._from_data(conv)

    @_async_report_read
    async def get(
        self, item_id: str, /, *, include_projects: bool = False, metadata_only: bool = False
    ) -> _AsyncChatT:
        """Get a chat, preferring a metadata-only lookup when requested and supported."""
        direct = await self._fetch_by_id(item_id, metadata_only=metadata_only)
        if direct is not None:
            if not include_projects and direct.project is not None:
                raise RemoteNotFound(f"Record was not found: {item_id}")
            return direct
        return await self._find_id(item_id, include_projects=include_projects)

    @_async_report_read
    async def get_with_responses(
        self, item_id: str, /, *, limit: int | None = 50, include_projects: bool = False
    ) -> tuple[_AsyncChatT, list[Response]]:
        """Read a chat and its latest responses from the same remote history snapshot."""
        validated = validate_limit(limit)
        conversation = await self._fetch_conversation(item_id, metadata_only=validated == 0)
        return self._finalize_get_with_responses(
            item_id, conversation, validated=validated, include_projects=include_projects
        )

    async def create(
        self,
        title: str = DEFAULT_CHAT_TITLE,
        *,
        project: AsyncProject | str | None = None,
        temporary: bool = False,
    ) -> _AsyncChatT:
        provider = self.provider
        context = self._prepare_create_context(title, project, temporary)

        async def work(ctx) -> _AsyncChatT:
            return self._build_created_chat(ctx)

        async def on_success(ctx) -> None:
            chat: AsyncChat = ctx.result
            await provider._emit(
                "chat.prepared",
                operation_id=ctx.operation_id,
                resource_id=chat.id,
                payload={"title": chat.title},
            )

        return await async_execute_with_hooks(
            operation_name="chat.create",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )

    async def update(
        self,
        chat: AsyncChat | str,
        *,
        title: str | None = None,
    ) -> AsyncChat:
        target = (
            self._check_owned_chat(chat)
            if isinstance(chat, AsyncChat)
            else await self.get(str(chat), include_projects=True)
        )
        provider = self.provider
        context = provider._new_context(
            "chat.update",
            resource_id=target.remote_id or target.id,
            parameters={"target": target, "title": title},
        )

        async def work(ctx) -> AsyncChat:
            new_title = ctx.parameters["title"]
            if new_title is not None:
                lock = provider._send_lock(target.remote_id or target.id)
                async with provider._activity(send_lock_id=id(lock)), lock:
                    if new_title != target.title:
                        if target.remote_id:
                            provider._require_provider()
                            await provider._rename_chat_data(target.remote_id, new_title)
                            ctx.metadata["remote_rename"] = True
                        target.title = new_title
            return target

        async def on_success(ctx) -> None:
            if ctx.metadata.get("remote_rename"):
                await _async_publish_collection_change(
                    provider, "chats", "update", target, operation_id=ctx.operation_id
                )
            await provider._emit(
                "chat.updated",
                operation_id=ctx.operation_id,
                resource_id=target.remote_id or target.id,
                payload={"title": target.title},
            )

        return await async_execute_with_hooks(
            operation_name="chat.update",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )

    async def delete(self, chat: AsyncChat | str) -> None:
        target = (
            self._check_owned_chat(chat)
            if isinstance(chat, AsyncChat)
            else await self.get(str(chat), include_projects=True)
        )
        provider = self.provider
        rid = target.remote_id or target.id
        context = provider._new_context(
            "chat.delete",
            resource_id=rid,
            parameters={"target": target},
        )

        async def work(_ctx) -> None:
            lock = provider._send_lock(target.remote_id or target.id)
            async with provider._activity(send_lock_id=id(lock)), lock:
                if target.remote_id:
                    provider._require_provider()
                    await provider._delete_chat_data(target.remote_id)
                    _ctx.resource_id = target.remote_id

        async def on_success(ctx) -> None:
            if target.remote_id:
                await _async_publish_collection_change(
                    provider, "chats", "delete", target.remote_id, operation_id=ctx.operation_id
                )
            await provider._emit(
                "chat.deleted",
                operation_id=ctx.operation_id,
                resource_id=ctx.resource_id,
            )

        await async_execute_with_hooks(
            operation_name="chat.delete",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )


class ChatsCollection(_BaseChatsCollection, RemoteCollection[_ChatT]):
    def __init__(self, provider: Provider[Any, Any], build: Callable[..., _ChatT]) -> None:
        self._init_chats_collection(provider, build)
        self.hooks = OperationHooks()

    def _fetch_page(
        self,
        cursor: str | None,
        page_limit: int | None,
        **options: Any,
    ) -> Page[_ChatT]:
        include_projects = bool(options.get("include_projects", False))
        session = self.provider
        with session._activity():
            session._require_provider()
            limit_arg = self._compute_page_limit(
                cursor, page_limit, include_projects=include_projects
            )
            raw_page = session._list_chats_data(cursor=cursor, limit=limit_arg)
            return self._build_chats_page(raw_page, include_projects=include_projects)

    def _fetch_project_chats(
        self,
        projects: list[Project],
        *,
        per_project_limit: int | None = None,
    ) -> list[_ChatT]:
        project_map = {p.remote_id: p for p in projects if p.remote_id}
        session = self.provider
        all_chats: list[_ChatT] = []
        for project in project_map.values():
            assert project.remote_id is not None
            state = _ProjectChatPageState(self, project, project_map, per_project_limit)
            for _ in range(_MAX_PROJECT_CHAT_PAGES):
                raw_page = session._list_project_chats_data(
                    project.remote_id, cursor=state.cursor, limit=state.remaining()
                )
                if state.consume(raw_page):
                    break
            else:
                raise ProviderError(
                    f"Provider project chat pagination exceeded {_MAX_PROJECT_CHAT_PAGES} pages"
                )
            all_chats.extend(state.chats)
        return all_chats

    def _collect_with_projects(self, *, limit: int | None) -> list[_ChatT]:
        validated = validate_limit(limit)
        if validated == 0:
            return []
        session = self.provider
        with session._activity():
            session._require_provider()
            normal = super()._collect_items(limit=validated)
            project_limit = None if validated is None else max(validated, _PROJECT_CHAT_CONCURRENCY)
            projects = session.projects._collect_items(limit=project_limit)
            scoped = self._fetch_project_chats(projects, per_project_limit=validated)
            merged = _merge_chat_lists(normal, scoped)
            return merged if validated is None else merged[:validated]

    def _iter_batches(self, *, limit: int | None = None, **options: Any):
        if self._should_collect_with_projects(options):
            items = self._collect_with_projects(limit=limit)
            if items:
                yield items
            return
        yield from super()._iter_batches(limit=limit, **options)

    def _fetch_conversation(self, item_id: str, *, metadata_only: bool = False) -> Conversation:
        session = self.provider
        with session._activity():
            session._require_provider()
            load = session._get_chat_metadata if metadata_only else session._get_chat_data
            self._mark_remote_read()
            return load(item_id)

    def _fetch_by_id(self, item_id: str, *, metadata_only: bool = False) -> _ChatT | None:
        try:
            conv = self._fetch_conversation(item_id, metadata_only=metadata_only)
        except FeatureUnavailable:
            return None
        if not isinstance(conv, Conversation):
            return None
        return self._from_data(conv)

    @_report_read
    def get(
        self, item_id: str, /, *, include_projects: bool = False, metadata_only: bool = False
    ) -> _ChatT:
        direct = self._fetch_by_id(item_id, metadata_only=metadata_only)
        if direct is not None:
            if not include_projects and direct.project is not None:
                raise RemoteNotFound(f"Record was not found: {item_id}")
            return direct
        return self._find_id(item_id, include_projects=include_projects)

    @_report_read
    def get_with_responses(
        self, item_id: str, /, *, limit: int | None = 50, include_projects: bool = False
    ) -> tuple[_ChatT, list[Response]]:
        validated = validate_limit(limit)
        conversation = self._fetch_conversation(item_id, metadata_only=validated == 0)
        return self._finalize_get_with_responses(
            item_id, conversation, validated=validated, include_projects=include_projects
        )

    def create(
        self,
        title: str = DEFAULT_CHAT_TITLE,
        *,
        project: Project | str | None = None,
        temporary: bool = False,
    ) -> _ChatT:
        provider = self.provider
        context = self._prepare_create_context(title, project, temporary)

        def work(ctx) -> _ChatT:
            return self._build_created_chat(ctx)

        def on_success(ctx) -> None:
            chat: Chat = ctx.result
            provider._emit(
                "chat.prepared",
                operation_id=ctx.operation_id,
                resource_id=chat.id,
                payload={"title": chat.title},
            )

        return execute_with_hooks(
            operation_name="chat.create",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )

    def update(
        self,
        chat: Chat | str,
        *,
        title: str | None = None,
        **fields: Any,
    ) -> _ChatT:
        if fields:
            raise TypeError(f"Unsupported chat fields: {', '.join(fields)}")
        target = (
            self._check_owned_chat(chat)
            if isinstance(chat, Chat)
            else self.get(str(chat), include_projects=True)
        )
        provider = self.provider
        context = provider._new_context(
            "chat.update",
            resource_id=target.remote_id or target.id,
            parameters={"target": target, "title": title},
        )

        def work(ctx) -> Any:
            new_title = ctx.parameters["title"]
            if new_title is not None:
                lock = provider._send_lock(target.remote_id or target.id)
                with provider._activity(send_lock_id=id(lock)), lock:
                    if new_title != target.title:
                        if target.remote_id:
                            provider._require_provider()
                            provider._rename_chat_data(target.remote_id, new_title)
                            ctx.metadata["remote_rename"] = True
                        target.title = new_title
            return target

        def on_success(ctx) -> None:
            if ctx.metadata.get("remote_rename"):
                _publish_collection_change(
                    provider, "chats", "update", target, operation_id=ctx.operation_id
                )
            provider._emit(
                "chat.updated",
                operation_id=ctx.operation_id,
                resource_id=target.remote_id or target.id,
                payload={"title": target.title},
            )

        return execute_with_hooks(
            operation_name="chat.update",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )

    def delete(self, chat: Chat | str) -> None:
        target = (
            self._check_owned_chat(chat)
            if isinstance(chat, Chat)
            else self.get(str(chat), include_projects=True)
        )
        provider = self.provider
        rid = target.remote_id or target.id
        context = provider._new_context(
            "chat.delete",
            resource_id=rid,
            parameters={"target": target},
        )

        def work(_ctx) -> None:
            lock = provider._send_lock(target.remote_id or target.id)
            with provider._activity(send_lock_id=id(lock)), lock:
                if target.remote_id:
                    provider._require_provider()
                    provider._delete_chat_data(target.remote_id)
                    _ctx.resource_id = target.remote_id

        def on_success(ctx) -> None:
            if target.remote_id:
                _publish_collection_change(
                    provider, "chats", "delete", target.remote_id, operation_id=ctx.operation_id
                )
            provider._emit(
                "chat.deleted",
                operation_id=ctx.operation_id,
                resource_id=ctx.resource_id,
            )

        execute_with_hooks(
            operation_name="chat.delete",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )
