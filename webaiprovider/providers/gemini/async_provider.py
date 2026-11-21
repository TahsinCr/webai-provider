from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator, AsyncIterator
from typing import TYPE_CHECKING, Any

from ...accounts import delete_session, session_exists
from ...accounts.credentials import read_session_async
from ...accounts.status import SessionState, SessionStatus
from ...async_provider import AsyncProvider
from ...base import Capability, ProviderType
from ...collections import Page
from ...core.cache import BoundedCache
from ...core.timing import current_send_timing
from ...errors import AccountPaused, FeatureUnavailable
from ...resources.chat.collections import AsyncChatsCollection
from ...resources.chat.types import Conversation
from ...resources.message.protocol import ProviderRequest, ProviderStreamEvent
from ...resources.message.types import target_message
from ...resources.model import ModelCatalogState
from ...resources.model.model import Model, ModelCatalog
from ...resources.project.collections import AsyncProjectsCollection
from ...resources.project.mutations import AsyncProjectMutationsMixin
from ...resources.project.types import ProjectData
from ...transport import AsyncTransport
from .async_client import (
    AsyncGeminiClient,
    AsyncGeminiNotebooks,
    AsyncGeminiProjectEditor,
    AsyncGeminiRPC,
    response_events,
)
from .base import _CHAT_METADATA_TTL_SECONDS, ORIGIN, BaseGeminiProvider
from .errors import GeminiCompatibilityError, GeminiStreamError
from .generation import _gemini_upload_spec
from .history import parse_chat_history, parse_chat_page
from .notebooks import notebook_id
from .resources import (
    AsyncGeminiChat,
    AsyncGeminiMessage,
    AsyncGeminiProject,
    GeminiArtifact,
    GeminiAttachment,
    GeminiResponse,
    GeminiSource,
)

if TYPE_CHECKING:
    from ...accounts.account import Account


class AsyncGeminiProvider(
    AsyncProjectMutationsMixin,
    AsyncProvider[
        AsyncChatsCollection[AsyncGeminiChat], AsyncProjectsCollection[AsyncGeminiProject]
    ],
    BaseGeminiProvider,
):
    _message_type = AsyncGeminiMessage
    _response_type = GeminiResponse
    _source_type = GeminiSource
    _artifact_type = GeminiArtifact
    _attachment_type = GeminiAttachment
    _chat_followup_page_size = 100

    def _make_chats(self) -> AsyncChatsCollection[AsyncGeminiChat]:
        return AsyncChatsCollection(self, AsyncGeminiChat)

    def _make_projects(self) -> AsyncProjectsCollection[AsyncGeminiProject]:
        return AsyncProjectsCollection(self, AsyncGeminiProject)

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        client_factory: Any = None,
        notebooks: AsyncGeminiNotebooks | None = None,
        transport: AsyncTransport | None = None,
    ) -> None:
        if transport is not None and transport.origin != ORIGIN:
            raise ValueError("Gemini transport must use the Gemini origin")
        self._client_factory = client_factory
        self._provided_transport = transport
        self._connected_transport: AsyncTransport | None = transport
        super().__init__(account, type=type, transport=transport)
        self._media: dict[tuple[str | None, str], dict[str, Any]] = {}
        self.cookie_file = account.store()
        self._client: AsyncGeminiClient | None = None
        self._cached_models: tuple[Model, ...] | None = None
        self._catalog_at: float | None = None
        self._model_gate = asyncio.Lock()
        self._gate = asyncio.Lock()
        self._connect_gate = asyncio.Lock()
        self._temporary: dict[str, list[Any]] = {}
        self._chat_metadata = BoundedCache(
            max_entries=128,
            ttl=_CHAT_METADATA_TTL_SECONDS,
            clock=lambda: time.monotonic(),
        )
        self._project_contexts: BoundedCache[str, list[Any]] = BoundedCache(
            max_entries=32,
            max_bytes=32768,
            ttl=120.0,
            clock=lambda: time.monotonic(),
        )
        self.rpc: AsyncGeminiRPC = AsyncGeminiRPC(self._connect)
        self.notebooks: AsyncGeminiNotebooks = notebooks or AsyncGeminiNotebooks(account.id)
        self.notebooks.reader = self.rpc.read
        self.editor: AsyncGeminiProjectEditor = AsyncGeminiProjectEditor(self.rpc, self.notebooks)

    async def _connect(self) -> AsyncGeminiClient:
        async with self._connect_gate:
            return await self._connect_once()

    async def _connect_once(self) -> AsyncGeminiClient:
        if self._client is not None:
            return self._client
        if not session_exists(self.cookie_file):
            raise AccountPaused("No Gemini session; sign in through the Accounts section")
        client = (
            AsyncGeminiClient(self.cookie_file, transport=self._provided_transport)
            if self._client_factory is None
            else self._client_factory(self.cookie_file)
        )
        await client.open()
        if (
            self._cached_models is not None
            and self._catalog_at is not None
            and (time.monotonic() - self._catalog_at < 300)
        ):
            client.models = self._cached_models
        self._client = client
        self._connected_transport = self._provided_transport or getattr(client, "transport", None)
        return client

    async def _close_unlocked(self) -> None:
        client = self._client
        if client is not None:
            await client.close()
        if self._client is client:
            self._client = None
            if self._provided_transport is None:
                self._connected_transport = None
        if self._client is None:
            self._chat_metadata.clear()
            self._project_contexts.clear()

    async def _prepare_session(self) -> None:
        self._prime_models(await self._load_catalog())

    async def _close_session(self) -> None:
        async with self._gate, self._connect_gate:
            await self._close_unlocked()

    async def _authenticate(self) -> SessionStatus:
        await self._connect()
        return SessionStatus(SessionState.AUTHENTICATED, True)

    async def _forget_session(self) -> SessionStatus:
        async with self._gate, self._connect_gate:
            await self._close_unlocked()
            delete_session(self.cookie_file)
            self._temporary.clear()
            self._cached_models = None
            self._catalog_at = None
        return SessionStatus(SessionState.LOGIN_REQUIRED)

    async def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
        async with self._model_gate:
            client = await self._connect()
            if (
                force
                or not client.models
                or self._catalog_at is None
                or (time.monotonic() - self._catalog_at >= 300)
            ):
                client.models = await client.discover_models()
                self._cached_models = client.models
                self._catalog_at = time.monotonic()
            return ModelCatalog(ModelCatalogState.AVAILABLE, client.models)

    async def _list_chats_data(
        self, *, cursor: str | None = None, limit: int = 50
    ) -> Page[Conversation]:
        offset = self._parse_list_chats_args(cursor, limit)
        parts = await self.rpc.call(
            "MaZiqc", [offset + limit + 1, None, [None, None, 1, None, 1]], "/app"
        )
        chats = parse_chat_page(parts, offset=offset, limit=limit + 1)
        return Page(chats[:limit], str(offset + limit) if len(chats) > limit else None)

    async def _get_chat_data(self, conversation_id: str) -> Conversation:
        parts = await self.rpc.call(
            "hNvQHb", [conversation_id, 1000, None, 1, [1], [4], None, 1], "/app"
        )
        if len(parts) != 1:
            raise GeminiCompatibilityError("Gemini history response format changed")
        chat = await asyncio.to_thread(parse_chat_history, parts[0], conversation_id)
        metadata = self._extract_history_metadata(parts[0], conversation_id)
        if conversation_id not in self._temporary and self._usable_chat_metadata(
            metadata, conversation_id
        ):
            self._remember_chat_metadata(conversation_id, metadata)
        return chat

    async def _branch_chat_data(self, conversation_id: str, message_id: str) -> Conversation:
        current = await self._get_chat_data(conversation_id)
        _, target = target_message(current.messages, message_id, "assistant")
        result = await self.rpc.call("KDNZr", [conversation_id, target.metadata["turn_id"]], "/app")
        remote, title = self._parse_branch_result(result, conversation_id)
        return self._apply_branch_title(await self._get_chat_data(remote), title)

    async def _delete_chat_data(self, conversation_id: str) -> None:
        await self._connect()
        await self.rpc.call("GzXR5e", [conversation_id], "/app", allow_empty=True)
        self._chat_metadata.remove(conversation_id)

    async def _rename_chat_data(self, conversation_id: str, title: str) -> None:
        cleaned = self._validate_chat_title(title)
        client = await self._connect()
        await client.rename_conversation(conversation_id, cleaned)

    async def _list_projects_data(self, *, cursor: str | None = None) -> Page[ProjectData]:
        if self.notebooks is None:
            raise FeatureUnavailable(Capability.PROJECTS)
        return await self.notebooks.list(cursor)

    async def _get_project_data(self, project_id: str) -> ProjectData:
        if self.notebooks is None:
            raise FeatureUnavailable(Capability.PROJECTS)
        return await self.notebooks.get(project_id)

    async def _list_project_chats_data(
        self, project_id: str, *, cursor: str | None = None, limit: int | None = None
    ) -> Page[Conversation]:
        del limit
        if self.notebooks is None:
            raise FeatureUnavailable(Capability.PROJECTS)
        return await self.notebooks.chats(project_id, cursor)

    async def _create_project_data(
        self, name: str, instructions: str = "", **options: Any
    ) -> ProjectData:
        del options
        if self.editor is None:
            raise FeatureUnavailable(Capability.PROJECT_CREATE)
        remote = await self.editor.create(name=self._validate_create_project(name, instructions))
        return await self._get_project_data(remote)

    async def _delete_project_data(self, project_id: str) -> None:
        remote = notebook_id(project_id)
        await self.editor.delete(remote)
        self._project_contexts.remove(remote)

    async def _stream_provider_events(
        self, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamEvent]:
        iterator = self._stream(request)
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("Gemini streaming requires an active asyncio task")
        deadline_cancel = object()
        timeout = loop.call_later(self.stream_total_timeout, lambda: task.cancel(deadline_cancel))
        try:
            async for event in iterator:
                yield event
        except asyncio.CancelledError as error:
            if not error.args or error.args[0] is not deadline_cancel:
                raise
            raise GeminiStreamError("Gemini request exceeded the 10-minute time limit") from None
        finally:
            timeout.cancel()
            await iterator.aclose()

    def _stream(self, request: ProviderRequest) -> AsyncGenerator[ProviderStreamEvent, None]:
        return response_events(
            request,
            prepare=self._prepare_send_context,
            request_timeout=(15, self.stream_idle_timeout + 10),
            media=self._media,
            remember=self._remember_response,
        )

    async def _prepare_send_context(self, request: ProviderRequest):
        timing = current_send_timing()
        client_started = time.perf_counter_ns() if timing is not None else None
        client = await self._connect()
        if client_started is not None:
            timing.record("provider_client_ready_ms", time.perf_counter_ns() - client_started)
        if timing is not None:
            timing.record("provider_send_gate_wait_ms", 0)

        async def ensure_models() -> None:
            catalog_started = time.perf_counter_ns() if timing is not None else None
            if not client.models:
                await self._load_catalog()
            if catalog_started is not None:
                timing.record("model_catalog_ms", time.perf_counter_ns() - catalog_started)

        async def sync_client_activity() -> None:
            activity_started = time.perf_counter_ns() if timing is not None else None
            await client.sync_activity()
            if activity_started is not None:
                timing.record(
                    "provider_activity_sync_ms", time.perf_counter_ns() - activity_started
                )

        _, context, continuation, _ = await asyncio.gather(
            ensure_models(),
            self._resolve_project_context(request.project_id, timing),
            self._load_chat_continuation(request, timing),
            sync_client_activity(),
        )
        request, metadata, action_files = continuation
        return client, request, metadata, context, action_files

    async def _resolve_project_context(self, project_id: str | None, timing: Any):
        project = notebook_id(project_id) if project_id else None
        if not project:
            return None
        cached = self._project_contexts.get(project)
        if cached is not None:
            if timing is not None:
                timing.record("pre_send_project_context_ms", 0)
            return project, list(cached)
        project_started = time.perf_counter_ns() if timing is not None else None
        notebook = await self.notebooks.get(project)
        if project_started is not None:
            timing.record("pre_send_project_context_ms", time.perf_counter_ns() - project_started)
        return self._cache_notebook_context(project, notebook)

    async def _load_chat_continuation(self, request: ProviderRequest, timing: Any):
        done, metadata, expected_turn, recent_action = self._prepare_continuation_lookup(
            request, timing
        )
        if done:
            return request, metadata, None

        async def read_history(limit: int):
            return await self.rpc.call(
                "hNvQHb", [request.conversation_id, limit, None, 1, [1], [4], None, 1], "/app"
            )

        history_started = time.perf_counter_ns() if timing is not None else None
        try:
            limit = 1000 if request.action else 1
            parts = await read_history(2 if recent_action else limit)
            if recent_action and self._needs_full_continuation_history(parts, expected_turn):
                parts = await read_history(limit)
        finally:
            if history_started is not None:
                timing.record(
                    "pre_send_get_conversation_ms", time.perf_counter_ns() - history_started
                )
        return self._apply_continuation_parts(request, metadata, parts)

    async def _upload_file(self, path: Any) -> dict[str, Any]:
        filename, mime, size, _content, local_path = await asyncio.to_thread(
            _gemini_upload_spec, path
        )
        return self._deferred_upload_record(filename, mime, size, local_path)

    async def _resolve_download(
        self,
        url: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
    ) -> Any | None:
        del message_id
        stable_reference, supported = self._is_stable_media_reference(url)
        if not supported:
            return None
        target = self._media.get((conversation_id, url))
        if stable_reference and conversation_id and conversation_id not in self._temporary:
            target = self._extract_media_target_from_chat(
                await self._get_chat_data(conversation_id), url
            )
        cookie_values = None
        if self._download_needs_cookies(target, url):
            client = self._client
            cookie_values = (
                client.http.cookies.get_dict()
                if client is not None and client.http is not None
                else await read_session_async(self.cookie_file)
            )
        return self._finalize_download_source(target, url, cookie_values)
