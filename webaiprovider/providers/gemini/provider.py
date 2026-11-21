from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from ...accounts import delete_session, read_session, session_exists
from ...accounts.status import SessionState, SessionStatus
from ...base import Capability, ProviderType
from ...collections import Page
from ...core.cache import BoundedCache
from ...core.timing import current_send_timing
from ...errors import AccountPaused, FeatureUnavailable
from ...provider import Provider
from ...resources.chat.collections import ChatsCollection
from ...resources.chat.types import Conversation
from ...resources.message.protocol import ProviderRequest, ProviderStreamEvent
from ...resources.message.types import target_message
from ...resources.model import ModelCatalogState
from ...resources.model.model import Model, ModelCatalog
from ...resources.project.collections import ProjectsCollection
from ...resources.project.mutations import ProjectMutationsMixin
from ...resources.project.types import ProjectData
from ...transport import Transport
from .base import _CHAT_METADATA_TTL_SECONDS, ORIGIN, BaseGeminiProvider
from .client import GeminiClient, GeminiProjectEditor
from .errors import GeminiStreamError
from .generation import _gemini_upload_spec, iter_response_events
from .history import parse_chat_page
from .notebooks import GeminiNotebooks, notebook_id
from .resources import (
    GeminiArtifact,
    GeminiAttachment,
    GeminiChat,
    GeminiMessage,
    GeminiProject,
    GeminiResponse,
    GeminiSource,
)
from .rpc import GeminiRPC

if TYPE_CHECKING:
    from ...accounts.account import Account


class GeminiProvider(
    ProjectMutationsMixin,
    Provider[ChatsCollection[GeminiChat], ProjectsCollection[GeminiProject]],
    BaseGeminiProvider,
):
    _message_type = GeminiMessage
    _response_type = GeminiResponse
    _source_type = GeminiSource
    _artifact_type = GeminiArtifact
    _attachment_type = GeminiAttachment
    _chat_followup_page_size = 100

    def _make_chats(self) -> ChatsCollection[GeminiChat]:
        return ChatsCollection(self, GeminiChat)

    def _make_projects(self) -> ProjectsCollection[GeminiProject]:
        return ProjectsCollection(self, GeminiProject)

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        client_factory: Any = None,
        notebooks: GeminiNotebooks | None = None,
        transport: Transport | None = None,
    ) -> None:
        if transport is not None and transport.origin != ORIGIN:
            raise ValueError("Gemini transport must use the Gemini origin")
        self._client_factory = client_factory
        self._provided_transport = transport
        self._connected_transport: Transport | None = transport
        super().__init__(account, type=type, transport=transport)
        self._media: dict[tuple[str | None, str], dict[str, Any]] = {}
        self.cookie_file = account.store()
        self._client: GeminiClient | None = None
        self._cached_models: tuple[Model, ...] | None = None
        self._catalog_at: float | None = None
        self._model_gate = threading.Lock()
        self._gate = threading.Lock()
        self._connect_gate = threading.Lock()
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
        self.rpc: GeminiRPC = GeminiRPC(self._connect)
        self.notebooks: GeminiNotebooks = notebooks or GeminiNotebooks(account.id)
        self.notebooks.reader = self.rpc.read
        self.editor: GeminiProjectEditor = GeminiProjectEditor(self.rpc, self.notebooks)

    def _connect(self) -> GeminiClient:
        with self._connect_gate:
            return self._connect_once()

    def _connect_once(self) -> GeminiClient:
        if self._client is not None:
            return self._client
        if not session_exists(self.cookie_file):
            raise AccountPaused("No Gemini session; sign in through the Accounts section")
        client = (
            GeminiClient(self.cookie_file, transport=self._provided_transport)
            if self._client_factory is None
            else self._client_factory(self.cookie_file)
        )
        client.open()
        if (
            self._cached_models is not None
            and self._catalog_at is not None
            and (time.monotonic() - self._catalog_at < 300)
        ):
            client.models = self._cached_models
        self._client = client
        self._connected_transport = self._provided_transport or getattr(client, "transport", None)
        return client

    def _close_unlocked(self) -> None:
        client = self._client
        if client is not None:
            client.close()
        if self._client is client:
            self._client = None
            if self._provided_transport is None:
                self._connected_transport = None
        if self._client is None:
            self._chat_metadata.clear()
            self._project_contexts.clear()

    def _prepare_session(self) -> None:
        self._prime_models(self._load_catalog())

    def _close_session(self) -> None:
        with self._gate, self._connect_gate:
            self._close_unlocked()

    def _authenticate(self) -> SessionStatus:
        self._connect()
        return SessionStatus(SessionState.AUTHENTICATED, True)

    def _forget_session(self) -> SessionStatus:
        with self._gate, self._connect_gate:
            self._close_unlocked()
            delete_session(self.cookie_file)
            self._temporary.clear()
            self._cached_models = None
            self._catalog_at = None
        return SessionStatus(SessionState.LOGIN_REQUIRED)

    def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
        with self._model_gate:
            client = self._connect()
            if (
                force
                or not client.models
                or self._catalog_at is None
                or (time.monotonic() - self._catalog_at >= 300)
            ):
                client.models = client.discover_models()
                self._cached_models = client.models
                self._catalog_at = time.monotonic()
            return ModelCatalog(ModelCatalogState.AVAILABLE, client.models)

    def _list_chats_data(self, *, cursor: str | None = None, limit: int = 50) -> Page[Conversation]:
        offset = self._parse_list_chats_args(cursor, limit)
        parts = self.rpc.call(
            "MaZiqc", [offset + limit + 1, None, [None, None, 1, None, 1]], "/app"
        )
        chats = parse_chat_page(parts, offset=offset, limit=limit + 1)
        return Page(chats[:limit], str(offset + limit) if len(chats) > limit else None)

    def _get_chat_data(self, conversation_id: str) -> Conversation:
        parts = self.rpc.call("hNvQHb", [conversation_id, 1000, None, 1, [1], [4], None, 1], "/app")
        return self._build_chat_from_history(conversation_id, parts)

    def _branch_chat_data(self, conversation_id: str, message_id: str) -> Conversation:
        current = self._get_chat_data(conversation_id)
        _, target = target_message(current.messages, message_id, "assistant")
        result = self.rpc.call("KDNZr", [conversation_id, target.metadata["turn_id"]], "/app")
        remote, title = self._parse_branch_result(result, conversation_id)
        return self._apply_branch_title(self._get_chat_data(remote), title)

    def _delete_chat_data(self, conversation_id: str) -> None:
        self._connect()
        self.rpc.call("GzXR5e", [conversation_id], "/app", allow_empty=True)
        self._chat_metadata.remove(conversation_id)

    def _rename_chat_data(self, conversation_id: str, title: str) -> None:
        cleaned = self._validate_chat_title(title)
        self._connect().rename_conversation(conversation_id, cleaned)

    def _list_projects_data(self, *, cursor: str | None = None) -> Page[ProjectData]:
        if self.notebooks is None:
            raise FeatureUnavailable(Capability.PROJECTS)
        return self.notebooks.list(cursor)

    def _get_project_data(self, project_id: str) -> ProjectData:
        if self.notebooks is None:
            raise FeatureUnavailable(Capability.PROJECTS)
        return self.notebooks.get(project_id)

    def _list_project_chats_data(
        self, project_id: str, *, cursor: str | None = None, limit: int | None = None
    ) -> Page[Conversation]:
        del limit
        if self.notebooks is None:
            raise FeatureUnavailable(Capability.PROJECTS)
        return self.notebooks.chats(project_id, cursor)

    def _create_project_data(
        self, name: str, instructions: str = "", **options: Any
    ) -> ProjectData:
        del options
        if self.editor is None:
            raise FeatureUnavailable(Capability.PROJECT_CREATE)
        remote = self.editor.create(name=self._validate_create_project(name, instructions))
        return self._get_project_data(remote)

    def _delete_project_data(self, project_id: str) -> None:
        remote = notebook_id(project_id)
        self.editor.delete(remote)
        self._project_contexts.remove(remote)

    def _stream_provider_events(self, request: ProviderRequest) -> Iterator[ProviderStreamEvent]:
        deadline = time.monotonic() + self.stream_total_timeout
        iterator = iter_response_events(
            request,
            prepare=self._prepare_send_context,
            request_timeout=(15, self.stream_idle_timeout + 10),
            media=self._media,
            remember=self._remember_response,
        )
        try:
            for event in iterator:
                if time.monotonic() > deadline:
                    raise GeminiStreamError("Gemini request exceeded the 10-minute time limit")
                yield event
        finally:
            close = getattr(iterator, "close", None)
            if callable(close):
                close()

    def _prepare_send_context(self, request: ProviderRequest):
        timing = current_send_timing()
        client_started = time.perf_counter_ns() if timing is not None else None
        client = self._connect()
        if client_started is not None:
            timing.record("provider_client_ready_ms", time.perf_counter_ns() - client_started)
        if timing is not None:
            timing.record("provider_send_gate_wait_ms", 0)
        catalog_started = time.perf_counter_ns() if timing is not None else None
        if not client.models:
            self._load_catalog()
        if catalog_started is not None:
            timing.record("model_catalog_ms", time.perf_counter_ns() - catalog_started)
        context = self._resolve_project_context(request.project_id, timing)
        request, metadata, action_files = self._load_chat_continuation(request, timing)
        activity_started = time.perf_counter_ns() if timing is not None else None
        client.sync_activity()
        if activity_started is not None:
            timing.record("provider_activity_sync_ms", time.perf_counter_ns() - activity_started)
        return client, request, metadata, context, action_files

    def _resolve_project_context(self, project_id: str | None, timing: Any):
        project = notebook_id(project_id) if project_id else None
        if not project:
            return None
        cached = self._project_contexts.get(project)
        if cached is not None:
            if timing is not None:
                timing.record("pre_send_project_context_ms", 0)
            return project, list(cached)
        project_started = time.perf_counter_ns() if timing is not None else None
        notebook = self.notebooks.get(project)
        if project_started is not None:
            timing.record("pre_send_project_context_ms", time.perf_counter_ns() - project_started)
        return self._cache_notebook_context(project, notebook)

    def _load_chat_continuation(self, request: ProviderRequest, timing: Any):
        done, metadata, expected_turn, recent_action = self._prepare_continuation_lookup(
            request, timing
        )
        if done:
            return request, metadata, None

        def read_history(limit: int):
            return self.rpc.call(
                "hNvQHb", [request.conversation_id, limit, None, 1, [1], [4], None, 1], "/app"
            )

        history_started = time.perf_counter_ns() if timing is not None else None
        try:
            limit = 1000 if request.action else 1
            parts = read_history(2 if recent_action else limit)
            if recent_action and self._needs_full_continuation_history(parts, expected_turn):
                parts = read_history(limit)
        finally:
            if history_started is not None:
                timing.record(
                    "pre_send_get_conversation_ms", time.perf_counter_ns() - history_started
                )
        return self._apply_continuation_parts(request, metadata, parts)

    def _upload_file(self, path: Any) -> dict[str, Any]:
        filename, mime, size, _content, local_path = _gemini_upload_spec(path)
        return self._deferred_upload_record(filename, mime, size, local_path)

    def _resolve_download(
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
            target = self._extract_media_target_from_chat(self._get_chat_data(conversation_id), url)
        cookie_values = None
        if self._download_needs_cookies(target, url):
            client = self._client
            cookie_values = (
                client.http.cookies.get_dict()
                if client is not None and client.http is not None
                else read_session(self.cookie_file)
            )
        return self._finalize_download_source(target, url, cookie_values)
