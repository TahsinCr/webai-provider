from __future__ import annotations

import mimetypes
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlsplit

from ...accounts import delete_session, read_session, session_exists
from ...accounts.filters import ProviderCookieFilter
from ...accounts.status import SessionState, SessionStatus
from ...base import Capability, ProviderType
from ...collections import Page
from ...core.timing import current_send_timing
from ...errors import FeatureUnavailable, ProviderError, RateLimited, RemoteNotFound
from ...provider import Provider
from ...resources.chat.collections import ChatsCollection
from ...resources.chat.types import Conversation
from ...resources.message.protocol import ProviderRequest, ProviderStreamEvent
from ...resources.message.types import target_message
from ...resources.model.model import ModelCatalog
from ...resources.project.mutations import SyncProjectMutationsMixin
from ...resources.project.types import ProjectData, ProjectOptions
from ...transport import HTTPTransport, Transport, TransportConfig
from ...transport.downloader import DownloadSource
from .auth import BrowserSession
from .base import BaseChatGPTClient, BaseChatGPTProvider
from .client import ChatGPTClient, ChatGPTProjectEditor, prepare_chatgpt_attachments_sync
from .collections import ChatGPTProjectsCollection
from .constants import DEFAULT_PAGE_SIZE
from .errors import ChatGPTAuthenticationError, ChatGPTError
from .formats import emit_post_send_events
from .resources import (
    ChatGPTArtifact,
    ChatGPTAttachment,
    ChatGPTChat,
    ChatGPTMessage,
    ChatGPTProject,
    ChatGPTResponse,
    ChatGPTSource,
)

if TYPE_CHECKING:
    from ...accounts.account import Account


def confirm_deleted_sync(read: Any, missing: Any = lambda value: False) -> None:
    """Poll a remote resource until removal is confirmed or raise ProviderError."""
    for delay in (0, 0.2, 0.5, 1):
        if delay:
            time.sleep(delay)
        try:
            value = read()
        except RemoteNotFound:
            return
        if missing(value):
            return
    raise ProviderError(
        "Deletion could not be confirmed by the server; the local record was preserved. Sync before retrying."
    )


class ChatGPTProvider(
    SyncProjectMutationsMixin,
    Provider[ChatsCollection[ChatGPTChat], ChatGPTProjectsCollection],
    BaseChatGPTProvider,
):
    _chat_page_size = DEFAULT_PAGE_SIZE
    _chat_initial_page_floor = 100
    _message_type = ChatGPTMessage
    _response_type = ChatGPTResponse
    _source_type = ChatGPTSource
    _artifact_type = ChatGPTArtifact
    _attachment_type = ChatGPTAttachment

    def _make_chats(self) -> ChatsCollection[ChatGPTChat]:
        return ChatsCollection(self, ChatGPTChat)

    def _make_projects(self) -> ChatGPTProjectsCollection:
        return ChatGPTProjectsCollection(self, ChatGPTProject)

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        session: BrowserSession | None = None,
        client_factory: Callable[..., Any] | None = None,
        transport: Transport | None = None,
    ) -> None:
        if transport is not None and transport.origin != "https://chatgpt.com":
            raise ValueError("ChatGPT transport must use the ChatGPT origin")
        self._provided_transport = transport
        self._owned_transport: HTTPTransport | None = None
        self.session_state = session or BrowserSession(account.store())
        resolved_factory = ChatGPTClient if client_factory is None else client_factory
        self._factory = resolved_factory
        self._persistent_client = resolved_factory is ChatGPTClient
        self._client: Any = None
        self._client_lock = threading.Lock()
        self._temporary_conversations: OrderedDict[str, Any] = OrderedDict()
        self._pending_branches: OrderedDict[str, Any] = OrderedDict()
        self._gate = threading.Lock()
        self._catalog_lock = threading.Lock()
        self._rename_gate = threading.Lock()
        self._history_list_gate = threading.Lock()
        self._history_list_next_request = 0.0
        self.editor = ChatGPTProjectEditor(
            self.session_state,
            self._factory,
            client_scope=self._ensure_client,
        )
        self._effective_transport = transport or self._ensure_owned_transport()
        super().__init__(account, type=type, transport=transport)

    def _ensure_owned_transport(self) -> HTTPTransport:
        if self._owned_transport is None:
            from curl_cffi.requests import Session as CurlSession

            self._owned_transport = HTTPTransport(
                "https://chatgpt.com",
                client_factory=lambda timeout=(10.0, 30.0): CurlSession(
                    impersonate="chrome",
                    trust_env=False,
                    timeout=timeout,
                    discard_cookies=True,
                ),
                config=TransportConfig(timeout=(10.0, 30.0)),
                headers=self._transport_headers,
            )
        return self._owned_transport

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        with self._client_lock:
            if self._client is None:
                options: dict[str, Any] = {}
                if self._provided_transport is not None or self._factory is ChatGPTClient:
                    options["transport"] = self._effective_transport
                client = self._factory(self.session_state, **options)
                client.temporary_conversations = self._temporary_conversations
                client.pending_branches = self._pending_branches
                self._client = client
            return self._client

    @contextmanager
    def _client_scope(self):
        if self._persistent_client:
            yield self._ensure_client()
            return
        client = self._factory(self.session_state)
        client.temporary_conversations = self._temporary_conversations
        client.pending_branches = self._pending_branches
        try:
            yield client
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()

    def _discard_client(self) -> None:
        client, self._client = self._client, None
        if client is None:
            return
        close = getattr(client, "close", None)
        if callable(close):
            close()

    def _prepare_session(self) -> None:
        if self._factory is ChatGPTClient:
            try:
                self.session_state.load()
            except ChatGPTAuthenticationError:
                stored = read_session(self.session_state.session_file)
                cookies = stored.get("cookies", stored)
                if not isinstance(cookies, dict):
                    raise
                cookies = ProviderCookieFilter.filter_cookies("https://chatgpt.com", cookies)
                self.session_state.import_cookies(cookies)
        self._prime_models(self._load_catalog())

    def _close_session(self) -> None:
        try:
            with self._gate, self._catalog_lock, self._rename_gate:
                self._temporary_conversations.clear()
                self._pending_branches.clear()
                self._discard_client()
        finally:
            try:
                if self._owned_transport is not None:
                    owned, self._owned_transport = self._owned_transport, None
                    owned.close()
            finally:
                if self._provided_transport is not None:
                    self._provided_transport.close()

    def _authenticate(self, *, capture_browser: bool = True) -> SessionStatus:
        with self._gate:
            self._discard_client()
            try:
                if capture_browser and self.session_state.is_ready():
                    url, headers = self.session_state.capture_auth()
                    self.session_state.save(url, headers)
                with self._client_scope() as client:
                    client.verify()
            except BaseException:
                self._discard_client()
                raise
        return SessionStatus(
            SessionState.AUTHENTICATED,
            session_exists(self.session_state.session_file),
        )

    def authenticate(self, *, capture_browser: bool = True) -> SessionStatus:
        return self._authenticate_session(capture_browser=capture_browser)

    def _forget_session(self) -> SessionStatus:
        with self._gate:
            self._discard_client()
            delete_session(self.session_state.session_file)
            self._temporary_conversations.clear()
            self._pending_branches.clear()
        return SessionStatus(SessionState.LOGIN_REQUIRED, False, "browser_session_retained")

    def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
        with self._catalog_lock, self._client_scope() as client:
            return client.model_catalog(refresh=True) if force else client.model_catalog()

    def _list_chats_data(
        self, *, cursor: str | None = None, limit: int = DEFAULT_PAGE_SIZE
    ) -> Page[Conversation]:
        offset = self._parse_list_chats_args(cursor, limit)
        with self._history_list_gate:
            delay = self._history_list_next_request - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            try:
                with self._client_scope() as client:
                    raw = client.list_page(offset, limit)
            except RateLimited as error:
                self._history_list_next_request = time.monotonic() + self._compute_list_backoff(
                    error
                )
                raise
        return self._build_list_chats_page(raw, offset, limit)

    def _get_chat_data(self, conversation_id: str) -> Conversation:
        with self._client_scope() as client:
            return self._conversation(client.get_conversation(conversation_id))

    def _branch_chat_data(self, conversation_id: str, message_id: str) -> Conversation:
        with self._client_scope() as client:
            current = self._conversation(client.get_conversation(conversation_id))
            _, target = target_message(current.messages, message_id, "assistant")
            if "branch" not in target.actions:
                raise FeatureUnavailable(Capability.CHAT_BRANCH)
            result = client.request(
                "POST",
                "https://chatgpt.com/backend-api/conversation/new_branch",
                json={"conversation_id": conversation_id, "message_id": message_id},
            ).json()
            remote, preview = self._parse_branch_preview(result, conversation_id, message_id)
            self._register_pending_branch(client, remote, preview)
            return self._conversation(preview)

    def _rename_chat_data(self, conversation_id: str, title: str) -> None:
        if not title.strip():
            raise ValueError("title must be nonempty")
        with self._client_scope() as client:
            client.rename_conversation(conversation_id, title.strip())

    def _delete_chat_data(self, conversation_id: str) -> None:
        with self._client_scope() as client:
            confirmed = client.delete_conversation(conversation_id)
        if confirmed is True:
            return

        def readback():
            with self._client_scope() as client:
                return client.get_conversation(conversation_id)

        confirm_deleted_sync(readback, lambda value: value.get("is_visible") is False)

    def _move_chat_data(
        self, conversation_id: str, project_id: str | None, *, source_project_id: str | None
    ) -> None:
        del source_project_id
        with self._client_scope() as client:
            client.move_conversation(conversation_id, project_id)

    def _list_projects_data(self, *, cursor: str | None = None) -> Page[ProjectData]:
        with self._client_scope() as client:
            raw = client.list_projects(cursor)
        return Page(tuple(self._project(item) for item in raw["items"]), raw.get("cursor"))

    def _create_project_data(
        self,
        name: str,
        instructions: str = "",
        *,
        memory_share: bool = True,
        **options: Any,
    ) -> ProjectData:
        del options
        if type(memory_share) is not bool:
            raise TypeError("memory_share must be a bool")
        with self._client_scope() as client:
            raw = client.create_project(name, instructions, memory_share=memory_share)
        return self._project(raw)

    def _get_project_data(self, project_id: str) -> ProjectData:
        with self._client_scope() as client:
            return self._project(client.get_project(project_id))

    def _project_options(self, project_id: str | None = None) -> ProjectOptions:
        return self.editor.options(project_id)

    def _list_project_chats_data(
        self, project_id: str, *, cursor: str | None = None, limit: int | None = None
    ) -> Page[Conversation]:
        with self._client_scope() as client:
            raw = client.list_project_chats(project_id, cursor, limit=min(limit or 30, 30))
        return Page(tuple(self._conversation(item) for item in raw["items"]), raw.get("cursor"))

    def _delete_project_data(self, project_id: str) -> None:
        with self._client_scope() as client:
            url = "https://chatgpt.com/backend-api/gizmos/" + quote(project_id, safe="")
            response = None
            for attempt in (0, 1):
                try:
                    response = client.request("DELETE", url, expect_json=False)
                    break
                except ChatGPTError as error:
                    if attempt == 0 and BaseChatGPTClient._is_retryable_gateway_error(error):
                        time.sleep(0.25)
                        continue
                    raise
        if self._check_delete_project_response(response):
            return
        confirm_deleted_sync(lambda: self._get_project_data(project_id))

    def _stream_provider_events(self, request: ProviderRequest) -> Iterator[ProviderStreamEvent]:
        timing = current_send_timing()
        if timing is not None:
            timing.record("provider_send_gate_wait_ms", 0)
        bound_emitted: str | None = None
        completed_tuple = None
        stream_finished = False
        try:
            scope_started = time.perf_counter_ns() if timing is not None else None
            with self._client_scope() as client:
                if timing is not None and scope_started is not None:
                    timing.record(
                        "provider_client_ready_ms", time.perf_counter_ns() - scope_started
                    )
                client.temporary_conversations = self._temporary_conversations
                client.pending_branches = self._pending_branches
                attachments = prepare_chatgpt_attachments_sync(client, request)
                for kind, value in client.iter_send_prompt(
                    request.conversation_id,
                    request.prompt,
                    **self._build_send_kwargs(request, attachments),
                ):
                    if kind == "paused":
                        yield value
                    elif kind == "raw":
                        bound_emitted, translated = self._translate_raw_event(value, bound_emitted)
                        if translated is not None:
                            yield translated
                    elif kind == "completed":
                        completed_tuple = value
                if completed_tuple is not None:
                    remote_id, completed, previous = completed_tuple
                    post_events: list[ProviderStreamEvent] = []
                    emit_post_send_events(
                        remote_id,
                        completed,
                        previous,
                        request,
                        lambda item: post_events.append(item[1]) if item[0] == "event" else None,
                        emit_bound=remote_id != bound_emitted,
                    )
                    yield from post_events
                stream_finished = True
        except ProviderError:
            raise
        except Exception as exc:
            raise ChatGPTError(
                "ChatGPT request failed; remote outcome is unknown and the request was not retried"
            ) from exc
        finally:
            if self._persistent_client and not stream_finished:
                self._discard_client()

    def _upload_file(self, path: Any, *, cancel_event: Any = None) -> dict[str, Any]:
        path_obj = Path(path)
        mime = mimetypes.guess_type(path_obj.name)[0] or "application/octet-stream"
        with self._client_scope() as client:
            return client.upload_file(path_obj.name, mime, path_obj, cancel_event=cancel_event)

    @classmethod
    def _download_conversation_snapshot(cls, client: Any, conversation_id: str, matches: Any):
        cached = cls._cached_download_snapshot(client, conversation_id, matches)
        return cached if cached is not None else client.get_conversation(conversation_id)

    def _resolve_download(
        self,
        url: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
    ) -> Any | None:
        parsed = urlsplit(url)
        if parsed.scheme == "file-service":
            file_id = self._validate_file_service_id(parsed, conversation_id)
            assert conversation_id is not None
            with self._client_scope() as client:
                chat = self._download_conversation_snapshot(
                    client, conversation_id, lambda p: self._has_file_service_url(p, url)
                )
                if not self._has_file_service_url(chat, url):
                    raise ValueError("File was not found in the conversation record or has expired")
                result = client.request(
                    "GET",
                    "https://chatgpt.com/backend-api/files/download/" + quote(file_id, safe=""),
                    params={"conversation_id": conversation_id, "inline": "false"},
                ).json()
                return self._build_download_source(
                    result,
                    client,
                    result.get("file_name") if isinstance(result, dict) else None,
                )
        estuary_id = self._parse_estuary_file_id(parsed, conversation_id)
        if estuary_id is not None:
            if estuary_id:
                return self._resolve_download(
                    "file-service://" + estuary_id, conversation_id, message_id
                )
            with self._client_scope() as client:
                return DownloadSource(url, force_download=True, headers=dict(client.headers))
        if parsed.scheme != "sandbox":
            return None
        sandbox_path = self._validate_sandbox_path(parsed, conversation_id)
        assert conversation_id is not None
        with self._client_scope() as client:
            chat = self._download_conversation_snapshot(
                client,
                conversation_id,
                lambda p: self._select_sandbox_message(p, sandbox_path, message_id) is not None,
            )
            selected = self._select_sandbox_message(chat, sandbox_path, message_id)
            if selected is None:
                raise ValueError("File was not found in the conversation record or has expired")
            endpoint = (
                "https://chatgpt.com/backend-api/conversation/"
                + quote(conversation_id, safe="")
                + "/interpreter/download"
            )
            result = client.request(
                "GET",
                endpoint,
                params={"message_id": selected["id"], "sandbox_path": sandbox_path},
            ).json()
            return self._build_download_source(result, client, sandbox_path.rsplit("/", 1)[-1])
