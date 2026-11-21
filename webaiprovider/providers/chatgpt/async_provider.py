from __future__ import annotations

import asyncio
import mimetypes
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlsplit

from ...accounts import delete_session, session_exists
from ...accounts.credentials import async_read_session
from ...accounts.filters import ProviderCookieFilter
from ...accounts.status import SessionState, SessionStatus
from ...async_provider import AsyncProvider
from ...base import Capability, ProviderType
from ...collections import Page
from ...core.concurrency import maybe_await
from ...core.timing import current_send_timing
from ...errors import FeatureUnavailable, ProviderError, RateLimited, RemoteNotFound
from ...resources.chat.collections import AsyncChatsCollection
from ...resources.chat.types import Conversation
from ...resources.message.protocol import ProviderRequest, ProviderStreamEvent
from ...resources.message.types import target_message
from ...resources.model.model import ModelCatalog
from ...resources.project.mutations import AsyncProjectMutationsMixin
from ...resources.project.types import ProjectData, ProjectOptions
from ...transport import AsyncHTTPTransport, AsyncTransport, TransportConfig
from ...transport.downloader import DownloadSource
from .async_client import (
    AsyncChatGPTClient,
    AsyncChatGPTProjectEditor,
    async_prepare_chatgpt_attachments,
)
from .auth import BrowserSession
from .base import BaseChatGPTClient, BaseChatGPTProvider
from .collections import AsyncChatGPTProjectsCollection
from .constants import DEFAULT_PAGE_SIZE
from .errors import ChatGPTAuthenticationError, ChatGPTError
from .formats import emit_post_send_events
from .resources import (
    AsyncChatGPTChat,
    AsyncChatGPTMessage,
    AsyncChatGPTProject,
    ChatGPTArtifact,
    ChatGPTAttachment,
    ChatGPTResponse,
    ChatGPTSource,
)

if TYPE_CHECKING:
    from ...accounts.account import Account


async def async_confirm_deleted(read: Any, missing: Any = lambda value: False) -> None:
    """Poll a remote resource until removal is confirmed or raise ProviderError."""
    for delay in (0, 0.2, 0.5, 1):
        if delay:
            await asyncio.sleep(delay)
        try:
            value = await read()
        except RemoteNotFound:
            return
        if missing(value):
            return
    raise ProviderError(
        "Deletion could not be confirmed by the server; the local record was preserved. Sync before retrying."
    )


class AsyncChatGPTProvider(
    AsyncProjectMutationsMixin,
    AsyncProvider[AsyncChatsCollection[AsyncChatGPTChat], AsyncChatGPTProjectsCollection],
    BaseChatGPTProvider,
):
    _chat_page_size = DEFAULT_PAGE_SIZE
    _chat_initial_page_floor = 100
    _message_type = AsyncChatGPTMessage
    _response_type = ChatGPTResponse
    _source_type = ChatGPTSource
    _artifact_type = ChatGPTArtifact
    _attachment_type = ChatGPTAttachment

    def _make_chats(self) -> AsyncChatsCollection[AsyncChatGPTChat]:
        return AsyncChatsCollection(self, AsyncChatGPTChat)

    def _make_projects(self) -> AsyncChatGPTProjectsCollection:
        return AsyncChatGPTProjectsCollection(self, AsyncChatGPTProject)

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        session: BrowserSession | None = None,
        client_factory: Callable[..., Any] | None = None,
        transport: AsyncTransport | None = None,
    ) -> None:
        if transport is not None and transport.origin != "https://chatgpt.com":
            raise ValueError("ChatGPT transport must use the ChatGPT origin")
        self._provided_transport = transport
        self._owned_transport: AsyncHTTPTransport | None = None
        self.session_state = session or BrowserSession(account.store())
        resolved_factory = AsyncChatGPTClient if client_factory is None else client_factory
        self._factory = resolved_factory
        self._persistent_client = resolved_factory is AsyncChatGPTClient
        self._client: Any = None
        self._client_lock = asyncio.Lock()
        self._temporary_conversations: OrderedDict[str, Any] = OrderedDict()
        self._pending_branches: OrderedDict[str, Any] = OrderedDict()
        self._gate = asyncio.Lock()
        self._catalog_lock = asyncio.Lock()
        self._rename_gate = asyncio.Lock()
        self._history_list_gate = asyncio.Lock()
        self._prepare_done: asyncio.Event | None = None
        self._read_loop: asyncio.AbstractEventLoop | None = None
        self._read_semaphore: asyncio.Semaphore | None = None
        self._history_list_next_request = 0.0
        self.editor = AsyncChatGPTProjectEditor(
            self.session_state,
            self._factory,
            client_scope=self._ensure_client,
        )
        self._effective_transport = transport or self._ensure_owned_transport()
        super().__init__(account, type=type, transport=transport)

    def _ensure_owned_transport(self) -> AsyncHTTPTransport:
        if self._owned_transport is None:
            from curl_cffi.requests import AsyncSession

            from ...transport.network import create_curl_share

            self._owned_transport = AsyncHTTPTransport(
                "https://chatgpt.com",
                client_factory=lambda timeout=(10.0, 30.0): AsyncSession(
                    impersonate="chrome",
                    trust_env=False,
                    timeout=timeout,
                    discard_cookies=True,
                    curl_share=create_curl_share(),
                ),
                config=TransportConfig(timeout=(10.0, 30.0)),
                headers=self._transport_headers,
            )
        return self._owned_transport

    async def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        async with self._client_lock:
            if self._client is None:
                options: dict[str, Any] = {}
                if self._provided_transport is not None or self._factory is AsyncChatGPTClient:
                    options["transport"] = self._effective_transport
                client = self._factory(self.session_state, **options)
                client.temporary_conversations = self._temporary_conversations
                client.pending_branches = self._pending_branches
                self._client = client
            return self._client

    @asynccontextmanager
    async def _client_scope(self):
        if self._persistent_client:
            yield await self._ensure_client()
            return
        client = self._factory(self.session_state)
        client.temporary_conversations = self._temporary_conversations
        client.pending_branches = self._pending_branches
        try:
            yield client
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                await maybe_await(close())

    async def _discard_client(self) -> None:
        client, self._client = self._client, None
        if client is None:
            return
        close = getattr(client, "close", None)
        if callable(close):
            await maybe_await(close())

    async def _wait_for_prepare(self) -> None:
        pending = self._prepare_done
        if pending is not None:
            await pending.wait()

    def _project_read_semaphore(self) -> asyncio.Semaphore:
        loop = asyncio.get_running_loop()
        if self._read_loop is not loop or self._read_semaphore is None:
            self._read_loop = loop
            self._read_semaphore = asyncio.Semaphore(4)
        return self._read_semaphore

    async def _prepare_session(self) -> None:
        done = asyncio.Event()
        previous_done = self._prepare_done
        self._prepare_done = done
        try:
            if previous_done is not None:
                await previous_done.wait()
            auth_ready = True
            if self._factory is AsyncChatGPTClient:
                try:
                    await self.session_state.load_async()
                except ChatGPTAuthenticationError:
                    auth_ready = False
                    stored = await async_read_session(self.session_state.session_file)
                    cookies = stored.get("cookies", stored)
                    if not isinstance(cookies, dict):
                        raise
                    cookies = ProviderCookieFilter.filter_cookies("https://chatgpt.com", cookies)
                    await self.session_state.import_cookies_async(cookies)
            prewarmed = False
            if self._persistent_client and auth_ready:
                client = await self._ensure_client()
                prewarm = getattr(client, "trigger_sentinel_prewarm", None)
                if callable(prewarm):
                    prewarm()
                    prewarmed = True
            self._prime_models(await self._load_catalog())
            if not prewarmed and self._persistent_client and self._client is not None:
                prewarm = getattr(self._client, "trigger_sentinel_prewarm", None)
                if callable(prewarm):
                    prewarm()
        finally:
            done.set()
            if self._prepare_done is done:
                self._prepare_done = None

    async def _close_session(self) -> None:
        try:
            async with self._gate, self._catalog_lock, self._rename_gate:
                try:
                    self._read_loop = None
                    self._read_semaphore = None
                finally:
                    self._temporary_conversations.clear()
                    self._pending_branches.clear()
                    await self._discard_client()
        finally:
            try:
                if self._owned_transport is not None:
                    owned, self._owned_transport = self._owned_transport, None
                    await owned.close()
            finally:
                if self._provided_transport is not None:
                    await self._provided_transport.close()

    async def _authenticate(self, *, capture_browser: bool = True) -> SessionStatus:
        async with self._gate:
            await self._discard_client()
            try:
                if capture_browser and self.session_state.is_ready():

                    def capture():
                        url, headers = self.session_state.capture_auth()
                        self.session_state.save(url, headers)

                    await asyncio.to_thread(capture)
                async with self._client_scope() as client:
                    await maybe_await(client.verify())
            except BaseException:
                await self._discard_client()
                raise
        return SessionStatus(
            SessionState.AUTHENTICATED,
            session_exists(self.session_state.session_file),
        )

    async def authenticate(self, *, capture_browser: bool = True) -> SessionStatus:
        return await self._authenticate_session(capture_browser=capture_browser)

    async def _forget_session(self) -> SessionStatus:
        async with self._gate:
            await self._discard_client()
            await asyncio.to_thread(delete_session, self.session_state.session_file)
            self._temporary_conversations.clear()
            self._pending_branches.clear()
        return SessionStatus(SessionState.LOGIN_REQUIRED, False, "browser_session_retained")

    async def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
        async with self._catalog_lock, self._client_scope() as client:
            return await maybe_await(
                client.model_catalog(refresh=True) if force else client.model_catalog()
            )

    async def _list_chats_data(
        self, *, cursor: str | None = None, limit: int = DEFAULT_PAGE_SIZE
    ) -> Page[Conversation]:
        offset = self._parse_list_chats_args(cursor, limit)
        await self._wait_for_prepare()
        loop = asyncio.get_running_loop()
        async with self._history_list_gate:
            delay = self._history_list_next_request - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                async with self._client_scope() as client:
                    raw = await maybe_await(client.list_page(offset, limit))
            except RateLimited as error:
                self._history_list_next_request = loop.time() + self._compute_list_backoff(error)
                raise
        return self._build_list_chats_page(raw, offset, limit)

    async def _warm_chat(self, chat: Any) -> None:
        cid = chat.remote_id
        if (
            not self._persistent_client
            or not cid
            or chat.temporary
            or cid in self._pending_branches
        ):
            return
        async with self._client_scope() as client:
            if client._cached_continuation(cid) is None:
                await maybe_await(client.get_conversation(cid))

    async def _get_chat_data(self, conversation_id: str) -> Conversation:
        await self._wait_for_prepare()
        async with self._client_scope() as client:
            raw = await maybe_await(client.get_conversation(conversation_id))
        return self._conversation(raw)

    async def _branch_chat_data(self, conversation_id: str, message_id: str) -> Conversation:
        async with self._client_scope() as client:
            current = self._conversation(
                await maybe_await(client.get_conversation(conversation_id))
            )
            _, target = target_message(current.messages, message_id, "assistant")
            if "branch" not in target.actions:
                raise FeatureUnavailable(Capability.CHAT_BRANCH)
            result = (
                await maybe_await(
                    client.request(
                        "POST",
                        "https://chatgpt.com/backend-api/conversation/new_branch",
                        json={"conversation_id": conversation_id, "message_id": message_id},
                    )
                )
            ).json()
            remote, preview = self._parse_branch_preview(result, conversation_id, message_id)
            self._register_pending_branch(client, remote, preview)
            return self._conversation(preview)

    async def _rename_chat_data(self, conversation_id: str, title: str) -> None:
        if not title.strip():
            raise ValueError("title must be nonempty")
        async with self._client_scope() as client:
            await maybe_await(client.rename_conversation(conversation_id, title.strip()))

    async def _delete_chat_data(self, conversation_id: str) -> None:
        async with self._client_scope() as client:
            confirmed = await maybe_await(client.delete_conversation(conversation_id))
        if confirmed is True:
            return

        async def readback():
            async with self._client_scope() as client:
                return await maybe_await(client.get_conversation(conversation_id))

        await async_confirm_deleted(readback, lambda value: value.get("is_visible") is False)

    async def _move_chat_data(
        self, conversation_id: str, project_id: str | None, *, source_project_id: str | None
    ) -> None:
        del source_project_id
        async with self._client_scope() as client:
            await maybe_await(client.move_conversation(conversation_id, project_id))

    async def _list_projects_data(self, *, cursor: str | None = None) -> Page[ProjectData]:
        await self._wait_for_prepare()
        async with self._project_read_semaphore(), self._client_scope() as client:
            raw = await maybe_await(client.list_projects(cursor))
        return Page(tuple(self._project(item) for item in raw["items"]), raw.get("cursor"))

    async def _create_project_data(
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
        async with self._client_scope() as client:
            raw = await maybe_await(
                client.create_project(name, instructions, memory_share=memory_share)
            )
        return self._project(raw)

    async def _get_project_data(self, project_id: str) -> ProjectData:
        await self._wait_for_prepare()
        async with self._client_scope() as client:
            raw = await maybe_await(client.get_project(project_id))
        return self._project(raw)

    async def _project_options(self, project_id: str | None = None) -> ProjectOptions:
        if project_id is not None:
            await self._wait_for_prepare()
            async with self._project_read_semaphore():
                return await self.editor.options(project_id)
        return await self.editor.options()

    async def _list_project_chats_data(
        self, project_id: str, *, cursor: str | None = None, limit: int | None = None
    ) -> Page[Conversation]:
        await self._wait_for_prepare()
        async with self._project_read_semaphore(), self._client_scope() as client:
            raw = await maybe_await(
                client.list_project_chats(project_id, cursor, limit=min(limit or 30, 30))
            )
        return Page(tuple(self._conversation(item) for item in raw["items"]), raw.get("cursor"))

    async def _delete_project_data(self, project_id: str) -> None:
        async with self._client_scope() as client:
            url = "https://chatgpt.com/backend-api/gizmos/" + quote(project_id, safe="")
            response = None
            for attempt in (0, 1):
                try:
                    response = await maybe_await(client.request("DELETE", url, expect_json=False))
                    break
                except ChatGPTError as error:
                    if attempt == 0 and BaseChatGPTClient._is_retryable_gateway_error(error):
                        await asyncio.sleep(0.25)
                        continue
                    raise
        if self._check_delete_project_response(response):
            return
        await async_confirm_deleted(lambda: self._get_project_data(project_id))

    async def _stream_provider_events(
        self, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamEvent]:
        queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
        bound_emitted: str | None = None

        def publish(item: tuple[str, Any]) -> None:
            queue.put_nowait(item)

        def event(value: dict[str, Any]) -> None:
            nonlocal bound_emitted
            bound_emitted, translated = self._translate_raw_event(value, bound_emitted)
            if translated is not None:
                publish(("event", translated))

        event._accepts_bound = True  # type: ignore[attr-defined]

        async def run_send() -> None:
            try:
                timing = current_send_timing()
                scope_started = time.perf_counter_ns() if timing is not None else None
                async with self._client_scope() as client:
                    if timing is not None and scope_started is not None:
                        timing.record(
                            "provider_client_ready_ms", time.perf_counter_ns() - scope_started
                        )
                    client.temporary_conversations = self._temporary_conversations
                    client.pending_branches = self._pending_branches
                    attachments = await async_prepare_chatgpt_attachments(client, request)
                    remote_id, completed, previous = await maybe_await(
                        client.send_prompt(
                            request.conversation_id,
                            request.prompt,
                            on_event=event,
                            **self._build_send_kwargs(request, attachments),
                        )
                    )
                    emit_post_send_events(
                        remote_id,
                        completed,
                        previous,
                        request,
                        publish,
                        emit_bound=remote_id != bound_emitted,
                    )
            except asyncio.CancelledError:
                raise
            except ProviderError as error:
                publish(("error", error))
            except Exception:
                publish(
                    (
                        "error",
                        ChatGPTError(
                            "ChatGPT request failed; remote outcome is unknown and the request was not retried"
                        ),
                    )
                )
            finally:
                publish(("done", None))

        timing = current_send_timing()
        if timing is not None:
            timing.record("provider_send_gate_wait_ms", 0)
        send_task = asyncio.create_task(run_send())
        stream_finished = False
        try:
            while True:
                kind, value = await queue.get()
                if kind == "done":
                    stream_finished = True
                    break
                if kind == "error":
                    raise value
                yield value
        finally:

            async def cleanup() -> None:
                if not send_task.done():
                    send_task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await send_task
                if self._persistent_client and not stream_finished:
                    await self._discard_client()

            cleanup_task = asyncio.create_task(cleanup())
            cancelled = False
            while not cleanup_task.done():
                try:
                    await asyncio.shield(cleanup_task)
                except asyncio.CancelledError:
                    cancelled = True
            cleanup_task.result()
            if cancelled:
                raise asyncio.CancelledError

    async def _upload_file(self, path: Any, *, cancel_event: Any = None) -> dict[str, Any]:
        path_obj = Path(path)
        mime = mimetypes.guess_type(path_obj.name)[0] or "application/octet-stream"
        if self._persistent_client:
            async with self._client_scope() as client:
                return await client.upload_file(
                    path_obj.name, mime, path_obj, cancel_event=cancel_event
                )

        client = self._factory(self.session_state)
        try:
            return await maybe_await(
                client.upload_file(path_obj.name, mime, path_obj, cancel_event=cancel_event)
            )
        except asyncio.CancelledError:
            close = getattr(client, "close", None)
            if callable(close):

                async def cleanup():
                    with suppress(Exception):
                        await maybe_await(close())

                cleanup_task = asyncio.create_task(cleanup())
                while not cleanup_task.done():
                    try:
                        await asyncio.shield(cleanup_task)
                    except asyncio.CancelledError:
                        continue
                cleanup_task.result()
            raise
        else:
            close = getattr(client, "close", None)
            if callable(close):
                await maybe_await(close())

    @staticmethod
    async def _download_conversation_snapshot(client: Any, conversation_id: str, matches: Any):
        cached = getattr(client, "temporary_conversations", {}).get(conversation_id)
        return (
            cached
            if isinstance(cached, dict) and matches(cached)
            else await maybe_await(client.get_conversation(conversation_id))
        )

    async def _resolve_download(
        self,
        url: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
    ) -> Any | None:
        parsed = urlsplit(url)
        if parsed.scheme == "file-service":
            file_id = self._validate_file_service_id(parsed, conversation_id)
            assert conversation_id is not None
            async with self._client_scope() as client:
                chat = await self._download_conversation_snapshot(
                    client, conversation_id, lambda p: self._has_file_service_url(p, url)
                )
                if not self._has_file_service_url(chat, url):
                    raise ValueError("File was not found in the conversation record or has expired")
                result = (
                    await maybe_await(
                        client.request(
                            "GET",
                            "https://chatgpt.com/backend-api/files/download/"
                            + quote(file_id, safe=""),
                            params={"conversation_id": conversation_id, "inline": "false"},
                        )
                    )
                ).json()
                return self._build_download_source(
                    result,
                    client,
                    result.get("file_name") if isinstance(result, dict) else None,
                )
        estuary_id = self._parse_estuary_file_id(parsed, conversation_id)
        if estuary_id is not None:
            if estuary_id:
                return await self._resolve_download(
                    "file-service://" + estuary_id, conversation_id, message_id
                )
            async with self._client_scope() as client:
                return DownloadSource(url, force_download=True, headers=dict(client.headers))
        if parsed.scheme != "sandbox":
            return None
        sandbox_path = self._validate_sandbox_path(parsed, conversation_id)
        assert conversation_id is not None
        async with self._client_scope() as client:
            chat = await self._download_conversation_snapshot(
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
            result = (
                await maybe_await(
                    client.request(
                        "GET",
                        endpoint,
                        params={"message_id": selected["id"], "sandbox_path": sandbox_path},
                    )
                )
            ).json()
            return self._build_download_source(result, client, sandbox_path.rsplit("/", 1)[-1])
