from __future__ import annotations

import asyncio
import math
import re
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, quote, unquote, urlsplit

from ...accounts import delete_session, session_exists
from ...accounts.credentials import read_session_async
from ...accounts.filters import ProviderCookieFilter
from ...accounts.status import SessionState, SessionStatus
from ...async_provider import AsyncProvider
from ...base import Capability, ProviderType
from ...collections import Page
from ...core.concurrency import maybe_await
from ...core.files import DownloadSource
from ...core.json import json_dumps
from ...core.timing import current_send_timing
from ...errors import (
    FeatureUnavailable,
    ProviderError,
    RateLimited,
    RemoteNotFound,
)
from ...resources.chat.collections import AsyncChatsCollection
from ...resources.chat.types import Conversation
from ...resources.message.protocol import (
    ConversationBound,
    ProviderRequest,
    ProviderStreamEvent,
    ResponseComplete,
    TextDelta,
    TextReplacement,
)
from ...resources.message.types import target_message
from ...resources.model.model import ModelCatalog
from ...resources.project.mutations import ProjectMutationsMixin
from ...resources.project.types import ProjectData, ProjectOptions
from ...transport import (
    AsyncHTTPTransport,
    AsyncTransport,
    TransportConfig,
)
from .auth import BrowserSession
from .base import BaseChatGPTProvider
from .client import ChatGPTClient, prepare_chatgpt_attachments
from .collections import AsyncChatGPTProjectsCollection
from .constants import DEFAULT_PAGE_SIZE
from .errors import ChatGPTAuthenticationError, ChatGPTError
from .formats import emit_post_send_events, message_files
from .projects import ChatGPTProjectEditor
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


async def confirm_deleted(read: Any, missing: Any = lambda value: False) -> None:
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
    ProjectMutationsMixin,
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
        resolved_factory = ChatGPTClient if client_factory is None else client_factory
        self._factory = resolved_factory
        self._persistent_client = resolved_factory is ChatGPTClient
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
        self.editor = ChatGPTProjectEditor(
            self.session_state,
            self._factory,
            client_scope=self._ensure_client,
        )
        self._effective_transport = transport or self._ensure_owned_transport()
        super().__init__(account, type=type, transport=transport)

    @property
    def session(self) -> BrowserSession:
        return self.session_state

    def _active_transport(self) -> AsyncTransport | None:
        return self._effective_transport

    def _transport_headers(self, method: str, path: str) -> dict[str, str]:
        client = self._client
        if client is not None and callable(getattr(client, "_transport_headers", None)):
            return client._transport_headers(method, path)
        return {}

    def _ensure_owned_transport(self) -> AsyncHTTPTransport:
        if self._owned_transport is None:
            from curl_cffi.requests import AsyncSession

            self._owned_transport = AsyncHTTPTransport(
                "https://chatgpt.com",
                client_factory=lambda timeout=(10.0, 30.0): AsyncSession(
                    impersonate="chrome",
                    trust_env=False,
                    timeout=timeout,
                    discard_cookies=True,
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
                if self._provided_transport is not None or self._factory is ChatGPTClient:
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
        """Bootstrap cookie credentials, then warm and retain the native async client."""
        done = asyncio.Event()
        previous_done = self._prepare_done
        self._prepare_done = done
        try:
            if previous_done is not None:
                await previous_done.wait()
            auth_ready = True
            if self._factory is ChatGPTClient:
                try:
                    await self.session_state.load_async()
                except ChatGPTAuthenticationError:
                    auth_ready = False
                    stored = await read_session_async(self.session_state.session_file)
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

    async def _session_status(self) -> SessionStatus:
        saved = session_exists(self.session_state.session_file)
        return SessionStatus(
            SessionState.UNKNOWN if saved else SessionState.LOGIN_REQUIRED,
            saved,
            "snapshot_not_verified" if saved else "no_saved_session",
        )

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
        async with self.runtime.activity():
            return await self._authenticate(capture_browser=capture_browser)

    async def _forget_session(self) -> SessionStatus:
        async with self._gate:
            await self._discard_client()
            await asyncio.to_thread(delete_session, self.session_state.session_file)
            self._temporary_conversations.clear()
            self._pending_branches.clear()
        return SessionStatus(SessionState.LOGIN_REQUIRED, False, "browser_session_retained")

    async def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
        async with self._catalog_lock:
            async with self._client_scope() as client:
                return await maybe_await(
                    client.model_catalog(refresh=True) if force else client.model_catalog()
                )

    async def _list_chats_data(
        self, *, cursor: str | None = None, limit: int = DEFAULT_PAGE_SIZE
    ) -> Page[Conversation]:
        offset = int(cursor or "0")
        if offset < 0 or not 1 <= limit <= 100:
            raise ValueError("invalid conversation page")

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
                retry_after = error.retry_after
                if (
                    not isinstance(retry_after, (int, float))
                    or not math.isfinite(retry_after)
                    or retry_after <= 0
                ):
                    retry_after = 2.0
                self._history_list_next_request = loop.time() + max(0.75, retry_after)
                raise
        items = raw.get("items", [])
        if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
            raise ValueError("invalid conversation page")
        return Page(
            tuple(self._conversation(item) for item in items),
            str(offset + len(items)) if len(items) == limit else None,
        )

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
            preview = result.get("conversation") if isinstance(result, dict) else None
            remote = preview.get("conversation_id") if isinstance(preview, dict) else None
            if not isinstance(remote, str) or not remote or remote == conversation_id:
                raise ValueError("ChatGPT did not confirm a new conversation")
            if quote(remote, safe="") != remote:
                raise ValueError("Invalid branch conversation identity")
            mapping, node = preview.get("mapping"), preview.get("current_node")
            if not isinstance(mapping, dict) or node not in mapping:
                raise ValueError("ChatGPT branch preview has no selected message path")
            selected, seen = [], set()
            while node in mapping and node not in seen:
                seen.add(node)
                item = mapping[node]
                if item.get("message"):
                    selected.append(item["message"])
                node = item.get("parent")
            preview = dict(
                preview,
                messages=list(reversed(selected)),
                pending_branch={
                    "conversation_id": conversation_id,
                    "message_id": message_id,
                },
            )
            preview.pop("mapping", None)
            client.pending_branches = self._pending_branches
            client.pending_branches[remote] = preview
            client.pending_branches.move_to_end(remote)
            while len(client.pending_branches) > 32:
                client.pending_branches.popitem(last=False)
            remember = getattr(client, "_remember_conversation", None)
            if callable(remember):
                remember(preview)
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

        await confirm_deleted(readback, lambda value: value.get("is_visible") is False)

    async def _move_chat_data(self, conversation_id: str, project_id: str | None) -> None:
        async with self._client_scope() as client:
            await maybe_await(client.move_conversation(conversation_id, project_id))

    async def _list_projects_data(self, *, cursor: str | None = None) -> Page[ProjectData]:
        await self._wait_for_prepare()
        async with self._project_read_semaphore():
            async with self._client_scope() as client:
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
        async with self._project_read_semaphore():
            async with self._client_scope() as client:
                raw = await maybe_await(
                    client.list_project_chats(project_id, cursor, limit=min(limit or 30, 30))
                )
        return Page(
            tuple(self._conversation(item) for item in raw["items"]),
            raw.get("cursor"),
        )

    async def _delete_project_data(self, project_id: str) -> None:
        async with self.runtime.project_lock(project_id):
            async with self._client_scope() as client:
                url = "https://chatgpt.com/backend-api/gizmos/" + quote(project_id, safe="")
                for attempt in (0, 1):
                    try:
                        response = await maybe_await(
                            client.request("DELETE", url, expect_json=False)
                        )
                        break
                    except ChatGPTError as error:
                        if attempt == 0 and any(
                            f"HTTP {code} " in str(error) for code in (502, 503, 504)
                        ):
                            await asyncio.sleep(0.25)
                            continue
                        raise
            payload = response.json() if getattr(response, "content", None) else None
            if isinstance(payload, dict) and payload.get("success") is False:
                raise ChatGPTError("ChatGPT refused to delete the project.")
            confirmed = getattr(response, "status_code", None) in (200, 202, 204)
        if confirmed:
            return
        await confirm_deleted(lambda: self._get_project_data(project_id))

    async def _stream_provider_events(
        self, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamEvent]:
        queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
        bound_emitted: str | None = None

        def publish(item: tuple[str, Any]) -> None:
            queue.put_nowait(item)

        def event(value: dict[str, Any]) -> None:
            nonlocal bound_emitted
            kind = value.get("type")
            if kind == "bound":
                cid = value.get("conversation_id")
                if isinstance(cid, str) and cid and cid != bound_emitted:
                    bound_emitted = cid
                    publish(("event", ConversationBound(cid)))
            elif kind == "delta":
                publish(("event", TextDelta(value.get("text", ""), value.get("channel"))))
            elif kind == "replace":
                publish(("event", TextReplacement(value.get("text", ""), value.get("channel"))))
            elif kind == "response_complete":
                publish(("event", ResponseComplete()))

        event._accepts_bound = True  # type: ignore[attr-defined]

        async def run_send() -> None:
            try:
                timing = current_send_timing()
                scope_started = time.perf_counter_ns() if timing is not None else None
                async with self._client_scope() as client:
                    if scope_started is not None:
                        timing.record(
                            "provider_client_ready_ms", time.perf_counter_ns() - scope_started
                        )
                    client.temporary_conversations = self._temporary_conversations
                    client.pending_branches = self._pending_branches
                    attachments = await prepare_chatgpt_attachments(client, request)
                    remote_id, completed, previous = await maybe_await(
                        client.send_prompt(
                            request.conversation_id,
                            request.prompt,
                            on_event=event,
                            **({"attachments": attachments} if attachments else {}),
                            **({"model": request.model} if request.model is not None else {}),
                            **({"effort": request.effort} if request.effort is not None else {}),
                            **({"project_id": request.project_id} if request.project_id else {}),
                            **({"temporary": True} if request.temporary else {}),
                            **(
                                {"action": request.action, "message_id": request.message_id}
                                if request.action
                                else {}
                            ),
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
        import mimetypes

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
        cached_fn = getattr(client, "_cached_conversation", None)
        if callable(cached_fn):
            cached = cached_fn(conversation_id)
            if isinstance(cached, dict) and matches(cached):
                return cached
        temporary = getattr(client, "temporary_conversations", None)
        if isinstance(temporary, dict):
            temp_snapshot = temporary.get(conversation_id)
            if isinstance(temp_snapshot, dict) and matches(temp_snapshot):
                return temp_snapshot
        return await maybe_await(client.get_conversation(conversation_id))

    async def _resolve_download(
        self,
        url: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
    ) -> Any | None:
        parsed = urlsplit(url)
        if parsed.scheme == "file-service":
            file_id = parsed.netloc
            if (
                not re.fullmatch(r"file[-_][A-Za-z0-9_-]+", file_id)
                or parsed.path
                or not conversation_id
            ):
                raise ValueError("File must belong to the selected conversation")

            def has_file(chat_payload: dict) -> bool:
                return any(
                    file.get("url") == url
                    for item in chat_payload.get("messages", ())
                    if isinstance(item, dict)
                    for file in message_files(item)
                )

            async with self._client_scope() as client:
                chat = await self._download_conversation_snapshot(client, conversation_id, has_file)
                if not has_file(chat):
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
                target = result.get("download_url") if isinstance(result, dict) else None
                if not target or urlsplit(target).scheme != "https":
                    raise ValueError("Provider did not return a file download URL")
                headers = dict(client.headers) if urlsplit(target).netloc == "chatgpt.com" else {}
                return DownloadSource(target, result.get("file_name"), True, headers)
        if (
            parsed.scheme == "https"
            and parsed.netloc == "chatgpt.com"
            and parsed.path == "/backend-api/estuary/content"
        ):
            file_id = parse_qs(parsed.query).get("id", [None])[0]
            if file_id and file_id.startswith(("file-", "file_")) and conversation_id:
                return await self._resolve_download(
                    "file-service://" + file_id, conversation_id, message_id
                )

            async with self._client_scope() as client:
                return DownloadSource(url, force_download=True, headers=dict(client.headers))
        if parsed.scheme != "sandbox":
            return None
        if not conversation_id:
            raise ValueError("Conversation ID is not available for this file yet")
        sandbox_path = unquote(parsed.path)
        if not sandbox_path.startswith("/mnt/data/"):
            raise ValueError("Unsupported conversation file path")

        def select_sandbox_message(chat_payload: dict) -> dict | None:
            candidates = chat_payload.get("messages", [])
            if not candidates and isinstance(chat_payload.get("mapping"), dict):
                candidates = [
                    node.get("message")
                    for node in chat_payload["mapping"].values()
                    if isinstance(node, dict)
                ]
            matched = [
                item
                for item in candidates
                if isinstance(item, dict) and sandbox_path in json_dumps(item.get("content", {}))
            ]
            selected = next(
                (item for item in matched if item.get("id") == message_id),
                matched[0] if len(matched) == 1 else None,
            )
            return selected if isinstance(selected, dict) and selected.get("id") else None

        async with self._client_scope() as client:
            chat = await self._download_conversation_snapshot(
                client,
                conversation_id,
                lambda payload: select_sandbox_message(payload) is not None,
            )
            selected = select_sandbox_message(chat)
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
            target = result.get("download_url") if isinstance(result, dict) else None
            if not target or urlsplit(target).scheme != "https":
                raise ValueError("Provider did not return a file download URL")
            headers = dict(client.headers) if urlsplit(target).netloc == "chatgpt.com" else {}
            return DownloadSource(target, sandbox_path.rsplit("/", 1)[-1], True, headers)
