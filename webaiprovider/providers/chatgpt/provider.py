from __future__ import annotations

import asyncio
import inspect
import math
import re
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs, quote, unquote, urlsplit

from ...accounts import delete_session, session_exists
from ...accounts.credentials import read_session_async
from ...accounts.filters import ProviderCookieFilter
from ...accounts.status import SessionState, SessionStatus
from ...async_provider import AsyncProvider
from ...base import Capability, ProjectMutationsMixin, confirm_deleted
from ...collections import Page, _publish_collection_change
from ...core import gather_bounded
from ...core.files import DownloadSource
from ...core.json import json_dumps
from ...core.operations import execute_with_hooks
from ...core.timing import current_send_timing
from ...errors import FeatureUnavailable, ProviderError, RateLimited, UnsupportedOperationError
from ...provider import Provider, ProviderType
from ...resources.chat.chat import (
    AsyncChat,
    BranchableChat,
    Chat,
    MovableChat,
    _resolve_project_arg,
)
from ...resources.chat.types import Conversation
from ...resources.message.attachment import Attachment
from ...resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    ProviderRequest,
    ProviderStreamEvent,
    ResponseComplete,
    TextDelta,
    TextReplacement,
)
from ...resources.message.types import Content, MessageData, actions_for, target_message
from ...resources.model.model import ModelCatalog
from ...resources.project.collections import ProjectsCollection
from ...resources.project.project import AsyncProject, Project
from ...resources.project.types import ProjectData
from ...resources.response.source import sources_from
from ...transport import (
    AsyncHTTPTransport,
    AsyncTransport,
    TransportConfig,
)
from .client import ChatGPTClient
from .collections import AsyncChatGPTProjectsCollection
from .constants import DEFAULT_PAGE_SIZE
from .discovery import project_id
from .errors import ChatGPTAuthenticationError, ChatGPTError
from .formats import message_files, message_parts, text_from_parts
from .projects import ChatGPTProjectEditor
from .resources import AsyncChatGPTChat
from .session import BrowserSession, SessionPaths

if TYPE_CHECKING:
    from ...accounts.account import Account


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _prepare_chatgpt_attachments(client: Any, request: ProviderRequest) -> list[Any]:
    items = request.attachments
    attachments: list[Any] = [None] * len(items)
    pending_uploads: list[tuple[int, dict[str, Any]]] = []
    for index, item in enumerate(items):
        if isinstance(item, dict) and ("bytes" in item or "path" in item):
            pending_uploads.append((index, item))
        else:
            attachments[index] = item

    async def run_upload(entry: tuple[int, dict[str, Any]]) -> tuple[int, Any]:
        index, spec = entry
        result = await _maybe_await(
            client.upload_file(
                spec.get("name") or (Path(spec["path"]).name if spec.get("path") else "attachment"),
                spec.get("mime_type") or "application/octet-stream",
                spec["bytes"] if spec.get("bytes") is not None else Path(spec["path"]),
                project_id=request.project_id or None,
            )
        )
        return index, result

    if len(pending_uploads) == 1:
        index, uploaded = await run_upload(pending_uploads[0])
        attachments[index] = uploaded
    elif len(pending_uploads) > 1:
        results = await gather_bounded(pending_uploads, run_upload, limit=3)
        for index, uploaded in results:
            attachments[index] = uploaded
    return attachments


def _emit_chatgpt_post_send_events(
    remote_id: str,
    completed: dict[str, Any],
    previous: object,
    request: ProviderRequest,
    publish: Callable[[tuple[str, object]], None],
    *,
    emit_bound: bool = True,
) -> None:
    response_messages = completed.get("messages", [])
    previous_ids = set(previous or ())
    summaries = []
    for item in response_messages:
        if item.get("id") in previous_ids or item.get("recipient") not in (None, "all"):
            continue
        if (item.get("metadata") or {}).get("is_visually_hidden_from_conversation"):
            continue
        if (item.get("author") or {}).get("role") != "assistant":
            continue
        content = item.get("content") or {}
        if item.get("channel") == "analysis" or content.get("content_type") in (
            "thoughts",
            "reasoning_recap",
            "reasoning_summary",
        ):
            text = text_from_parts(message_parts(content))
            if text:
                summaries.append(text)
    if summaries:
        publish(("event", TextReplacement("\n\n".join(summaries), "analysis")))

    final = None
    last_user_index = None
    for index in range(len(response_messages) - 1, -1, -1):
        item = response_messages[index]
        role = (item.get("author") or {}).get("role")
        if (
            final is None
            and role == "assistant"
            and item.get("channel") != "analysis"
            and item.get("recipient") in (None, "all")
        ):
            final = item
        if last_user_index is None and role == "user":
            last_user_index = index
        if final is not None and last_user_index is not None:
            break
    if final:
        turn_start = (last_user_index + 1) if last_user_index is not None else 0
        user = response_messages[last_user_index] if last_user_index is not None else {}
        metadata = final.get("metadata") or {}
        publish(
            (
                "event",
                MessageMetadata(
                    final.get("id"),
                    user.get("id"),
                    metadata.get("model_slug") or request.model,
                    metadata.get("reasoning_effort") or request.effort,
                    sources_from(final),
                    files=message_files(*response_messages[turn_start:]),
                ),
            )
        )
    if emit_bound:
        publish(("event", ConversationBound(remote_id)))


class _Stopped(Exception):
    pass


class AsyncChatGPTProvider(ProjectMutationsMixin, AsyncProvider):
    provider_type = ProviderType.CHATGPT

    _chat_page_size = DEFAULT_PAGE_SIZE
    _chat_type = AsyncChatGPTChat
    _projects_collection_type = AsyncChatGPTProjectsCollection

    capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.DELETE,
            Capability.PROJECT_DELETE,
            Capability.ATTACHMENTS,
            Capability.TEMPORARY,
            Capability.HISTORY,
            Capability.RENAME,
            Capability.AUTH,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.RESOURCES,
            Capability.PROJECT_MEMBERSHIP,
            Capability.MODELS,
            Capability.EFFORT,
            Capability.MESSAGE_EDIT,
            Capability.MESSAGE_RETRY,
            Capability.CHAT_BRANCH,
        }
    )

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
        self.session_state = session or BrowserSession(SessionPaths(account.store()))
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

    def _get_project_editor(self) -> ChatGPTProjectEditor:
        return self.editor

    def _get_project_gate(self) -> asyncio.Lock:
        return self._gate

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
                await _maybe_await(close())

    async def _discard_client(self) -> None:
        client, self._client = self._client, None
        if client is None:
            return
        close = getattr(client, "close", None)
        if callable(close):
            await _maybe_await(close())

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

    def _project(self, raw: dict[str, Any]) -> ProjectData:
        return ProjectData(
            raw["id"],
            raw["name"],
            raw.get("instructions"),
            tuple(
                Attachment(
                    str(item.get("file_id") or item.get("id") or ""),
                    str(item.get("name") or ""),
                    item.get("type"),
                    metadata=dict(item),
                )
                for item in raw.get("resources", [])
            ),
            dict(raw.get("metadata") or {}),
        )

    def _message(self, item: dict[str, Any], raw: dict[str, Any]) -> MessageData:
        content = item.get("content") or {}
        parts = message_parts(content)
        parts = parts if isinstance(parts, list) else []
        author = item.get("author")
        author = author if isinstance(author, dict) else {}
        role = author.get("role", "unknown")
        blocks = tuple(
            Content("text", text=part)
            if isinstance(part, str)
            else Content("text", text=part["text"], metadata={"native": part})
            if isinstance(part, dict) and isinstance(part.get("text"), str)
            else Content("provider", metadata={"value": part})
            for part in parts
        )
        return MessageData(
            str(item.get("id", "")),
            role,
            blocks,
            "analysis"
            if content.get("content_type") in ("thoughts", "reasoning_recap", "reasoning_summary")
            or (role == "assistant" and item.get("recipient") not in (None, "all"))
            else item.get("channel"),
            dict(item),
            actions=actions_for(
                role,
                final=item.get("channel") != "analysis"
                and content.get("content_type")
                not in ("thoughts", "reasoning_recap", "reasoning_summary")
                and item.get("recipient") in (None, "all"),
                branch=True,
            )
            if item.get("id")
            and not raw.get("pending_branch")
            and not raw.get("is_read_only")
            and not raw.get("is_temporary_chat")
            else (),
        )

    @staticmethod
    def _active_messages(raw: dict[str, Any]) -> list[dict[str, Any]]:
        mapping = raw.get("mapping")
        if not isinstance(mapping, dict) or not mapping:
            messages = raw.get("messages")
            return messages if isinstance(messages, list) else []
        node_id = raw.get("current_node")
        if not isinstance(node_id, str) or node_id not in mapping:
            raise ValueError("ChatGPT conversation has no active message path")
        reversed_messages: list[dict[str, Any]] = []
        visited: set[str] = set()
        while node_id is not None:
            if node_id in visited:
                raise ValueError("ChatGPT conversation message path contains a cycle")
            visited.add(node_id)
            node = mapping.get(node_id)
            if not isinstance(node, dict):
                raise ValueError("ChatGPT conversation message path is incomplete")
            message = node.get("message")
            if isinstance(message, dict):
                reversed_messages.append(message)
            parent_id = node.get("parent")
            if parent_id is None:
                break
            if not isinstance(parent_id, str) or parent_id not in mapping:
                raise ValueError("ChatGPT conversation message path is incomplete")
            node_id = parent_id
        reversed_messages.reverse()
        return reversed_messages

    def _conversation(self, raw: dict[str, Any]) -> Conversation:
        remote_id = raw.get("conversation_id") or raw.get("id")
        if not isinstance(remote_id, str) or not remote_id:
            raise ValueError("provider conversation has no remote ID")
        messages, turn = [], []
        for item in self._active_messages(raw):
            if not isinstance(item, dict):
                continue
            if (item.get("author") or {}).get("role") == "user":
                turn.clear()
            else:
                turn.append(item)
            message = self._message(item, raw)
            if (
                message.role == "assistant"
                and message.channel != "analysis"
                and item.get("recipient") in (None, "all")
            ):
                message.metadata["files"] = message_files(*turn)
                turn.clear()
            messages.append(message)
        metadata = {key: value for key, value in raw.items() if key != "mapping"}
        latest_assistant = next(
            (message for message in reversed(messages) if message.role == "assistant"), None
        )
        if latest_assistant and latest_assistant.metadata.get("status") == "in_progress":
            generation: dict[str, Any] = {"active": True}
            started_at = latest_assistant.metadata.get("create_time")
            if (
                isinstance(started_at, (int, float))
                and not isinstance(started_at, bool)
                and 0 < started_at < 4_102_444_800
                and math.isfinite(started_at)
            ):
                generation["started_at"] = started_at
            metadata["generation"] = generation
        pid = project_id(raw)
        return Conversation(
            remote_id,
            raw.get("title") or "Untitled",
            tuple(messages),
            project=ProjectData(pid, "") if pid else None,
            metadata=metadata,
        )

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
                    stored = await read_session_async(self.session_state.paths.session_file)
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
                    await self.editor.close()
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
        saved = session_exists(self.session_state.paths.session_file)
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
                    await _maybe_await(client.verify())
            except BaseException:
                await self._discard_client()
                raise
        return SessionStatus(
            SessionState.AUTHENTICATED,
            session_exists(self.session_state.paths.session_file),
        )

    async def authenticate(self, *, capture_browser: bool = True) -> SessionStatus:
        async with self.runtime.activity():
            return await self._authenticate(capture_browser=capture_browser)

    async def _forget_session(self) -> SessionStatus:
        async with self._gate:
            await self._discard_client()
            await asyncio.to_thread(delete_session, self.session_state.paths.session_file)
            self._temporary_conversations.clear()
            self._pending_branches.clear()
        return SessionStatus(SessionState.LOGIN_REQUIRED, False, "browser_session_retained")

    async def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
        async with self._catalog_lock:
            async with self._client_scope() as client:
                return await _maybe_await(
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
                    raw = await _maybe_await(client.list_page(offset, limit))
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
            raw = await _maybe_await(client.get_conversation(conversation_id))
        return self._conversation(raw)

    async def _branch_chat_data(self, conversation_id: str, message_id: str) -> Conversation:
        async with self._gate:
            async with self._client_scope() as client:
                current = self._conversation(
                    await _maybe_await(client.get_conversation(conversation_id))
                )
                _, target = target_message(current.messages, message_id, "assistant")
                if "branch" not in target.actions:
                    raise FeatureUnavailable(Capability.CHAT_BRANCH)
                result = (
                    await _maybe_await(
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
        async with self._rename_gate:
            async with self._client_scope() as client:
                await _maybe_await(client.rename_conversation(conversation_id, title.strip()))

    async def _delete_chat_data(self, conversation_id: str) -> None:
        async with self._gate:
            async with self._client_scope() as client:
                confirmed = await _maybe_await(client.delete_conversation(conversation_id))
        if confirmed is True:
            return

        async def readback():
            async with self._client_scope() as client:
                return await _maybe_await(client.get_conversation(conversation_id))

        await confirm_deleted(readback, lambda value: value.get("is_visible") is False)

    async def _move_chat_data(self, conversation_id: str, project_id: str | None) -> None:
        async with self._rename_gate:
            async with self._client_scope() as client:
                await _maybe_await(client.move_conversation(conversation_id, project_id))

    async def _list_projects_data(self, *, cursor: str | None = None) -> Page[ProjectData]:
        await self._wait_for_prepare()
        async with self._project_read_semaphore():
            async with self._client_scope() as client:
                raw = await _maybe_await(client.list_projects(cursor))
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
        async with self._gate:
            async with self._client_scope() as client:
                raw = await _maybe_await(
                    client.create_project(name, instructions, memory_share=memory_share)
                )
        return self._project(raw)

    async def _get_project_data(self, project_id: str) -> ProjectData:
        await self._wait_for_prepare()
        async with self._client_scope() as client:
            raw = await _maybe_await(client.get_project(project_id))
        return self._project(raw)

    async def _list_project_chats_data(
        self, project_id: str, *, cursor: str | None = None
    ) -> Page[Conversation]:
        await self._wait_for_prepare()
        async with self._project_read_semaphore():
            async with self._client_scope() as client:
                raw = await _maybe_await(client.list_project_chats(project_id, cursor))
        return Page(
            tuple(self._conversation(item) for item in raw["items"]),
            raw.get("cursor"),
        )

    async def _delete_project_data(self, project_id: str) -> None:
        async with self._gate:
            async with self._client_scope() as client:
                url = "https://chatgpt.com/backend-api/gizmos/" + quote(project_id, safe="")
                for attempt in (0, 1):
                    try:
                        response = await _maybe_await(
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
                    attachments = await _prepare_chatgpt_attachments(client, request)
                    remote_id, completed, previous = await _maybe_await(
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
                    _emit_chatgpt_post_send_events(
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
        gate_wait_started = time.perf_counter_ns() if timing is not None else None
        async with self._gate:
            if gate_wait_started is not None:
                timing.record(
                    "provider_send_gate_wait_ms", time.perf_counter_ns() - gate_wait_started
                )
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
            return await _maybe_await(
                client.upload_file(path_obj.name, mime, path_obj, cancel_event=cancel_event)
            )
        except asyncio.CancelledError:
            close = getattr(client, "close", None)
            if callable(close):

                async def cleanup():
                    with suppress(Exception):
                        await _maybe_await(close())

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
                await _maybe_await(close())

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
        return await _maybe_await(client.get_conversation(conversation_id))

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
                    await _maybe_await(
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
                await _maybe_await(
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

    async def move_project_chat(
        self, chat: AsyncChat | str, project: AsyncProject | str | None
    ) -> AsyncChat:
        """Move a ChatGPT chat that already belongs to a project into another project or detach it."""
        target_chat = (
            chat
            if isinstance(chat, AsyncChat)
            else await self.chats.get(chat, include_projects=True)
        )
        if target_chat.provider is not self:
            raise ValueError("Chat must belong to this ChatGPT provider session")
        if target_chat.project is None:
            raise UnsupportedOperationError(
                "Chat does not belong to a project; use chat.move_to(project) instead"
            )
        target_project = _resolve_project_arg(self, project)
        if target_project is not None and target_chat.remote_id and not target_project.remote_id:
            raise UnsupportedOperationError(
                "A remote chat cannot be moved into a project that is not persisted remotely"
            )
        target_id = (
            (target_project.remote_id or target_project.id) if target_project is not None else None
        )
        runtime = self.runtime
        context = runtime.new_context(
            "chat.move",
            resource_id=target_chat.remote_id or target_chat.id,
            parameters={"project_id": target_id},
        )

        async def work(ctx) -> AsyncChat:
            if target_chat.remote_id:
                async with runtime.activity():
                    self._require_provider()
                    await self._move_chat_data(target_chat.remote_id, ctx.parameters["project_id"])
            target_chat.project = target_project
            return target_chat

        async def on_success(ctx) -> None:
            if target_chat.remote_id and not target_chat.temporary:
                await _publish_collection_change(
                    runtime, "chats", "update", target_chat, operation_id=ctx.operation_id
                )
            await runtime.emit(
                "chat.moved",
                operation_id=ctx.operation_id,
                resource_id=target_chat.remote_id or target_chat.id,
                payload={"project_id": target_id},
            )

        return await execute_with_hooks(
            operation_name="chat.move",
            context=context,
            hooks_chain=(runtime.hooks, self.chats.hooks),
            work=work,
            on_success_event=on_success,
        )


class ChatGPTChat(BranchableChat, MovableChat, Chat):
    pass


class ChatGPTProjectsCollection(ProjectsCollection):
    def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
        memory_share: bool = True,
    ) -> Project:
        collection = cast(AsyncChatGPTProjectsCollection, self._collection)
        project = self._owner._call(
            collection.create(title, instruction=instruction, memory_share=memory_share)
        )
        return self._wrap(project)


class ChatGPTProvider(Provider):
    provider_type = ProviderType.CHATGPT
    async_provider_type = AsyncChatGPTProvider
    chat_wrapper_type = ChatGPTChat
    projects_collection_type = ChatGPTProjectsCollection

    def move_project_chat(
        self,
        chat: Chat | AsyncChat | str,
        project: Project | AsyncProject | str | None,
    ) -> Chat:
        """Move a ChatGPT chat that already belongs to a project into another project or detach it."""
        raw_chat = chat._resource if isinstance(chat, Chat) else chat
        raw_project = project._resource if isinstance(project, Project) else project
        updated = self._call(
            cast(AsyncChatGPTProvider, self._provider).move_project_chat(raw_chat, raw_project)
        )
        return self._wrap_chat(updated)
