"""ChatGPT provider built on its persistent web client."""

from __future__ import annotations

import asyncio
import concurrent.futures
import math
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, cast

from ...accounts import delete_session, session_exists
from ...accounts.account import Account
from ...accounts.status import (
    SessionState,
    SessionStatus,
)
from ...async_provider import AsyncProvider, confirm_deleted
from ...base import Capability
from ...collections import Page
from ...errors import (
    FeatureUnavailable,
    ProviderError,
    RateLimited,
    UnsupportedOperationError,
)
from ...normalizers import sources_from
from ...provider import Provider, ProviderType
from ...resources.chat.chat import (
    AsyncChat,
    BranchableChat,
    Chat,
    MovableChat,
    _resolve_project_arg,
)
from ...resources.chat.types import Conversation
from ...resources.common import (
    Origin,
    ResourceData,
)
from ...resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    ProviderRequest,
    ResponseComplete,
    TextDelta,
    TextReplacement,
)
from ...resources.message.types import (
    Content,
    MessageData,
    actions_for,
)
from ...resources.model.model import ModelCatalog
from ...resources.project.collections import ProjectsCollection
from ...resources.project.project import AsyncProject, Project
from ...resources.project.types import ProjectData
from ...transport import AsyncTransport, PortalTransport, ThreadedTransport
from ...utils.timing import current_send_timing
from .client import ChatGPTClient
from .collections import AsyncChatGPTProjectsCollection
from .discovery import project_id
from .errors import ChatGPTError
from .formats import message_files, message_parts
from .projects import ChatGPTProjectEditor
from .resources import AsyncChatGPTChat
from .session import BrowserSession, SessionPaths


class _Stopped(Exception):
    pass


class AsyncChatGPTProvider(AsyncProvider):
    provider_type = ProviderType.CHATGPT

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
        client_factory: Callable = ChatGPTClient,
        transport: AsyncTransport | None = None,
    ) -> None:
        super().__init__(account, type=type, transport=transport)
        self._provided_transport = transport
        if transport is not None and transport.origin != "https://chatgpt.com":
            raise ValueError("ChatGPT transport must use the ChatGPT origin")
        self._transport = transport or ThreadedTransport(
            "https://chatgpt.com", self._transport_scope
        )
        self.session = session or BrowserSession(SessionPaths(account.store()))
        self._factory = client_factory
        self._persistent_client = client_factory is ChatGPTClient
        self._http_client = None
        self._http_lock = threading.RLock()
        self._temporary_conversations = OrderedDict()
        self._gate = asyncio.Lock()
        self._rename_gate = asyncio.Lock()
        self._history_list_gate = asyncio.Lock()
        self._history_list_next_request = 0.0
        self.editor = ChatGPTProjectEditor(
            self.session, self._factory, client_scope=self._client_scope
        )

    @contextmanager
    def _client_scope(self):
        """Reuse the production HTTP pool while retaining injectable test clients."""
        if self._persistent_client:
            with self._http_lock:
                if self._http_client is None:
                    options = {}
                    if self._provided_transport is not None:
                        options["transport"] = PortalTransport(
                            self._call_transport, lambda: self._provided_transport
                        )
                    self._http_client = self._factory(self.session, **options)
                    self._http_client.temporary_conversations = self._temporary_conversations
                yield self._http_client
            return
        with self._factory(self.session) as client:
            yield client

    @contextmanager
    def _transport_scope(self):
        with self._client_scope() as client:
            yield client.transport

    def _call_transport(self, coroutine):
        if self._loop is None or not self._loop.is_running():
            coroutine.close()
            raise ProviderError("Open the provider before using its transport")
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result()

    async def _discard_http_client(self):
        def discard():
            with self._http_lock:
                client, self._http_client = self._http_client, None
                if client is not None:
                    client.close()

        await asyncio.to_thread(discard)

    async def upload_file(self, path, *, cancel_event=None) -> dict:
        import mimetypes
        import threading
        from pathlib import Path

        path = Path(path)
        mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        cancelled = cancel_event or threading.Event()
        holder = []

        def upload():
            with self._factory(self.session) as client:
                holder.append(client)
                try:
                    return client.upload_file(path.name, mime, path, cancel_event=cancelled)
                except Exception:
                    if cancelled.is_set():
                        return None
                    raise

        worker = asyncio.create_task(asyncio.to_thread(upload))

        def consume_failure(task):
            if not task.cancelled():
                task.exception()

        worker.add_done_callback(consume_failure)
        waiter = asyncio.shield(worker)
        waiter.add_done_callback(consume_failure)
        try:
            result = await waiter
            if result is None:
                raise asyncio.CancelledError
            return result
        except asyncio.CancelledError:
            cancelled.set()
            # This dedicated client only serves this upload. Closing its transport
            # interrupts an in-flight PUT; chunk checks stop the worker if it is
            # between writes. Wait for the worker so cancellation cannot leak it.
            if holder:
                try:
                    holder[0].close()
                except Exception:
                    pass
            try:
                cleanup = asyncio.shield(worker)
                cleanup.add_done_callback(consume_failure)
                await cleanup
            except BaseException:
                pass
            raise

    async def _close_provider(self):
        try:
            async with self._gate:
                async with self._rename_gate:
                    try:
                        await self.editor.close()
                    finally:
                        self._temporary_conversations.clear()
                        await self._discard_http_client()
        finally:
            if self._provided_transport is not None:
                await self._provided_transport.close()

    async def session_status(self) -> SessionStatus:
        saved = session_exists(self.session.paths.session_file)
        return SessionStatus(
            SessionState.UNKNOWN if saved else SessionState.LOGIN_REQUIRED,
            saved,
            "snapshot_not_verified" if saved else "no_saved_session",
        )

    async def login(self) -> SessionStatus:
        async with self._gate:
            await asyncio.to_thread(self.session.launch)
        return SessionStatus(
            SessionState.UNKNOWN,
            session_exists(self.session.paths.session_file),
            "complete_login_in_browser_then_authenticate",
        )

    async def authenticate(self, *, capture_browser: bool = True) -> SessionStatus:
        def verify():
            if capture_browser and self.session.is_ready():
                url, headers = self.session.capture_auth()
                self.session.save(url, headers)
            with self._factory(self.session) as client:
                client.verify()

        async with self._gate:
            await self._discard_http_client()
            await asyncio.to_thread(verify)
        return SessionStatus(
            SessionState.AUTHENTICATED, session_exists(self.session.paths.session_file)
        )

    async def forget_session(self) -> SessionStatus:
        async with self._gate:
            await self._discard_http_client()
            await asyncio.to_thread(delete_session, self.session.paths.session_file)
            self._temporary_conversations.clear()
        return SessionStatus(SessionState.LOGIN_REQUIRED, False, "browser_session_retained")

    async def forget_temporary(self, remote_id: str) -> None:
        self._temporary_conversations.pop(remote_id, None)

    async def forget_conversation(self, conversation_id: str) -> None:
        async with self._gate:
            with self._http_lock:
                if self._http_client is not None:
                    self._http_client.forget_conversation(conversation_id)

    async def prepare(self) -> None:
        """Bootstrap cookie credentials, then warm and retain the authenticated client."""
        if self._factory is ChatGPTClient:
            from ...accounts.credentials import read_session
            from ...accounts.filters import ProviderCookieFilter
            from .errors import ChatGPTAuthenticationError

            try:
                self.session.load()
            except ChatGPTAuthenticationError:
                stored = read_session(self.session.paths.session_file)
                cookies = stored.get("cookies", stored)
                if not isinstance(cookies, dict):
                    raise
                cookies = ProviderCookieFilter.filter_cookies("https://chatgpt.com", cookies)
                await asyncio.to_thread(self.session.import_cookies, cookies)
        await self._load_model_catalog()

    async def prepare_for_send(self) -> None:
        """Start the existing Sentinel prewarm for the selected account."""

        def prewarm():
            with self._client_scope() as client:
                client.trigger_sentinel_prewarm()

        async with self._gate:
            await asyncio.to_thread(prewarm)

    async def _load_model_catalog(self, *, force: bool = False) -> ModelCatalog:
        def read():
            with self._client_scope() as client:
                return client.model_catalog(refresh=True) if force else client.model_catalog()

        async with self._gate:
            return await asyncio.to_thread(read)

    def _project(self, raw: dict) -> ProjectData:
        return ProjectData(
            Origin(self.id, self.account_id, raw["id"]),
            raw["name"],
            raw.get("instructions"),
            tuple(
                ResourceData(
                    str(item.get("file_id") or item.get("id") or ""),
                    str(item.get("name") or ""),
                    item.get("type"),
                    metadata=dict(item),
                )
                for item in raw.get("resources", [])
            ),
            dict(raw.get("metadata") or {}),
        )

    async def delete_chat(self, conversation_id):
        async with self._gate:

            def remove():
                with self._client_scope() as client:
                    client.delete_conversation(conversation_id)

            await asyncio.to_thread(remove)

            async def readback():
                def read():
                    with self._client_scope() as client:
                        return client.get_conversation(conversation_id)

                return await asyncio.to_thread(read)

            await confirm_deleted(readback, lambda value: value.get("is_visible") is False)

    async def delete_project(self, project_id):
        from urllib.parse import quote

        def remove():
            with self._client_scope() as client:
                client.request(
                    "DELETE",
                    "https://chatgpt.com/backend-api/gizmos/" + quote(project_id, safe=""),
                    expect_json=False,
                )

        async with self._gate:
            await asyncio.to_thread(remove)
        await confirm_deleted(lambda: self.get_project(project_id))

    async def list_projects(self, cursor: str | None = None) -> Page[ProjectData]:
        def read():
            with self._client_scope() as client:
                return client.list_projects(cursor)

        async with self._gate:
            raw = await asyncio.to_thread(read)
        return Page(tuple(self._project(item) for item in raw["items"]), raw.get("cursor"))

    async def create_project(
        self,
        name: str,
        instructions: str = "",
        *,
        memory_share: bool = True,
    ) -> ProjectData:
        if type(memory_share) is not bool:
            raise TypeError("memory_share must be a bool")

        def create():
            with self._client_scope() as client:
                return client.create_project(name, instructions, memory_share=memory_share)

        async with self._gate:
            return self._project(await asyncio.to_thread(create))

    async def get_project(self, project_id: str) -> ProjectData:
        def read():
            with self._client_scope() as client:
                return client.get_project(project_id)

        async with self._gate:
            return self._project(await asyncio.to_thread(read))

    async def list_project_chats(
        self, project_id: str, cursor: str | None = None
    ) -> Page[Conversation]:
        def read():
            with self._client_scope() as client:
                return client.list_project_chats(project_id, cursor)

        async with self._gate:
            raw = await asyncio.to_thread(read)
        return Page(tuple(self._conversation(item) for item in raw["items"]), raw.get("cursor"))

    def _message(self, item: dict, raw: dict) -> MessageData:
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

    def _conversation(self, raw: dict) -> Conversation:
        remote_id = raw.get("conversation_id") or raw.get("id")
        if not isinstance(remote_id, str) or not remote_id:
            raise ValueError("provider conversation has no remote ID")
        messages, turn = [], []
        raw_messages = raw.get("messages")
        if not isinstance(raw_messages, list):
            mapping = raw.get("mapping")
            raw_messages = []
            if isinstance(mapping, dict) and mapping:
                node_id = raw.get("current_node")
                if not isinstance(node_id, str) or node_id not in mapping:
                    raise ValueError("ChatGPT conversation has no active message path")
                reversed_messages = []
                visited = set()
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
                raw_messages = list(reversed(reversed_messages))
        for item in raw_messages:
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
        # Mapping responses can be much larger than the active branch. Keep
        # only normalized messages in conversation metadata and the archive.
        metadata = {key: value for key, value in raw.items() if key != "mapping"}
        latest_assistant = next(
            (message for message in reversed(messages) if message.role == "assistant"), None
        )
        if latest_assistant and latest_assistant.metadata.get("status") == "in_progress":
            generation = {"active": True}
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
            Origin(self.id, self.account_id, remote_id),
            raw.get("title") or "Untitled",
            tuple(messages),
            project=ProjectData(Origin(self.id, self.account_id, pid), "") if pid else None,
            metadata=metadata,
        )

    async def list_chats(self, cursor: str | None = None, limit: int = 28) -> Page[Conversation]:
        offset = int(cursor or "0")
        if offset < 0 or not 1 <= limit <= 100:
            raise ValueError("invalid conversation page")

        def read():
            with self._client_scope() as client:
                return client.list_page(offset, limit)

        loop = asyncio.get_running_loop()
        async with self._history_list_gate:
            delay = self._history_list_next_request - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                async with self._gate:
                    raw = await asyncio.to_thread(read)
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
            self._history_list_next_request = loop.time() + 0.75
        items = raw.get("items", [])
        if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
            raise ValueError("invalid conversation page")
        return Page(
            tuple(self._conversation(item) for item in items),
            str(offset + len(items)) if len(items) == limit else None,
        )

    async def get_chat(self, conversation_id: str) -> Conversation:
        def read():
            with self._client_scope() as client:
                raw = client.get_conversation(conversation_id)
            # Parse large native histories on the worker that performed the HTTP
            # read. This keeps the shared provider event loop available to sends.
            return self._conversation(raw)

        async with self._gate:
            return await asyncio.to_thread(read)

    async def resolve_download(self, url, conversation_id=None, message_id=None):
        from urllib.parse import parse_qs, quote, unquote, urlsplit

        from ...utils.files import DownloadSource

        parsed = urlsplit(url)
        if parsed.scheme == "file-service":
            import re

            file_id = parsed.netloc
            if (
                not re.fullmatch(r"file[-_][A-Za-z0-9_-]+", file_id)
                or parsed.path
                or not conversation_id
            ):
                raise ValueError("File must belong to the selected conversation")

            def resolve_file():
                with self._client_scope() as client:
                    chat = client.get_conversation(conversation_id)
                    if not any(
                        file.get("url") == url
                        for item in chat.get("messages", ())
                        for file in message_files(item)
                    ):
                        raise ValueError(
                            "File was not found in the conversation record or has expired"
                        )
                    result = client.request(
                        "GET",
                        "https://chatgpt.com/backend-api/files/download/" + quote(file_id, safe=""),
                        params={"conversation_id": conversation_id, "inline": "false"},
                    ).json()
                    target = result.get("download_url") if isinstance(result, dict) else None
                    if not target or urlsplit(target).scheme != "https":
                        raise ValueError("Provider did not return a file download URL")
                    headers = (
                        dict(client.headers) if urlsplit(target).netloc == "chatgpt.com" else {}
                    )
                    return DownloadSource(target, result.get("file_name"), True, headers)

            return await asyncio.to_thread(resolve_file)
        if (
            parsed.scheme == "https"
            and parsed.netloc == "chatgpt.com"
            and parsed.path == "/backend-api/estuary/content"
        ):
            file_id = parse_qs(parsed.query).get("id", [None])[0]
            if file_id and file_id.startswith(("file-", "file_")) and conversation_id:
                return await self.resolve_download(
                    "file-service://" + file_id, conversation_id, message_id
                )

            def resolve_estuary():
                with self._client_scope() as client:
                    return DownloadSource(url, force_download=True, headers=dict(client.headers))

            return await asyncio.to_thread(resolve_estuary)
        if parsed.scheme != "sandbox":
            return None
        if not conversation_id:
            raise ValueError("Conversation ID is not available for this file yet")
        sandbox_path = unquote(parsed.path)
        if not sandbox_path.startswith("/mnt/data/"):
            raise ValueError("Unsupported conversation file path")

        def resolve():
            with self._client_scope() as client:
                chat = client.get_conversation(conversation_id)
                candidates = chat.get("messages", [])
                if not candidates and isinstance(chat.get("mapping"), dict):
                    candidates = [
                        node.get("message")
                        for node in chat["mapping"].values()
                        if isinstance(node, dict)
                    ]
                import json

                matched = [
                    item
                    for item in candidates
                    if isinstance(item, dict)
                    and sandbox_path in json.dumps(item.get("content", {}), ensure_ascii=False)
                ]
                selected = next(
                    (item for item in matched if item.get("id") == message_id),
                    matched[0] if len(matched) == 1 else None,
                )
                if selected is None or not selected.get("id"):
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
                target = result.get("download_url") if isinstance(result, dict) else None
                if not target or urlsplit(target).scheme != "https":
                    raise ValueError("Provider did not return a file download URL")
                # Estuary URLs remain authenticated ChatGPT endpoints, unlike signed CDN URLs.
                headers = dict(client.headers) if urlsplit(target).netloc == "chatgpt.com" else {}
                return DownloadSource(target, sandbox_path.rsplit("/", 1)[-1], True, headers)

        return await asyncio.to_thread(resolve)

    async def branch_chat(self, conversation_id: str, message_id: str) -> Conversation:
        from urllib.parse import quote

        from ...resources.message.types import target_message

        def branch():
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
                return self._conversation(preview)

        async with self._gate:
            return await asyncio.to_thread(branch)

    async def rename_chat(self, conversation_id: str, title: str) -> None:
        if not title.strip():
            raise ValueError("title must be nonempty")

        def rename():
            # A long-running stream owns the pooled client's scope and lock.
            # Rename uses a short-lived client so metadata changes can proceed
            # without waiting for generation to finish.
            with self._factory(self.session) as client:
                client.rename_conversation(conversation_id, title.strip())

        async with self._rename_gate:
            await asyncio.to_thread(rename)

    async def move_chat(self, conversation_id: str, project_id: str | None) -> None:
        def move():
            with self._client_scope() as client:
                client.move_conversation(conversation_id, project_id)

        async with self._gate:
            await asyncio.to_thread(move)

    async def move_project_chat(
        self, chat: AsyncChat | str, project: AsyncProject | str | None
    ) -> AsyncChat:
        """Move a ChatGPT chat that already belongs to a project into another project or detach it."""
        target_chat = (
            chat
            if isinstance(chat, AsyncChat)
            else await self.chats.get(chat, include_projects=True)
        )
        if target_chat.session is not self:
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
        if target_chat.remote_id:
            await self.move_chat(target_chat.remote_id, target_id)
        target_chat.project = target_project
        return target_chat

    async def _stream_request(self, request: ProviderRequest):
        loop = asyncio.get_running_loop()
        queue = asyncio.Queue(maxsize=32)
        stopped = threading.Event()
        holder = []

        def publish(item):
            if stopped.is_set():
                raise _Stopped()
            future = asyncio.run_coroutine_threadsafe(queue.put(item), loop)
            while True:
                try:
                    future.result(timeout=0.1)
                    return
                except concurrent.futures.TimeoutError:
                    if stopped.is_set():
                        future.cancel()
                        raise _Stopped()

        def event(value):
            kind = value.get("type")
            if kind == "delta":
                publish(("event", TextDelta(value.get("text", ""), value.get("channel"))))
            elif kind == "replace":
                publish(("event", TextReplacement(value.get("text", ""), value.get("channel"))))
            elif kind == "response_complete":
                publish(("event", ResponseComplete()))

        def work():
            try:
                timing = current_send_timing()
                scope_started = time.perf_counter_ns() if timing is not None else None
                with self._client_scope() as client:
                    if scope_started is not None:
                        timing.record(
                            "provider_client_ready_ms", time.perf_counter_ns() - scope_started
                        )
                    client.temporary_conversations = self._temporary_conversations
                    holder.append(client)
                    if stopped.is_set():
                        return
                    attachments = []
                    for item in request.attachments:
                        if stopped.is_set():
                            return
                        if isinstance(item, dict) and ("bytes" in item or "path" in item):
                            attachments.append(
                                client.upload_file(
                                    item.get("name")
                                    or (
                                        Path(item["path"]).name
                                        if item.get("path")
                                        else "attachment"
                                    ),
                                    item.get("mime_type") or "application/octet-stream",
                                    item["bytes"]
                                    if item.get("bytes") is not None
                                    else Path(item["path"]),
                                    project_id=request.project_id or None,
                                    cancel_event=stopped,
                                )
                            )
                        else:
                            attachments.append(item)
                    if stopped.is_set():
                        return
                    remote_id, completed, previous = client.send_prompt(
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
                    # Native summaries can arrive only in the completion snapshot.
                    # The common API must not drop these provider-visible blocks.
                    from .formats import message_parts, text_from_parts

                    summaries = []
                    previous = set(previous or ())
                    for item in completed.get("messages", []):
                        if item.get("id") in previous or item.get("recipient") not in (None, "all"):
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
                    final = next(
                        (
                            item
                            for item in reversed(completed.get("messages", []))
                            if (item.get("author") or {}).get("role") == "assistant"
                            and item.get("channel") != "analysis"
                            and item.get("recipient") in (None, "all")
                        ),
                        None,
                    )
                    if final:
                        response_messages = completed.get("messages", [])
                        turn_start = next(
                            (
                                index + 1
                                for index in range(len(response_messages) - 1, -1, -1)
                                if (response_messages[index].get("author") or {}).get("role")
                                == "user"
                            ),
                            0,
                        )
                        user = next(
                            (
                                item
                                for item in reversed(completed.get("messages", []))
                                if (item.get("author") or {}).get("role") == "user"
                            ),
                            {},
                        )
                        publish(
                            (
                                "event",
                                MessageMetadata(
                                    final.get("id"),
                                    user.get("id"),
                                    (final.get("metadata") or {}).get("model_slug")
                                    or request.model,
                                    (final.get("metadata") or {}).get("reasoning_effort")
                                    or request.effort,
                                    sources_from(final),
                                    files=message_files(*response_messages[turn_start:]),
                                ),
                            )
                        )
                    publish(("event", ConversationBound(remote_id)))
            except _Stopped:
                return
            except ProviderError as error:
                if not stopped.is_set():
                    publish(("error", error))
            except Exception:
                if not stopped.is_set():
                    publish(
                        (
                            "error",
                            ChatGPTError(
                                "ChatGPT request failed; remote outcome is unknown and the request was not retried"
                            ),
                        )
                    )
            finally:
                if not stopped.is_set():
                    publish(("done", None))

        timing = current_send_timing()
        gate_wait_started = time.perf_counter_ns() if timing is not None else None
        async with self._gate:
            if gate_wait_started is not None:
                timing.record(
                    "provider_send_gate_wait_ms", time.perf_counter_ns() - gate_wait_started
                )
            worker = asyncio.create_task(asyncio.to_thread(work))
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
                stopped.set()
                if holder and (not self._persistent_client or not stream_finished):
                    # Closing local transport cannot guarantee remote generation stops.
                    await asyncio.to_thread(holder[0].close)
                # Keep the account gate until the worker exits; do not overlap an ambiguous send.
                await asyncio.shield(worker)
                if self._persistent_client and not stream_finished:
                    await self._discard_http_client()


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
