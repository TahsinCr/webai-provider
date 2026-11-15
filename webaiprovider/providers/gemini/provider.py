"""Gemini web provider implementing the shared provider contract."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator
from dataclasses import replace
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from ...accounts import delete_session, session_exists
from ...accounts.account import Account
from ...accounts.credentials import read_session_async
from ...accounts.status import (
    SessionState,
    SessionStatus,
)
from ...async_provider import AsyncProvider, _provider_activity, confirm_deleted
from ...base import Capability
from ...collections import Page
from ...core import MultiDimensionBoundedCache
from ...errors import AccountPaused, FeatureUnavailable, ProviderError
from ...normalizers import files_from, sources_from
from ...provider import Provider, ProviderType
from ...resources.chat.chat import BranchableChat, Chat
from ...resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    ProviderEvent,
    ProviderRequest,
    ResponseComplete,
    TextDelta,
    TextReplacement,
)
from ...resources.message.types import MessageAction, target_message
from ...resources.model import ModelCatalogState
from ...resources.model.model import (
    Model,
    ModelCatalog,
)
from ...resources.project.types import ProjectOptions
from ...transport import AsyncTransport
from ...utils.timing import current_send_timing
from . import generation
from .errors import GeminiCompatibilityError, GeminiStreamError
from .history import history
from .media import PLACEHOLDER, resolve_image_links, valid_media_url
from .notebooks import field, notebook_id
from .resources import AsyncGeminiChat
from .rpc import nested

if TYPE_CHECKING:
    from .notebooks import Notebooks
    from .session import GeminiSession

_CHAT_METADATA_TTL_SECONDS = 60.0


class AsyncGeminiProvider(AsyncProvider):
    provider_type = ProviderType.GEMINI
    stream_idle_timeout = 120
    stream_total_timeout = 600

    _chat_type = AsyncGeminiChat

    capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.HISTORY,
            Capability.DELETE,
            Capability.AUTH,
            Capability.MODELS,
            Capability.EFFORT,
            Capability.ATTACHMENTS,
            Capability.TEMPORARY,
            Capability.MESSAGE_EDIT,
            Capability.MESSAGE_RETRY,
            Capability.CHAT_BRANCH,
            Capability.RENAME,
        }
    )

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        client_factory=None,
        notebooks: Notebooks | None = None,
        transport: AsyncTransport | None = None,
    ) -> None:
        super().__init__(account, type=type, transport=transport)
        self._provided_transport = transport
        if transport is not None and transport.origin != "https://gemini.google.com":
            raise ValueError("Gemini transport must use the Gemini origin")
        self._media = {}
        self.cookie_file = account.store()
        self._factory = client_factory
        self._client: GeminiSession | None = None
        self._cached_models: tuple[Model, ...] | None = None
        self._catalog_at = None
        self._model_gate = asyncio.Lock()
        self._gate = asyncio.Lock()
        self._connect_gate = asyncio.Lock()
        self._temporary = {}
        self._chat_metadata = MultiDimensionBoundedCache(
            max_entries=128, default_ttl=_CHAT_METADATA_TTL_SECONDS, clock=lambda: time.monotonic()
        )
        self._project_contexts: MultiDimensionBoundedCache[str, list] = MultiDimensionBoundedCache(
            max_entries=32, max_bytes=32768, default_ttl=120.0, clock=lambda: time.monotonic()
        )
        from .notebooks import Notebooks
        from .rpc import NotebookEditor, NotebookRPC

        self.rpc: NotebookRPC = NotebookRPC(self._connect)
        self.notebooks: Notebooks = notebooks or Notebooks(self.account_id)
        self.notebooks.reader = self.rpc.read
        self.editor: NotebookEditor = NotebookEditor(self.rpc, self.notebooks)
        self.capabilities = self.capabilities | {
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.PROJECT_DELETE,
            Capability.RESOURCES,
        }

    async def upload_file(self, path):
        from .generation import _gemini_upload_spec

        filename, mime, size, _content, local_path = await asyncio.to_thread(
            _gemini_upload_spec, path
        )
        return {
            "id": filename,
            "path": local_path,
            "name": filename,
            "mime_type": mime,
            "size_bytes": size,
            "deferred_upload": True,
        }

    async def _connect(self) -> GeminiSession:
        async with self._connect_gate:
            return await self._connect_once()

    async def _connect_once(self) -> GeminiSession:
        if self._client is not None:
            return self._client
        if not session_exists(self.cookie_file):
            raise AccountPaused("No Gemini session; sign in through the Accounts section")
        from .session import GeminiSession

        client = (
            GeminiSession(self.cookie_file, transport=self._provided_transport)
            if self._factory is None
            else self._factory(self.cookie_file)
        )
        await client.open()
        if (
            self._cached_models is not None
            and self._catalog_at is not None
            and (time.monotonic() - self._catalog_at < 300)
        ):
            client.models = self._cached_models
        self._client = client
        self._transport = self._provided_transport or getattr(client, "transport", None)
        return client

    async def _close_provider(self):
        async with self._gate, self._connect_gate:
            await self._close_unlocked()

    async def _close_unlocked(self):
        editor_error = None
        try:
            await self.editor.close()
        except BaseException as error:
            editor_error = error
        client = self._client
        try:
            if client is not None:
                await client.close()
        except BaseException as close_error:
            if editor_error is not None:
                raise editor_error from close_error
            raise
        else:
            if self._client is client:
                self._client = None
                if self._provided_transport is None:
                    self._transport = None
            if self._client is None:
                self._chat_metadata.clear()
                self._project_contexts.clear()
        if editor_error is not None:
            raise editor_error

    @_provider_activity
    async def session_status(self):
        return SessionStatus(
            SessionState.AUTHENTICATED
            if self._client
            else SessionState.UNKNOWN
            if session_exists(self.cookie_file)
            else SessionState.LOGIN_REQUIRED,
            session_exists(self.cookie_file),
        )

    @_provider_activity
    async def authenticate(self):
        await self._connect()
        return SessionStatus(SessionState.AUTHENTICATED, True)

    async def prepare(self):
        """Open the authenticated HTTP session and discover its initial models."""
        await self._load_model_catalog()

    @_provider_activity
    async def forget_session(self):
        async with self._gate, self._connect_gate:
            await self._close_unlocked()
            delete_session(self.cookie_file)
            self._temporary.clear()
            self._cached_models = None
            self._catalog_at = None
        return SessionStatus(SessionState.LOGIN_REQUIRED)

    async def _load_model_catalog(self, *, force: bool = False) -> ModelCatalog:
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

    @staticmethod
    def _usable_chat_metadata(metadata, conversation_id):
        return (
            isinstance(metadata, (list, tuple))
            and len(metadata) >= 3
            and metadata[0] == conversation_id
            and all(isinstance(metadata[index], str) and metadata[index] for index in (1, 2))
        )

    def _take_chat_metadata(self, conversation_id):
        return self._chat_metadata.get(conversation_id)

    def _remember_chat_metadata(self, conversation_id, metadata):
        self._chat_metadata.put(conversation_id, list(metadata))

    async def list_chats(self, cursor=None, limit=50):
        try:
            offset = int(cursor or 0)
        except (ValueError, TypeError):
            raise ValueError("invalid Gemini conversation cursor") from None
        if offset < 0 or offset > 5000 or not 1 <= limit <= 100:
            raise ValueError("invalid Gemini conversation page")
        await self._connect()
        from .history import conversations

        parts = await self.rpc.call(
            "MaZiqc", [offset + limit + 1, None, [None, None, 1, None, 1]], "/app"
        )
        chats = conversations(parts, self._origin)
        return Page(
            chats[offset : offset + limit],
            str(offset + limit) if len(chats) > offset + limit else None,
        )

    async def get_chat(self, conversation_id):
        from .history import history
        from .rpc import nested

        parts = await self.rpc.call(
            "hNvQHb", [conversation_id, 1000, None, 1, [1], [4], None, 1], "/app"
        )
        if len(parts) != 1:
            raise GeminiCompatibilityError("Gemini history response format changed")
        chat = await asyncio.to_thread(history, parts[0], self._origin, conversation_id)
        turns = nested(parts[0], 0, default=[])
        turn = turns[0] if turns else None
        metadata = [
            conversation_id,
            nested(turn, 0, 1) or "",
            nested(turn, 3, 0, 0, 0) or "",
            None,
            None,
            None,
            None,
            None,
            None,
            "",
        ]
        if conversation_id not in self._temporary and self._usable_chat_metadata(
            metadata, conversation_id
        ):
            self._remember_chat_metadata(conversation_id, metadata)
        return chat

    async def branch_chat(self, conversation_id, message_id):
        from ...resources.message.types import target_message
        from .rpc import nested

        current = await self.get_chat(conversation_id)
        _, target = target_message(current.messages, message_id, "assistant")
        async with self._gate:
            result = await self.rpc.call(
                "KDNZr", [conversation_id, target.metadata["turn_id"]], "/app"
            )
            remote = nested(result, 0, 0, 0)
            if (
                not isinstance(remote, str)
                or not remote.startswith("c_")
                or remote == conversation_id
            ):
                raise ProviderError("Gemini did not confirm a new conversation")
        chat = await self.get_chat(remote)
        title = nested(result, 0, 0, 1)
        if isinstance(title, str) and title.strip():
            from dataclasses import replace

            chat = replace(
                chat, title=title, metadata={**chat.metadata, "title_unavailable": False}
            )
        return chat

    async def delete_chat(self, conversation_id):
        async with self._gate:
            await self._connect()
            await self.rpc.call("GzXR5e", [conversation_id], "/app", allow_empty=True)
        await confirm_deleted(
            lambda: self.rpc.call(
                "hNvQHb",
                [conversation_id, 1, None, 1, [1], [4], None, 1],
                "/app",
                allow_empty=True,
            ),
            lambda parts: not parts,
        )
        self._chat_metadata.pop(conversation_id, None)

    async def rename_chat(self, conversation_id, title):
        if not isinstance(title, str) or not 1 <= len(title.strip()) <= 100:
            raise ValueError("Gemini chat title must contain 1–100 characters")
        async with self._gate:
            client = await self._connect()
            await client.rename_conversation(conversation_id, title.strip())

    async def delete_project(self, project_id):
        from .notebooks import notebook_id

        remote = notebook_id(project_id)
        async with self._gate:
            await self.editor.write("delete", remote)
        self._project_contexts.remove(remote)
        await confirm_deleted(lambda: self.notebooks.get(remote))

    async def list_projects(self, cursor=None):
        if self.notebooks is None:
            raise FeatureUnavailable(Capability.PROJECTS)
        return await self.notebooks.list(cursor)

    async def project_options(self, project_id=None):
        if not hasattr(self, "editor"):
            return ProjectOptions()
        return await super().project_options(project_id)

    async def create_project(self, name, instructions=""):
        if not hasattr(self, "editor"):
            raise FeatureUnavailable(Capability.PROJECT_CREATE)
        if not name.strip() or instructions:
            raise ValueError(
                "Provide only a name when creating a notebook; edit instructions afterward"
            )
        async with self._gate:
            remote = await self.editor.write("create", name=name.strip())
        return await self.get_project(remote)

    async def get_project(self, project_id):
        if self.notebooks is None:
            raise FeatureUnavailable(Capability.PROJECTS)
        return await self.notebooks.get(project_id)

    async def list_project_chats(self, project_id, cursor=None):
        if self.notebooks is None:
            raise FeatureUnavailable(Capability.PROJECTS)
        return await self.notebooks.chats(project_id, cursor)

    async def _stream_request(
        self, request: ProviderRequest
    ) -> AsyncGenerator[ProviderEvent, None]:
        iterator = self._stream(request)
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("Gemini streaming requires an active asyncio task")
        deadline_cancel = object()

        def cancel_at_deadline() -> None:
            task.cancel(deadline_cancel)

        timeout = loop.call_later(self.stream_total_timeout, cancel_at_deadline)
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

    def _invalidate_project_cache(self, project_id: str) -> None:
        self._project_contexts.remove(notebook_id(project_id))

    async def _project_after_write(self, project_id, confirmed):
        self._invalidate_project_cache(project_id)
        return await super()._project_after_write(project_id, confirmed)

    async def _resolve_project_context(self, project_id, timing):
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
        value = field(field(notebook.metadata.get("native"), 1), 12)
        if not isinstance(value, list) or not value or value[0] not in (1, 2):
            raise ProviderError("Gemini notebook context could not be read")
        self._project_contexts.put(project, list(value))
        return project, value

    async def _load_chat_continuation(self, request: ProviderRequest, timing):
        metadata = self._temporary.get(request.conversation_id) if request.temporary else None
        if request.temporary and request.conversation_id and metadata is None:
            raise ProviderError("Temporary Gemini context is no longer in memory")
        if metadata is not None:
            return request, metadata, None

        cached = (
            self._take_chat_metadata(request.conversation_id)
            if request.conversation_id and not request.action
            else None
        )
        if self._usable_chat_metadata(cached, request.conversation_id):
            if timing is not None:
                timing.increment("pre_send_conversation_cache_hit")
            return request, list(cached), None

        if timing is not None and request.conversation_id:
            timing.increment("pre_send_conversation_cache_miss")
        metadata = [request.conversation_id or "", "", "", None, None, None, None, None, None, ""]
        if not request.conversation_id:
            return request, metadata, None

        history_started = time.perf_counter_ns() if timing is not None else None
        try:
            parts = await self.rpc.call(
                "hNvQHb",
                [
                    request.conversation_id,
                    1000 if request.action else 1,
                    None,
                    1,
                    [1],
                    [4],
                    None,
                    1,
                ],
                "/app",
            )
        finally:
            if history_started is not None:
                timing.record(
                    "pre_send_get_conversation_ms", time.perf_counter_ns() - history_started
                )

        turns = nested(parts[0], 0, default=[])
        turn = turns[0] if turns else None
        action_files = None
        if request.action:
            if request.message_id is None:
                raise ValueError("message action requires a message ID")
            current = history(parts[0], self._origin, request.conversation_id)
            role = "user" if request.action == MessageAction.EDIT else "assistant"
            _, target = target_message(current.messages, request.message_id, role)
            if request.action.value not in target.actions:
                raise ProviderError("This Gemini message action is not available")
            selected_index = next(
                i
                for i, item in enumerate(turns)
                if nested(item, 0, 1) == target.metadata["turn_id"]
            )
            original = turns[selected_index]
            action_files = nested(original, 2, 0, 3) or []
            if not isinstance(action_files, list):
                raise ProviderError("Gemini message attachments cannot be preserved")
            if request.action == MessageAction.RETRY:
                request = replace(
                    request,
                    prompt=nested(original, 2, 0, 0) or "",
                    action=MessageAction.EDIT if selected_index > 0 else MessageAction.RETRY,
                )
            turn = turns[selected_index + 1] if selected_index + 1 < len(turns) else None
        rid = nested(turn, 0, 1)
        rcid = nested(turn, 3, 0, 0, 0)
        if turn is not None and (not rid or not rcid):
            raise GeminiCompatibilityError(
                "Could not retrieve Gemini conversation continuation IDs."
            )
        metadata[1:3] = [rid or "", rcid or ""]
        return request, metadata, action_files

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
                await self._load_model_catalog()
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

    async def _stream(self, request: ProviderRequest) -> AsyncGenerator[ProviderEvent, None]:
        client, request, metadata, context, action_files = await self._prepare_send_context(request)
        bound, visible, cid = bool(request.conversation_id), False, request.conversation_id
        final_rid = None
        raw_files: list[dict] = []
        seen_file_keys: set[tuple[str, str]] = set()
        raw_sources: list[dict] = []
        seen_source_keys: set[str] = set()
        rendered_by_channel = {"analysis": "", "final": ""}
        async for chunk in generation.generate(
            client,
            request,
            metadata,
            context,
            files=action_files,
            request_timeout=(15, self.stream_idle_timeout + 10),
        ):
            cid = chunk.get("cid") or cid
            metadata = chunk["metadata"]
            if cid and not bound and cid != request.conversation_id:
                yield ConversationBound(cid)
                bound = True
            if chunk.get("context_only"):
                continue
            images = chunk["files"]
            for image in images:
                key = (str(image["alias"]), str(image["url"]))
                if key not in seen_file_keys:
                    seen_file_keys.add(key)
                    raw_files.append(
                        {
                            "id": image["alias"],
                            "name": image["name"],
                            "remote_url": image["url"],
                            "media_type": image.get("media_type"),
                        }
                    )
                self._media[cid, image["alias"]] = image
                self._media[cid, image["url"]] = image
            while len(self._media) > 256:
                self._media.pop(next(iter(self._media)))
            chunk_sources = chunk.get("sources")
            if chunk_sources:
                for item in chunk_sources:
                    if not isinstance(item, dict):
                        continue
                    source_key = item.get("url") or item.get("title")
                    if source_key:
                        if source_key not in seen_source_keys:
                            seen_source_keys.add(source_key)
                            raw_sources.append(item)
                    elif item not in raw_sources:
                        raw_sources.append(item)
            visible = visible or bool(chunk["text"] or images)
            for channel, content_text in (
                ("analysis", chunk["thoughts"]),
                ("final", resolve_image_links(chunk["text"], images)),
            ):
                rendered = rendered_by_channel[channel]
                if not content_text or content_text == rendered:
                    continue
                if content_text.startswith(rendered):
                    yield TextDelta(content_text[len(rendered) :], channel)
                else:
                    yield TextReplacement(content_text, channel)
                rendered_by_channel[channel] = content_text
            if chunk.get("response_complete"):
                yield ResponseComplete()
            rid = metadata[1] if len(metadata) > 1 else None
            if rid:
                final_rid = rid
        if not visible or not cid:
            raise GeminiStreamError("Gemini response did not complete; the request was not retried")
        if final_rid:
            yield MessageMetadata(
                message_id=final_rid + ":model",
                user_message_id=final_rid + ":user",
                model=request.model,
                effort=request.effort,
                sources=sources_from({"sources": raw_sources}) if raw_sources else (),
                files=files_from({"files": raw_files}) if raw_files else (),
            )
        if cid and not request.temporary and self._usable_chat_metadata(metadata, cid):
            self._remember_chat_metadata(cid, metadata)
        if request.temporary:
            self._temporary[cid] = metadata
            while len(self._temporary) > 32:
                oldest = next(iter(self._temporary))
                self._temporary.pop(oldest)

    async def resolve_download(self, url, conversation_id=None, message_id=None):
        from ...utils.files import DownloadSource, NativeDownloadClient, cookie_header

        stable_reference = bool(PLACEHOLDER.fullmatch(url) or url.startswith("gemini-media:"))
        if not stable_reference and not valid_media_url(url):
            return None
        target = self._media.get((conversation_id, url))
        if stable_reference and conversation_id and conversation_id not in self._temporary:
            target = None
            chat = await self.get_chat(conversation_id)
            for message in chat.messages:
                for content in message.content:
                    resource = content.resource
                    if resource and resource.id == url:
                        target = {"url": resource.remote_url, "name": resource.name}
        if target is None and valid_media_url(url):
            target = {"url": url, "name": "image"}
        if not target or not valid_media_url(target["url"]):
            raise ValueError("The provider did not return the file URL; sync the conversation")

        headers = {"Referer": "https://gemini.google.com/"}
        # Generated media can require the owning account, through the owning HTTP session.
        # Never attach Google cookies to arbitrary user-controlled CDN subdomains.
        host = urlsplit(target["url"]).hostname
        if host in {"lh3.googleusercontent.com", "lh3-gg.googleusercontent.com"}:
            client = self._client
            values = (
                client.http.cookies.get_dict()
                if client is not None and client.http is not None
                else await read_session_async(self.cookie_file)
            )
            headers["Cookie"] = cookie_header(values, ("__Secure-1PSID", "__Secure-1PSIDTS"))
        return DownloadSource(target["url"], target["name"], True, headers, NativeDownloadClient)


class GeminiChat(BranchableChat, Chat):
    pass


class GeminiProvider(Provider):
    provider_type = ProviderType.GEMINI
    async_provider_type = AsyncGeminiProvider
    chat_wrapper_type = GeminiChat
