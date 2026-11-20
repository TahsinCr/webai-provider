from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from ...accounts import delete_session, session_exists
from ...accounts.status import SessionState, SessionStatus
from ...async_provider import AsyncProvider
from ...base import ProviderType
from ...collections import Page
from ...core.cache import BoundedCache
from ...core.concurrency import gather_bounded
from ...errors import AccountPaused, ProviderError, RateLimited
from ...resources.chat.collections import AsyncChatsCollection
from ...resources.chat.types import Conversation
from ...resources.message.protocol import (
    ProviderRequest,
    ProviderStreamEvent,
)
from ...resources.message.types import MessageAction
from ...resources.model.model import ModelCatalog
from ...resources.project.collections import AsyncProjectsCollection
from ...resources.project.mutations import ProjectMutationsMixin
from ...resources.project.types import ProjectData
from ...transport import AsyncTransport
from .base import (
    BaseGrokProvider,
    _grok_attachment_entry,
    _inherit_grok_edit_attachments,
    _validate_grok_request,
    _validated_grok_attachment,
)
from .catalogs import parse_model_catalog
from .client import GrokClient, remote_id
from .projects import GrokProjectEditor
from .records import active_responses, conversation, project, response_events
from .resources import (
    AsyncGrokChat,
    AsyncGrokMessage,
    AsyncGrokProject,
    GrokArtifact,
    GrokAttachment,
    GrokResponse,
    GrokSource,
)

if TYPE_CHECKING:
    from ...accounts.account import Account

_CATALOG_TTL = 300.0
_CONTINUATION_TTL = 1800.0
_CONTINUATION_LIMIT = 128


class AsyncGrokProvider(
    ProjectMutationsMixin,
    AsyncProvider[AsyncChatsCollection[AsyncGrokChat], AsyncProjectsCollection[AsyncGrokProject]],
    BaseGrokProvider,
):
    _message_type = AsyncGrokMessage
    _response_type = GrokResponse
    _source_type = GrokSource
    _artifact_type = GrokArtifact
    _attachment_type = GrokAttachment

    _chat_page_size = 100

    def _make_chats(self) -> AsyncChatsCollection[AsyncGrokChat]:
        return AsyncChatsCollection(self, AsyncGrokChat)

    def _make_projects(self) -> AsyncProjectsCollection[AsyncGrokProject]:
        return AsyncProjectsCollection(self, AsyncGrokProject)

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        client_factory: Any = None,
        transport: AsyncTransport | None = None,
    ) -> None:
        self._client_factory = client_factory
        super().__init__(account, type=type, transport=transport)
        self.credential_file = account.store()
        self.client = GrokClient(
            self.credential_file,
            client_factory=client_factory,
            transport=transport,
        )
        self.editor = GrokProjectEditor(self.client)
        self._gate = asyncio.Lock()
        self._catalog: ModelCatalog | None = None
        self._catalog_at = 0.0
        self._continuations = BoundedCache(
            max_entries=_CONTINUATION_LIMIT,
            ttl=_CONTINUATION_TTL,
        )

    def _active_transport(self) -> AsyncTransport | None:
        return self.client.http

    def _take_continuation(self, conversation_id: str) -> str | None:
        return self._continuations.remove(conversation_id)

    def _remember_continuation(self, conversation_id: str, response_id: str) -> None:
        if (
            not isinstance(conversation_id, str)
            or not isinstance(response_id, str)
            or not response_id
        ):
            return
        self._continuations.put(conversation_id, response_id)

    async def _prepare_session(self) -> None:
        _, catalog = await asyncio.gather(
            self.client.authenticate(),
            self._load_catalog(),
        )
        self._prime_models(catalog)

    async def _close_session(self) -> None:
        async with self._gate:
            self._continuations.clear()
            await self.client.close()

    async def _authenticate(self) -> SessionStatus:
        await self.client.authenticate()
        return SessionStatus(SessionState.AUTHENTICATED, True)

    def _session_status(self) -> SessionStatus:
        saved = session_exists(self.credential_file)
        if self.client.session.authenticated:
            state = SessionState.AUTHENTICATED
        elif saved:
            state = SessionState.UNKNOWN
        else:
            state = SessionState.LOGIN_REQUIRED
        return SessionStatus(state, saved)

    async def _forget_session(self) -> SessionStatus:
        async with self._gate:
            await self.client.close()
            delete_session(self.credential_file)
            self._catalog = None
            self._continuations.clear()
            self.client.session.authenticated = False
        return SessionStatus(SessionState.LOGIN_REQUIRED)

    async def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
        now = time.monotonic()
        if not force and self._catalog and now - self._catalog_at < _CATALOG_TTL:
            return self._catalog
        async with self._gate:
            now = time.monotonic()
            if not force and self._catalog and now - self._catalog_at < _CATALOG_TTL:
                return self._catalog
            data, details = await asyncio.gather(
                self.client.request("POST", "/rest/modes", json={"locale": "en"}),
                self.client.request("POST", "/rest/models", json={"locale": "en"}),
            )
            self._catalog = parse_model_catalog(data, details)
            self._catalog_at = time.monotonic()
            return self._catalog

    async def _list_chats_data(
        self, *, cursor: str | None = None, limit: int = 50
    ) -> Page[Conversation]:
        query: dict[str, int | str] = {"pageSize": max(1, min(int(limit), 100))}
        if cursor:
            query["pageToken"] = cursor
        data = await self.client.request("GET", "/rest/app-chat/conversations", params=query)
        if not isinstance(data, dict):
            raise ProviderError("Grok conversation list response contract changed")
        conversations = data.get("conversations")
        if conversations is not None and not isinstance(conversations, list):
            raise ProviderError("Grok conversation list response contract changed")
        return Page(
            tuple(conversation(item) for item in (conversations or ())),
            data.get("nextPageToken"),
        )

    async def _conversation_get(self, conversation_id: str, suffix: str = "", **params: Any):
        return await self.client.request(
            "GET",
            "/rest/app-chat/conversations/" + remote_id(conversation_id) + suffix,
            params=params,
        )

    async def _history(self, conversation_id: str):
        return await self._conversation_get(conversation_id, "/responses", includeThreads=True)

    async def _response_nodes(self, conversation_id: str):
        return await self._conversation_get(conversation_id, "/response-node", includeThreads=False)

    async def _conversation_metadata(self, conversation_id: str):
        return await self._conversation_get(conversation_id, includeWorkspaces=True)

    async def _get_chat_metadata(self, conversation_id: str) -> Conversation:
        return conversation(await self._conversation_metadata(conversation_id))

    async def _get_chat_data(self, conversation_id: str) -> Conversation:
        metadata, history = await asyncio.gather(
            self._conversation_metadata(conversation_id),
            self._history(conversation_id),
        )
        active = active_responses(history)
        chat = conversation(metadata, active_rows=active)
        inflight = history.get("inflightResponses") if isinstance(history, dict) else None
        if isinstance(inflight, list) and inflight:
            return Conversation(
                chat.id,
                chat.title,
                chat.messages,
                chat.project,
                {**chat.metadata, "generation": {"active": True}},
            )
        if active:
            self._remember_continuation(conversation_id, active[-1]["responseId"])
        return chat

    async def _conversation_attachment_count(self, conversation_id: str) -> int | None:
        del conversation_id
        return None

    async def _rename_chat_data(self, conversation_id: str, title: str) -> None:
        if not isinstance(title, str) or not 1 <= len(title.strip()) <= 200:
            raise ValueError("Conversation title must be between 1 and 200 characters")
        await self.client.request(
            "PUT",
            "/rest/app-chat/conversations/" + remote_id(conversation_id),
            json={"title": title.strip()},
        )

    async def _delete_chat_data(self, conversation_id: str) -> None:
        await self.client.request(
            "DELETE", "/rest/app-chat/conversations/" + remote_id(conversation_id)
        )
        self._continuations.remove(conversation_id)

    async def _move_chat_data(
        self, conversation_id: str, project_id: str | None, *, source_project_id: str | None
    ) -> None:
        await self.editor.move_chat(
            conversation_id,
            project_id,
            source_project_id=source_project_id,
            fetch_metadata=self._conversation_metadata,
        )

    async def _list_projects_data(self, *, cursor: str | None = None) -> Page[ProjectData]:
        query: dict[str, int | str] = {"pageSize": 50}
        if cursor:
            query["pageToken"] = cursor
        data = await self.client.request("GET", "/rest/workspaces", params=query)
        if not isinstance(data, dict):
            raise ProviderError("Grok workspace list response contract changed")
        workspaces = data.get("workspaces")
        if workspaces is not None and not isinstance(workspaces, list):
            raise ProviderError("Grok workspace list response contract changed")
        return Page(
            tuple(project(item) for item in (workspaces or ())),
            data.get("nextPageToken"),
        )

    async def _get_project_data(self, project_id: str) -> ProjectData:
        path = "/rest/workspaces/" + remote_id(project_id)
        workspace, listing = await asyncio.gather(
            self.client.request("GET", path),
            self.client.request("GET", path + "/files", params={"recursive": "true"}),
        )
        if not isinstance(listing, dict):
            raise ProviderError("Grok workspace file response contract changed")
        files = listing.get("files")
        if files is not None and not isinstance(files, list):
            raise ProviderError("Grok workspace file response contract changed")
        return project(workspace, files)

    async def _list_project_chats_data(
        self, project_id: str, *, cursor: str | None = None, limit: int | None = None
    ) -> Page[Conversation]:
        query = {"pageSize": min(limit or 50, 50), "workspaceId": project_id}
        if cursor:
            query["pageToken"] = cursor
        data = await self.client.request("GET", "/rest/app-chat/conversations", params=query)
        if not isinstance(data, dict):
            raise ProviderError("Grok project conversation response contract changed")
        conversations = data.get("conversations")
        if conversations is not None and not isinstance(conversations, list):
            raise ProviderError("Grok project conversation response contract changed")
        return Page(
            tuple(
                conversation(
                    {
                        **item,
                        "workspaceId": item.get("workspaceId") or project_id,
                    },
                )
                for item in (conversations or ())
            ),
            data.get("nextPageToken"),
        )

    async def _create_project_data(
        self,
        name: str,
        instructions: str = "",
        **options: Any,
    ) -> ProjectData:
        del options
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 200:
            raise ValueError("Project name must be between 1 and 200 characters")
        if not isinstance(instructions, str):
            raise ValueError("Project instructions must be text")
        data = await self.client.request(
            "POST",
            "/rest/workspaces",
            json={"name": name.strip(), "customPersonality": instructions or ""},
        )
        return project(data)

    async def _delete_project_data(self, project_id: str) -> None:
        await self.client.request("DELETE", "/rest/workspaces/" + remote_id(project_id))

    async def _select_model_spec(
        self, requested_model: str | None
    ) -> tuple[str, str | None, str | None]:
        catalog = await self._load_catalog()
        selected = catalog.chat_model(requested_model) if requested_model else None
        if selected is None and requested_model:
            catalog = await self._load_catalog(force=True)
            selected = catalog.chat_model(requested_model)
        if selected is None and not requested_model:
            selected = catalog.chat_model(catalog.default_model)
        if selected is None:
            raise ProviderError("Selected Grok model is unavailable for this account")
        model = selected.metadata.get("modelName")
        model_mode = selected.metadata.get("modelMode")
        if not isinstance(model_mode, str) or not model_mode:
            model_mode = None
        return selected.id, model, model_mode

    async def _prepare_attachments(
        self, prepared: list[dict], pending_uploads: list[tuple[int, dict]]
    ) -> tuple[list, list]:
        if pending_uploads:
            uploaded = await gather_bounded(
                pending_uploads,
                lambda entry: self.client.upload_file(
                    entry[1].get("path"),
                    content=entry[1].get("bytes"),
                    name=entry[1].get("name"),
                    mime_type=entry[1].get("mime_type"),
                ),
                limit=2,
            )
            for (index, _), metadata in zip(pending_uploads, uploaded, strict=True):
                prepared[index] = metadata
        file_attachments = []
        image_attachments = []
        for attachment in prepared:
            metadata, _, mime = _validated_grok_attachment(attachment)
            entry = _grok_attachment_entry(metadata)
            (image_attachments if mime.startswith("image/") else file_attachments).append(entry)
        return file_attachments, image_attachments

    async def _resolve_parent_response_id(
        self,
        request: ProviderRequest,
        file_attachments: list,
        image_attachments: list,
    ) -> str:
        conversation_id = request.conversation_id
        if not conversation_id:
            return ""
        is_retry = request.action == MessageAction.RETRY
        is_edit = request.action == MessageAction.EDIT
        if is_retry or is_edit:
            self._continuations.remove(conversation_id)
            history = await self._history(conversation_id)
        else:
            cached_parent = self._take_continuation(conversation_id)
            if cached_parent is not None:
                return cached_parent
            history = await self._response_nodes(conversation_id)
        if not isinstance(history, dict):
            raise ProviderError("Grok conversation history response contract changed")
        if not is_retry and not is_edit and not isinstance(history.get("responseNodes"), list):
            raise ProviderError("Grok conversation node response contract changed")
        if history.get("inflightResponses"):
            raise ProviderError("Grok conversation already has a response in progress")
        active = active_responses(history)
        if not (is_retry or is_edit):
            return (active[-1].get("responseId") or "") if active else ""
        target = next(
            (item for item in active if item.get("responseId") == request.message_id),
            None,
        )
        expected_senders = ("assistant", "model", "grok") if is_retry else ("human", "user")
        if target is None or str(target.get("sender", "")).lower() not in expected_senders:
            label = "retry" if is_retry else "edit"
            raise ProviderError(f"Grok {label} target is no longer on the active conversation path")
        if is_edit:
            _inherit_grok_edit_attachments(target, file_attachments, image_attachments)
        return target.get("parentResponseId") or ""

    def _raise_stream_error(self, error: object) -> None:
        code = str(error.get("code") or "").upper() if isinstance(error, dict) else ""
        msg = str(error.get("message") or "") if isinstance(error, dict) else str(error)
        if code in ("UNAUTHORIZED", "AUTH_REQUIRED"):
            self.client.session.authenticated = False
            raise AccountPaused("Grok session expired; verify the account again")
        if (
            "RATE_LIMIT" in code
            or "QUOTA" in code
            or "TOO_MANY_REQUESTS" in code
            or "rate limit" in msg.lower()
            or "too many requests" in msg.lower()
        ):
            retry_after = error.get("retryAfter") if isinstance(error, dict) else None
            try:
                delay = float(retry_after) if retry_after is not None else None
            except (ValueError, TypeError):
                delay = None
            raise RateLimited(delay)
        raise ProviderError("Grok stream reported an error; the request was not resent")

    def _stream_provider_events(
        self, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamEvent]:
        return response_events(
            request,
            prepare=self._prepare_stream,
            raise_stream_error=self._raise_stream_error,
            remember=self._remember_continuation,
        )

    async def _prepare_stream(
        self, request: ProviderRequest
    ) -> tuple[AsyncIterator[dict], str | None]:
        if any(
            isinstance(item, dict)
            and item.get("grok_file") is None
            and item.get("path") is not None
            and item.get("bytes") is None
            for item in request.attachments
        ):
            prepared, pending_uploads = await asyncio.to_thread(_validate_grok_request, request)
        else:
            prepared, pending_uploads = _validate_grok_request(request)
        selected_id, model, model_mode = await self._select_model_spec(request.model)
        file_attachments, image_attachments = await self._prepare_attachments(
            prepared, pending_uploads
        )

        is_retry = request.action == MessageAction.RETRY
        conversation_id = request.conversation_id
        parent_response_id = await self._resolve_parent_response_id(
            request, file_attachments, image_attachments
        )
        body: dict[str, object] = {
            "message": "" if is_retry else request.prompt,
            "parentResponseId": parent_response_id,
            "fileAttachments": file_attachments,
            "imageAttachments": image_attachments,
            "disableSearch": False,
            "temporary": request.temporary,
            "isRegenRequest": is_retry,
        }
        if model_mode:
            body["modelMode"] = model_mode
        if model or not model_mode:
            body["modelName"] = model or selected_id
        if request.project_id:
            body["workspaceIds"] = [request.project_id]
        path = (
            "/rest/app-chat/conversations/new"
            if not conversation_id
            else "/rest/app-chat/conversations/" + remote_id(conversation_id) + "/responses"
        )

        return self.client.events(path, body), model

    async def _upload_file(self, path: Any) -> dict[str, Any]:
        return await self.client.upload_file(path)
