from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from ...accounts import delete_session
from ...accounts.status import SessionState, SessionStatus
from ...async_provider import AsyncProvider
from ...base import ProviderType
from ...collections import Page
from ...core.cache import ContinuationCache
from ...core.concurrency import gather_bounded
from ...resources.chat.collections import AsyncChatsCollection
from ...resources.chat.types import Conversation
from ...resources.message.protocol import ProviderRequest, ProviderStreamEvent
from ...resources.model.model import ModelCatalog
from ...resources.project.collections import AsyncProjectsCollection
from ...resources.project.mutations import AsyncProjectMutationsMixin
from ...resources.project.types import ProjectData
from ...transport import AsyncTransport
from .async_client import AsyncGrokClient, AsyncGrokProjectEditor
from .base import (
    _CATALOG_TTL,
    _CONTINUATION_TTL,
    BaseGrokProvider,
    _validate_grok_request,
    remote_id,
)
from .catalogs import parse_model_catalog
from .records import async_response_events, conversation, project
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


class AsyncGrokProvider(
    AsyncProjectMutationsMixin,
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
        self.client = AsyncGrokClient(
            self.credential_file, client_factory=client_factory, transport=transport
        )
        self.editor = AsyncGrokProjectEditor(self.client)
        self._gate = asyncio.Lock()
        self._catalog: ModelCatalog | None = None
        self._catalog_at = 0.0
        self._continuations = ContinuationCache[str](ttl=_CONTINUATION_TTL)

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

    async def _forget_session(self) -> SessionStatus:
        async with self._gate:
            await self.client.close()
            delete_session(self.credential_file)
            self._catalog = None
            self._continuations.clear()
            self.client.session.authenticated = False
        return SessionStatus(SessionState.LOGIN_REQUIRED)

    async def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
        if not force and self._catalog and time.monotonic() - self._catalog_at < _CATALOG_TTL:
            return self._catalog
        async with self._gate:
            if not force and self._catalog and time.monotonic() - self._catalog_at < _CATALOG_TTL:
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
        return self._parse_chats_page(
            await self.client.request(
                "GET", "/rest/app-chat/conversations", params=self._build_chats_query(cursor, limit)
            )
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

    async def _warm_chat(self, chat: Any) -> None:
        cid = chat.remote_id
        if cid and self._continuations.get(cid) is None:
            nodes = await self._response_nodes(cid)
            if isinstance(nodes, dict) and nodes.get("inflightResponses"):
                return
            parent = self._resolve_parent_from_history(
                ProviderRequest("", conversation_id=cid), nodes, [], []
            )
            self._remember_continuation(cid, parent)

    async def _get_chat_data(self, conversation_id: str) -> Conversation:
        metadata, history = await asyncio.gather(
            self._conversation_metadata(conversation_id),
            self._history(conversation_id),
        )
        return self._build_chat_from_history(conversation_id, metadata, history)

    async def _conversation_attachment_count(self, conversation_id: str) -> int | None:
        del conversation_id
        return None

    async def _rename_chat_data(self, conversation_id: str, title: str) -> None:
        await self.client.request(
            "PUT",
            "/rest/app-chat/conversations/" + remote_id(conversation_id),
            json={"title": self._validate_chat_title(title)},
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
        return self._parse_projects_page(
            await self.client.request(
                "GET", "/rest/workspaces", params=self._build_projects_query(cursor)
            )
        )

    async def _get_project_data(self, project_id: str) -> ProjectData:
        path = "/rest/workspaces/" + remote_id(project_id)
        workspace, listing = await asyncio.gather(
            self.client.request("GET", path),
            self.client.request("GET", path + "/files", params={"recursive": "true"}),
        )
        return self._parse_project_detail(workspace, listing)

    async def _list_project_chats_data(
        self, project_id: str, *, cursor: str | None = None, limit: int | None = None
    ) -> Page[Conversation]:
        return self._parse_project_chats_page(
            project_id,
            await self.client.request(
                "GET",
                "/rest/app-chat/conversations",
                params=self._build_project_chats_query(project_id, cursor, limit),
            ),
        )

    async def _create_project_data(
        self, name: str, instructions: str = "", **options: Any
    ) -> ProjectData:
        del options
        data = await self.client.request(
            "POST", "/rest/workspaces", json=self._validate_create_project(name, instructions)
        )
        return project(data)

    async def _delete_project_data(self, project_id: str) -> None:
        await self.client.request("DELETE", "/rest/workspaces/" + remote_id(project_id))

    async def _select_model_spec(
        self, requested_model: str | None
    ) -> tuple[str, str | None, str | None]:
        catalog = await self._load_catalog()
        if requested_model and catalog.chat_model(requested_model) is None:
            catalog = await self._load_catalog(force=True)
        return self._extract_model_spec(catalog, requested_model)

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
        return self._split_attachments(prepared)

    async def _resolve_parent_response_id(
        self, request: ProviderRequest, file_attachments: list, image_attachments: list
    ) -> str:
        conversation_id = request.conversation_id
        if not conversation_id:
            return ""
        cached_parent, needs_history = self._send_continuation(request, conversation_id)
        if cached_parent is not None:
            return cached_parent
        history = (
            await self._history(conversation_id)
            if needs_history
            else await self._response_nodes(conversation_id)
        )
        return self._resolve_parent_from_history(
            request, history, file_attachments, image_attachments
        )

    def _stream_provider_events(
        self, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamEvent]:
        return async_response_events(
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
        parent_response_id = await self._resolve_parent_response_id(
            request, file_attachments, image_attachments
        )
        path, body = self._build_stream_payload(
            request,
            selected_id=selected_id,
            model=model,
            model_mode=model_mode,
            parent_response_id=parent_response_id,
            file_attachments=file_attachments,
            image_attachments=image_attachments,
        )
        return self.client.events(path, body), model

    async def _upload_file(self, path: Any) -> dict[str, Any]:
        return await self.client.upload_file(path)
