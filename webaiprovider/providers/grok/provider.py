from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from ...accounts import delete_session
from ...accounts.status import SessionState, SessionStatus
from ...base import ProviderType
from ...collections import Page
from ...core.cache import ContinuationCache
from ...provider import Provider
from ...resources.chat.collections import ChatsCollection
from ...resources.chat.types import Conversation
from ...resources.message.protocol import ProviderRequest, ProviderStreamEvent
from ...resources.model.model import ModelCatalog
from ...resources.project.collections import ProjectsCollection
from ...resources.project.mutations import ProjectMutationsMixin
from ...resources.project.types import ProjectData
from ...transport import Transport
from .base import (
    _CATALOG_TTL,
    _CONTINUATION_TTL,
    BaseGrokProvider,
    _validate_grok_request,
    remote_id,
)
from .catalogs import parse_model_catalog
from .client import GrokClient, GrokProjectEditor
from .records import conversation, project, response_events
from .resources import (
    GrokArtifact,
    GrokAttachment,
    GrokChat,
    GrokMessage,
    GrokProject,
    GrokResponse,
    GrokSource,
)

if TYPE_CHECKING:
    from ...accounts.account import Account


class GrokProvider(
    ProjectMutationsMixin,
    Provider[ChatsCollection[GrokChat], ProjectsCollection[GrokProject]],
    BaseGrokProvider,
):
    _message_type = GrokMessage
    _response_type = GrokResponse
    _source_type = GrokSource
    _artifact_type = GrokArtifact
    _attachment_type = GrokAttachment
    _chat_page_size = 100

    def _make_chats(self) -> ChatsCollection[GrokChat]:
        return ChatsCollection(self, GrokChat)

    def _make_projects(self) -> ProjectsCollection[GrokProject]:
        return ProjectsCollection(self, GrokProject)

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        client_factory: Any = None,
        transport: Transport | None = None,
    ) -> None:
        self._client_factory = client_factory
        super().__init__(account, type=type, transport=transport)
        self.credential_file = account.store()
        self.client = GrokClient(
            self.credential_file, client_factory=client_factory, transport=transport
        )
        self.editor = GrokProjectEditor(self.client)
        self._gate = threading.Lock()
        self._catalog: ModelCatalog | None = None
        self._catalog_at = 0.0
        self._continuations = ContinuationCache[str](ttl=_CONTINUATION_TTL)

    def _prepare_session(self) -> None:
        self.client.authenticate()
        self._prime_models(self._load_catalog())

    def _close_session(self) -> None:
        with self._gate:
            self._continuations.clear()
            self.client.close()

    def _authenticate(self) -> SessionStatus:
        self.client.authenticate()
        return SessionStatus(SessionState.AUTHENTICATED, True)

    def _forget_session(self) -> SessionStatus:
        with self._gate:
            self.client.close()
            delete_session(self.credential_file)
            self._catalog = None
            self._continuations.clear()
            self.client.session.authenticated = False
        return SessionStatus(SessionState.LOGIN_REQUIRED)

    def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
        if not force and self._catalog and time.monotonic() - self._catalog_at < _CATALOG_TTL:
            return self._catalog
        with self._gate:
            if not force and self._catalog and time.monotonic() - self._catalog_at < _CATALOG_TTL:
                return self._catalog
            data = self.client.request("POST", "/rest/modes", json={"locale": "en"})
            details = self.client.request("POST", "/rest/models", json={"locale": "en"})
            self._catalog = parse_model_catalog(data, details)
            self._catalog_at = time.monotonic()
            return self._catalog

    def _list_chats_data(self, *, cursor: str | None = None, limit: int = 50) -> Page[Conversation]:
        return self._parse_chats_page(
            self.client.request(
                "GET", "/rest/app-chat/conversations", params=self._build_chats_query(cursor, limit)
            )
        )

    def _conversation_get(self, conversation_id: str, suffix: str = "", **params: Any):
        return self.client.request(
            "GET",
            "/rest/app-chat/conversations/" + remote_id(conversation_id) + suffix,
            params=params,
        )

    def _history(self, conversation_id: str):
        return self._conversation_get(conversation_id, "/responses", includeThreads=True)

    def _response_nodes(self, conversation_id: str):
        return self._conversation_get(conversation_id, "/response-node", includeThreads=False)

    def _conversation_metadata(self, conversation_id: str):
        return self._conversation_get(conversation_id, includeWorkspaces=True)

    def _get_chat_metadata(self, conversation_id: str) -> Conversation:
        return conversation(self._conversation_metadata(conversation_id))

    def _warm_chat(self, chat: Any) -> None:
        cid = chat.remote_id
        if cid and self._continuations.get(cid) is None:
            nodes = self._response_nodes(cid)
            if isinstance(nodes, dict) and nodes.get("inflightResponses"):
                return
            parent = self._resolve_parent_from_history(
                ProviderRequest("", conversation_id=cid), nodes, [], []
            )
            self._remember_continuation(cid, parent)

    def _get_chat_data(self, conversation_id: str) -> Conversation:
        return self._build_chat_from_history(
            conversation_id,
            self._conversation_metadata(conversation_id),
            self._history(conversation_id),
        )

    def _conversation_attachment_count(self, conversation_id: str) -> int | None:
        del conversation_id
        return None

    def _rename_chat_data(self, conversation_id: str, title: str) -> None:
        self.client.request(
            "PUT",
            "/rest/app-chat/conversations/" + remote_id(conversation_id),
            json={"title": self._validate_chat_title(title)},
        )

    def _delete_chat_data(self, conversation_id: str) -> None:
        self.client.request("DELETE", "/rest/app-chat/conversations/" + remote_id(conversation_id))
        self._continuations.remove(conversation_id)

    def _move_chat_data(
        self, conversation_id: str, project_id: str | None, *, source_project_id: str | None
    ) -> None:
        self.editor.move_chat(
            conversation_id,
            project_id,
            source_project_id=source_project_id,
            fetch_metadata=self._conversation_metadata,
        )

    def _list_projects_data(self, *, cursor: str | None = None) -> Page[ProjectData]:
        return self._parse_projects_page(
            self.client.request(
                "GET", "/rest/workspaces", params=self._build_projects_query(cursor)
            )
        )

    def _get_project_data(self, project_id: str) -> ProjectData:
        path = "/rest/workspaces/" + remote_id(project_id)
        return self._parse_project_detail(
            self.client.request("GET", path),
            self.client.request("GET", path + "/files", params={"recursive": "true"}),
        )

    def _list_project_chats_data(
        self, project_id: str, *, cursor: str | None = None, limit: int | None = None
    ) -> Page[Conversation]:
        return self._parse_project_chats_page(
            project_id,
            self.client.request(
                "GET",
                "/rest/app-chat/conversations",
                params=self._build_project_chats_query(project_id, cursor, limit),
            ),
        )

    def _create_project_data(
        self, name: str, instructions: str = "", **options: Any
    ) -> ProjectData:
        del options
        return project(
            self.client.request(
                "POST", "/rest/workspaces", json=self._validate_create_project(name, instructions)
            )
        )

    def _delete_project_data(self, project_id: str) -> None:
        self.client.request("DELETE", "/rest/workspaces/" + remote_id(project_id))

    def _select_model_spec(self, requested_model: str | None) -> tuple[str, str | None, str | None]:
        catalog = self._load_catalog()
        if requested_model and catalog.chat_model(requested_model) is None:
            catalog = self._load_catalog(force=True)
        return self._extract_model_spec(catalog, requested_model)

    def _prepare_attachments(
        self, prepared: list[dict], pending_uploads: list[tuple[int, dict]]
    ) -> tuple[list, list]:
        for index, entry in pending_uploads:
            prepared[index] = self.client.upload_file(
                entry.get("path"),
                content=entry.get("bytes"),
                name=entry.get("name"),
                mime_type=entry.get("mime_type"),
            )
        return self._split_attachments(prepared)

    def _resolve_parent_response_id(
        self, request: ProviderRequest, file_attachments: list, image_attachments: list
    ) -> str:
        conversation_id = request.conversation_id
        if not conversation_id:
            return ""
        cached_parent, needs_history = self._send_continuation(request, conversation_id)
        if cached_parent is not None:
            return cached_parent
        history = (
            self._history(conversation_id)
            if needs_history
            else self._response_nodes(conversation_id)
        )
        return self._resolve_parent_from_history(
            request, history, file_attachments, image_attachments
        )

    def _stream_provider_events(self, request: ProviderRequest) -> Iterator[ProviderStreamEvent]:
        return response_events(
            request,
            prepare=self._prepare_stream,
            raise_stream_error=self._raise_stream_error,
            remember=self._remember_continuation,
        )

    def _prepare_stream(self, request: ProviderRequest) -> tuple[Iterator[dict], str | None]:
        prepared, pending_uploads = _validate_grok_request(request)
        selected_id, model, model_mode = self._select_model_spec(request.model)
        file_attachments, image_attachments = self._prepare_attachments(prepared, pending_uploads)
        parent_response_id = self._resolve_parent_response_id(
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

    def _upload_file(self, path: Any) -> dict[str, Any]:
        return self.client.upload_file(path)
