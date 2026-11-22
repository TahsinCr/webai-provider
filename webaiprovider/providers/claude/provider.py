from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import closing
from typing import TYPE_CHECKING, Any

from ...accounts import read_session
from ...accounts.status import SessionState, SessionStatus
from ...base import ProviderType
from ...collections import Page
from ...provider import Provider
from ...resources.chat.collections import ChatsCollection
from ...resources.chat.types import Conversation
from ...resources.message.protocol import ProviderRequest, ProviderStreamEvent
from ...resources.model.model import ModelCatalog
from ...resources.project.collections import ProjectsCollection
from ...resources.project.mutations import ProjectMutationsMixin
from ...resources.project.types import ProjectData
from ...transport import Transport
from .base import BaseClaudeProvider, remote_id
from .client import ClaudeClient, ClaudeProjectEditor, iter_stream_response
from .records import conversation, conversation_attachment_count, project
from .resources import (
    ClaudeArtifact,
    ClaudeAttachment,
    ClaudeChat,
    ClaudeMessage,
    ClaudeProject,
    ClaudeResponse,
    ClaudeSource,
)

if TYPE_CHECKING:
    from ...accounts.account import Account


class ClaudeProvider(
    ProjectMutationsMixin,
    Provider[ChatsCollection[ClaudeChat], ProjectsCollection[ClaudeProject]],
    BaseClaudeProvider,
):
    _message_type = ClaudeMessage
    _response_type = ClaudeResponse
    _source_type = ClaudeSource
    _artifact_type = ClaudeArtifact
    _attachment_type = ClaudeAttachment

    def _make_chats(self) -> ChatsCollection[ClaudeChat]:
        return ChatsCollection(self, ClaudeChat)

    def _make_projects(self) -> ProjectsCollection[ClaudeProject]:
        return ProjectsCollection(self, ClaudeProject)

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        transport: Transport | None = None,
    ) -> None:
        super().__init__(account, type=type, transport=transport)
        self.credential_file = account.store()
        self.client = ClaudeClient(self.credential_file, transport=transport)
        self.editor = ClaudeProjectEditor(self.client)
        self._gate = threading.Lock()
        self._rename_gate = threading.Lock()

    def purge_credentials(self) -> SessionStatus:
        with self._gate:
            return self._purge_credentials_unlocked()

    def _prepare_session(self) -> None:
        import time

        from ...resources.model.collections import MODEL_CATALOG_TTL

        cached = self.account._get_verified_session(
            self._account_fingerprint(), ttl=MODEL_CATALOG_TTL
        )
        if isinstance(cached, tuple) and len(cached) == 2:
            organization, catalog = cached
            if isinstance(organization, str) and isinstance(catalog, ModelCatalog):
                self.client.organization = organization
                self.client.catalog = catalog
                self.client.authenticated = True
                self.client._discovered_at = time.monotonic()
                self._prime_models(catalog)
                return
        catalog = self.client.prepare()
        self._prime_models(catalog)
        if self.client.organization:
            self.account._set_verified_session(
                self._account_fingerprint(), (self.client.organization, catalog)
            )

    def _close_session(self) -> None:
        with self._rename_gate:
            self.client.close()

    def _authenticate(self) -> SessionStatus:
        with self._gate:
            self.client.request("status")
        return SessionStatus(SessionState.AUTHENTICATED, True)

    def forget_session(self) -> SessionStatus:
        self.close()
        return self.purge_credentials()

    def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
        return self.client.request("models", force=force)

    def _list_chats_data(self, *, cursor: str | None = None, limit: int = 50) -> Page[Conversation]:
        return self._parse_chats_page(
            self.client.request("chats", cursor=cursor, limit=limit), cursor, limit
        )

    def _warm_chat(self, chat: Any) -> None:
        cid = chat.remote_id
        options = {"temporary": chat.temporary}
        if cid and not self.client._lookup_cached_continuation(cid, options, None)[0]:
            self.client.request("continuation", conversation_id=cid, **options)

    def _get_chat_data(self, conversation_id: str) -> Conversation:
        return conversation(self.client.request("history", conversation_id=conversation_id))

    def _conversation_attachment_count(self, conversation_id: str) -> int | None:
        return conversation_attachment_count(
            self.client.request("history", conversation_id=conversation_id)
        )

    def _rename_chat_data(self, conversation_id: str, title: str) -> None:
        self.client.request(
            "rename_chat",
            conversation_id=conversation_id,
            title=self._validate_chat_title(title),
        )

    def _delete_chat_data(self, conversation_id: str) -> None:
        self.client.request("delete_chat", conversation_id=conversation_id)

    def _move_chat_data(
        self, conversation_id: str, project_id: str | None, *, source_project_id: str | None
    ) -> None:
        del source_project_id
        self.client.request("move_chat", conversation_id=conversation_id, project_id=project_id)

    def _list_projects_data(self, *, cursor: str | None = None) -> Page[ProjectData]:
        if cursor:
            raise ValueError("Claude project listing does not use a cursor")
        return self._parse_projects_page(self.client.request("projects"))

    def _get_project_data(self, project_id: str) -> ProjectData:
        return project(self.client.request("project", project_id=project_id))

    def _list_project_chats_data(
        self, project_id: str, *, cursor: str | None = None, limit: int | None = None
    ) -> Page[Conversation]:
        del limit
        if cursor:
            raise ValueError("Claude project conversation listing does not use a cursor")
        return self._parse_project_chats_page(
            project_id, self.client.request("project_chats", project_id=project_id)
        )

    def _create_project_data(
        self, name: str, instructions: str = "", **options: Any
    ) -> ProjectData:
        del options
        return project(
            self.client.request(
                "create_project", name=self._validate_project_name(name), instructions=instructions
            )
        )

    def _delete_project_data(self, project_id: str) -> None:
        self.client.request("delete_project", project_id=project_id)

    def _stream_provider_events(self, request: ProviderRequest) -> Iterator[ProviderStreamEvent]:
        self._validate_stream_preconditions(request)
        with closing(iter_stream_response(self.client, request)) as events:
            yield from events

    def _upload_file(self, path: Any) -> dict[str, Any]:
        return self.client.upload_file(path)

    def _resolve_download(
        self,
        url: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
    ) -> Any | None:
        del message_id
        path = self._download_path(url)
        if path is None:
            return None
        if not conversation_id:
            raise ValueError("Conversation ID is not available for this file yet")
        endpoint = self.client.base() + "/conversations/" + remote_id(conversation_id) + "/wiggle"
        listing = self.client.http.request("GET", endpoint + "/list-files?prefix=")
        return self._build_download_source(
            endpoint, path, listing, read_session(self.credential_file)
        )
