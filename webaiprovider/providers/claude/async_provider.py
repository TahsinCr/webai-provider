from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import aclosing
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlencode

from ...accounts import delete_session, session_exists
from ...accounts.credentials import read_session_async
from ...accounts.status import SessionState, SessionStatus
from ...async_provider import AsyncProvider
from ...base import Capability, ProviderType
from ...collections import Page
from ...core.files import DownloadSource, NativeDownloadClient, cookie_header
from ...errors import AccountPaused, FeatureUnavailable
from ...resources.chat.collections import AsyncChatsCollection
from ...resources.chat.types import Conversation
from ...resources.message.protocol import (
    ProviderRequest,
    ProviderStreamEvent,
)
from ...resources.model.model import ModelCatalog
from ...resources.project.collections import AsyncProjectsCollection
from ...resources.project.mutations import ProjectMutationsMixin
from ...resources.project.types import ProjectData
from ...transport import AsyncTransport
from .base import BaseClaudeProvider
from .client import ClaudeClient, remote_id, stream_response
from .errors import ClaudeCompatibilityError
from .projects import ClaudeProjectEditor
from .records import conversation, conversation_attachment_count, project
from .resources import (
    AsyncClaudeChat,
    AsyncClaudeMessage,
    AsyncClaudeProject,
    ClaudeArtifact,
    ClaudeAttachment,
    ClaudeResponse,
    ClaudeSource,
)

if TYPE_CHECKING:
    from ...accounts.account import Account


class AsyncClaudeProvider(
    ProjectMutationsMixin,
    AsyncProvider[
        AsyncChatsCollection[AsyncClaudeChat], AsyncProjectsCollection[AsyncClaudeProject]
    ],
    BaseClaudeProvider,
):
    _message_type = AsyncClaudeMessage
    _response_type = ClaudeResponse
    _source_type = ClaudeSource
    _artifact_type = ClaudeArtifact
    _attachment_type = ClaudeAttachment

    def _make_chats(self) -> AsyncChatsCollection[AsyncClaudeChat]:
        return AsyncChatsCollection(self, AsyncClaudeChat)

    def _make_projects(self) -> AsyncProjectsCollection[AsyncClaudeProject]:
        return AsyncProjectsCollection(self, AsyncClaudeProject)

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        transport: AsyncTransport | None = None,
    ) -> None:
        super().__init__(account, type=type, transport=transport)
        self.credential_file = account.store()
        self.client = ClaudeClient(self.credential_file, transport=transport)
        self.editor = ClaudeProjectEditor(self.client)
        self._gate = asyncio.Lock()
        self._rename_gate = asyncio.Lock()

    def _active_transport(self) -> AsyncTransport | None:
        return self.client.http

    def _purge_credentials_unlocked(self) -> SessionStatus:
        delete_session(self.credential_file)
        self.client.authenticated = False
        return SessionStatus(SessionState.LOGIN_REQUIRED)

    async def purge_credentials(self) -> SessionStatus:
        async with self._gate:
            return self._purge_credentials_unlocked()

    async def _prepare_session(self) -> None:
        self._prime_models(await self.client.prepare())

    async def _close_session(self) -> None:
        async with self._rename_gate:
            await self.client.close()

    async def _authenticate(self) -> SessionStatus:
        async with self._gate:
            await self.client.request("status")
        return SessionStatus(SessionState.AUTHENTICATED, True)

    def _session_status(self) -> SessionStatus:
        saved = session_exists(self.credential_file)
        if self.client.authenticated:
            state = SessionState.AUTHENTICATED
        elif saved:
            state = SessionState.UNKNOWN
        else:
            state = SessionState.LOGIN_REQUIRED
        return SessionStatus(state, saved)

    async def forget_session(self) -> SessionStatus:
        await self.close()
        return await self.purge_credentials()

    async def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
        return await self.client.request("models", force=force)

    async def _list_chats_data(
        self, *, cursor: str | None = None, limit: int = 50
    ) -> Page[Conversation]:
        values = await self.client.request("chats", cursor=cursor, limit=limit)
        if not isinstance(values, list):
            raise ClaudeCompatibilityError("Claude conversation list response changed")
        return Page(
            tuple(conversation(x) for x in values),
            str(int(cursor or 0) + limit) if len(values) == limit else None,
        )

    async def _get_chat_data(self, conversation_id: str) -> Conversation:
        raw = await self.client.request("history", conversation_id=conversation_id)
        return await asyncio.to_thread(conversation, raw)

    async def _conversation_attachment_count(self, conversation_id: str) -> int | None:
        value = await self.client.request("history", conversation_id=conversation_id)
        return conversation_attachment_count(value)

    async def _rename_chat_data(self, conversation_id: str, title: str) -> None:
        if not isinstance(title, str) or not 1 <= len(title.strip()) <= 200:
            raise ValueError("Chat title must contain 1–200 characters")
        await self.client.request(
            "rename_chat", conversation_id=conversation_id, title=title.strip()
        )

    async def _delete_chat_data(self, conversation_id: str) -> None:
        await self.client.request("delete_chat", conversation_id=conversation_id)

    async def _move_chat_data(self, conversation_id: str, project_id: str | None) -> None:
        await self.client.request(
            "move_chat", conversation_id=conversation_id, project_id=project_id
        )

    async def _list_projects_data(self, *, cursor: str | None = None) -> Page[ProjectData]:
        if cursor:
            raise ValueError("Claude project listing does not use a cursor")
        values = await self.client.request("projects")
        if not isinstance(values, list):
            raise ClaudeCompatibilityError("Claude project list response changed")
        return Page(tuple(project(x) for x in values))

    async def _get_project_data(self, project_id: str) -> ProjectData:
        return project(await self.client.request("project", project_id=project_id))

    async def _list_project_chats_data(
        self, project_id: str, *, cursor: str | None = None, limit: int | None = None
    ) -> Page[Conversation]:
        if cursor:
            raise ValueError("Claude project conversation listing does not use a cursor")
        values = await self.client.request("project_chats", project_id=project_id)
        if not isinstance(values, list):
            raise ClaudeCompatibilityError("Claude project conversation response changed")
        return Page(
            tuple(conversation(dict(x, project_uuid=project_id)) for x in values)
        )

    async def _create_project_data(
        self,
        name: str,
        instructions: str = "",
        **options: Any,
    ) -> ProjectData:
        del options
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 200:
            raise ValueError("Project name must contain 1–200 characters")
        return project(
            await self.client.request(
                "create_project", name=name.strip(), instructions=instructions
            ),
        )

    async def _delete_project_data(self, project_id: str) -> None:
        async with self.runtime.project_lock(project_id):
            await self.client.request("delete_project", project_id=project_id)

    async def _stream_provider_events(
        self, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamEvent]:
        if request.temporary and request.project_id:
            raise FeatureUnavailable(Capability.TEMPORARY, "projects_not_supported")
        if not session_exists(self.credential_file) and not self.client.authenticated:
            raise AccountPaused("Verify the Claude account in the Accounts section")
        async with aclosing(stream_response(self.client, request)) as events:
            async for event in events:
                yield event

    async def _upload_file(self, path: Any) -> dict[str, Any]:
        return await self.client.upload_file(path)

    async def _resolve_download(
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
        base = await self.client.base()
        endpoint = base + "/conversations/" + remote_id(conversation_id) + "/wiggle"
        listing = await self.client.http.request("GET", endpoint + "/list-files?prefix=")
        files = listing.get("files_metadata", []) if isinstance(listing, dict) else []
        if not any(isinstance(item, dict) and item.get("path") == path for item in files):
            raise ValueError("File was not found in the conversation record or has expired")
        saved = await read_session_async(self.credential_file)
        cookies = saved.get("cookies", saved)
        values = {key: cookies.get(key) for key in ("sessionKey", "lastActiveOrg")}
        if not values["sessionKey"]:
            raise AccountPaused("Claude session could not be verified")
        headers = {
            "Cookie": cookie_header(values, ("sessionKey", "lastActiveOrg")),
            "Referer": "https://claude.ai/",
        }
        return DownloadSource(
            "https://claude.ai"
            + endpoint
            + "/download-file?"
            + urlencode({"path": path}, quote_via=quote),
            path.rsplit("/", 1)[-1],
            True,
            headers,
            NativeDownloadClient,
        )
