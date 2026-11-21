from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import aclosing
from typing import TYPE_CHECKING, Any

from ...accounts.credentials import read_session_async
from ...accounts.status import SessionState, SessionStatus
from ...async_provider import AsyncProvider
from ...base import ProviderType
from ...collections import Page
from ...resources.chat.collections import AsyncChatsCollection
from ...resources.chat.types import Conversation
from ...resources.message.protocol import ProviderRequest, ProviderStreamEvent
from ...resources.model.model import ModelCatalog
from ...resources.project.collections import AsyncProjectsCollection
from ...resources.project.mutations import ProjectMutationsMixin
from ...resources.project.types import ProjectData
from ...transport import AsyncTransport
from .async_client import AsyncClaudeClient, AsyncClaudeProjectEditor, stream_response
from .base import BaseClaudeProvider, remote_id
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
        self.client = AsyncClaudeClient(self.credential_file, transport=transport)
        self.editor = AsyncClaudeProjectEditor(self.client)
        self._gate = asyncio.Lock()
        self._rename_gate = asyncio.Lock()

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

    async def forget_session(self) -> SessionStatus:
        await self.close()
        return await self.purge_credentials()

    async def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
        return await self.client.request("models", force=force)

    async def _list_chats_data(
        self, *, cursor: str | None = None, limit: int = 50
    ) -> Page[Conversation]:
        values = await self.client.request("chats", cursor=cursor, limit=limit)
        return self._parse_chats_page(values, cursor, limit)

    async def _get_chat_data(self, conversation_id: str) -> Conversation:
        raw = await self.client.request("history", conversation_id=conversation_id)
        return await asyncio.to_thread(conversation, raw)

    async def _conversation_attachment_count(self, conversation_id: str) -> int | None:
        value = await self.client.request("history", conversation_id=conversation_id)
        return conversation_attachment_count(value)

    async def _rename_chat_data(self, conversation_id: str, title: str) -> None:
        await self.client.request(
            "rename_chat",
            conversation_id=conversation_id,
            title=self._validate_chat_title(title),
        )

    async def _delete_chat_data(self, conversation_id: str) -> None:
        await self.client.request("delete_chat", conversation_id=conversation_id)

    async def _move_chat_data(
        self, conversation_id: str, project_id: str | None, *, source_project_id: str | None
    ) -> None:
        del source_project_id
        await self.client.request(
            "move_chat", conversation_id=conversation_id, project_id=project_id
        )

    async def _list_projects_data(self, *, cursor: str | None = None) -> Page[ProjectData]:
        if cursor:
            raise ValueError("Claude project listing does not use a cursor")
        return self._parse_projects_page(await self.client.request("projects"))

    async def _get_project_data(self, project_id: str) -> ProjectData:
        return project(await self.client.request("project", project_id=project_id))

    async def _list_project_chats_data(
        self, project_id: str, *, cursor: str | None = None, limit: int | None = None
    ) -> Page[Conversation]:
        del limit
        if cursor:
            raise ValueError("Claude project conversation listing does not use a cursor")
        return self._parse_project_chats_page(
            project_id, await self.client.request("project_chats", project_id=project_id)
        )

    async def _create_project_data(
        self, name: str, instructions: str = "", **options: Any
    ) -> ProjectData:
        del options
        return project(
            await self.client.request(
                "create_project", name=self._validate_project_name(name), instructions=instructions
            )
        )

    async def _delete_project_data(self, project_id: str) -> None:
        await self.client.request("delete_project", project_id=project_id)

    async def _stream_provider_events(
        self, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamEvent]:
        self._validate_stream_preconditions(request)
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
        endpoint = (
            await self.client.base() + "/conversations/" + remote_id(conversation_id) + "/wiggle"
        )
        listing = await self.client.http.request("GET", endpoint + "/list-files?prefix=")
        saved = await read_session_async(self.credential_file)
        return self._build_download_source(endpoint, path, listing, saved)
