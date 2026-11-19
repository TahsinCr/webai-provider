from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, quote, unquote, urlencode, urlsplit

from ...accounts import delete_session, session_exists
from ...accounts.credentials import read_session_async
from ...accounts.status import SessionState, SessionStatus
from ...async_provider import AsyncProvider
from ...base import Capability
from ...collections import Page
from ...core import gather_bounded
from ...core.files import DownloadSource, NativeDownloadClient, cookie_header
from ...core.timing import current_send_timing
from ...errors import AccountPaused, FeatureUnavailable
from ...provider import Provider, ProviderType
from ...resources.chat.types import Conversation
from ...resources.message.attachment import CLAUDE_ATTACHMENTS
from ...resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    ProviderRequest,
    ProviderStreamEvent,
    ResponseComplete,
    TextDelta,
    TextReplacement,
)
from ...resources.model.model import ModelCatalog
from ...resources.project.mutations import ProjectMutationsMixin
from ...resources.project.types import ProjectData
from ...transport import AsyncTransport
from .client import ClaudeClient, remote_id, validate_upload_candidate
from .errors import ClaudeCompatibilityError, ClaudeStreamError
from .projects import ClaudeProjectEditor
from .records import conversation, conversation_attachment_count, project
from .resources import (
    AsyncClaudeChat,
    AsyncClaudeMessage,
    AsyncClaudeProject,
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


class AsyncClaudeProvider(ProjectMutationsMixin, AsyncProvider):
    provider_type = ProviderType.CLAUDE

    _chat_type = AsyncClaudeChat
    _project_type = AsyncClaudeProject
    _message_type = AsyncClaudeMessage
    _response_type = ClaudeResponse
    _source_type = ClaudeSource
    _artifact_type = ClaudeArtifact
    _attachment_type = ClaudeAttachment

    capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.TEMPORARY,
            Capability.AUTH,
            Capability.HISTORY,
            Capability.MODELS,
            Capability.EFFORT,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.RESOURCES,
            Capability.PROJECT_DELETE,
            Capability.PROJECT_MEMBERSHIP,
            Capability.DELETE,
            Capability.RENAME,
            Capability.ATTACHMENTS,
            Capability.MESSAGE_EDIT,
            Capability.MESSAGE_RETRY,
        }
    )

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

    async def _session_status(self) -> SessionStatus:
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
            tuple(conversation(x, self.account_id) for x in values),
            str(int(cursor or 0) + limit) if len(values) == limit else None,
        )

    async def _get_chat_data(self, conversation_id: str) -> Conversation:
        raw = await self.client.request("history", conversation_id=conversation_id)
        return await asyncio.to_thread(conversation, raw, self.account_id)

    async def _conversation_attachment_count(self, conversation_id: str) -> int | None:
        value = await self.client.request("history", conversation_id=conversation_id)
        return conversation_attachment_count(value)

    async def _rename_chat_data(self, conversation_id: str, title: str) -> None:
        if not isinstance(title, str) or not 1 <= len(title.strip()) <= 200:
            raise ValueError("Chat title must contain 1–200 characters")
        async with self._rename_gate:
            await self.client.request(
                "rename_chat", conversation_id=conversation_id, title=title.strip()
            )

    async def _delete_chat_data(self, conversation_id: str) -> None:
        await self.client.request("delete_chat", conversation_id=conversation_id)

    async def _move_chat_data(self, conversation_id: str, project_id: str | None) -> None:
        async with self._gate:
            await self.client.request(
                "move_chat", conversation_id=conversation_id, project_id=project_id
            )

    async def _list_projects_data(self, *, cursor: str | None = None) -> Page[ProjectData]:
        if cursor:
            raise ValueError("Claude project listing does not use a cursor")
        values = await self.client.request("projects")
        if not isinstance(values, list):
            raise ClaudeCompatibilityError("Claude project list response changed")
        return Page(tuple(project(x, self.account_id) for x in values))

    async def _get_project_data(self, project_id: str) -> ProjectData:
        return project(
            await self.client.request("project", project_id=project_id),
            self.account_id,
        )

    async def _list_project_chats_data(
        self, project_id: str, *, cursor: str | None = None, limit: int | None = None
    ) -> Page[Conversation]:
        if cursor:
            raise ValueError("Claude project conversation listing does not use a cursor")
        values = await self.client.request("project_chats", project_id=project_id)
        if not isinstance(values, list):
            raise ClaudeCompatibilityError("Claude project conversation response changed")
        return Page(
            tuple(conversation(dict(x, project_uuid=project_id), self.account_id) for x in values)
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
        async with self._gate:
            return project(
                await self.client.request(
                    "create_project", name=name.strip(), instructions=instructions
                ),
                self.account_id,
            )

    async def _delete_project_data(self, project_id: str) -> None:
        async with self._gate:
            await self.client.request("delete_project", project_id=project_id)

    async def _stream_provider_events(
        self, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamEvent]:
        if request.temporary and request.project_id:
            raise FeatureUnavailable(Capability.TEMPORARY, "projects_not_supported")
        if not session_exists(self.credential_file) and not self.client.authenticated:
            raise AccountPaused("Verify the Claude account in the Accounts section")
        if len(request.attachments) > CLAUDE_ATTACHMENTS.max_files:
            raise ValueError(f"Claude accepts at most {CLAUDE_ATTACHMENTS.max_files} attachments")
        timing = current_send_timing()
        if timing is not None:
            timing.record("provider_send_gate_wait_ms", 0)
        uploaded = list(request.attachments)
        pending = [
            (index, item)
            for index, item in enumerate(uploaded)
            if isinstance(item, dict) and ("bytes" in item or "path" in item)
        ]

        def validate_uploads() -> None:
            for _, item in pending:
                validate_upload_candidate(
                    item.get("path"),
                    content=item.get("bytes"),
                    name=item.get("name"),
                    mime_type=item.get("mime_type"),
                )

        if any(item.get("path") is not None and item.get("bytes") is None for _, item in pending):
            await asyncio.to_thread(validate_uploads)
        else:
            validate_uploads()
        if pending:
            results = await gather_bounded(
                pending,
                lambda entry: self.client.upload_file(
                    entry[1].get("path"),
                    content=entry[1].get("bytes"),
                    name=entry[1].get("name"),
                    mime_type=entry[1].get("mime_type"),
                ),
                limit=2,
            )
            for (index, _), attachment in zip(pending, results, strict=True):
                uploaded[index] = attachment
        attachments = tuple(uploaded)
        completed = False
        async for event in self.client.events(
            "send",
            prompt=request.prompt,
            conversation_id=request.conversation_id,
            draft_title=request.draft_title,
            project_id=request.project_id,
            model=request.model,
            effort=request.effort,
            temporary=request.temporary,
            attachments=attachments,
            **(
                {"action": request.action, "message_id": request.message_id}
                if request.action
                else {}
            ),
        ):
            if event["type"] == "bound":
                yield ConversationBound(event["id"])
            elif event["type"] in ("replace", "delta"):
                event_type = TextDelta if event["type"] == "delta" else TextReplacement
                yield event_type(event["text"], event.get("channel", "final"))
            elif event["type"] == "done":
                completed = True
            elif event["type"] == "response_complete":
                yield ResponseComplete()
            elif event["type"] == "metadata":
                yield MessageMetadata(
                    message_id=event.get("message_id"),
                    user_message_id=event.get("user_message_id"),
                    model=event.get("model") or request.model,
                    effort=request.effort,
                    sources=tuple(event.get("sources", ())),
                    files=tuple(event.get("files", ())),
                )
        if not completed:
            raise ClaudeStreamError("Claude response ended before completion; it was not resent")

    async def _upload_file(self, path: Any) -> dict[str, Any]:
        return await self.client.upload_file(path)

    async def _resolve_download(
        self,
        url: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
    ) -> Any | None:
        del message_id
        parsed = urlsplit(url)
        path = unquote(parsed.path)
        direct = (
            parsed.scheme == "https"
            and parsed.netloc == "claude.ai"
            and parsed.path.endswith("/wiggle/download-file")
        )
        if direct:
            path = parse_qs(parsed.query).get("path", [""])[0]
        elif parsed.scheme not in ("", "sandbox", "computer") or not path.startswith(
            "/mnt/user-data/"
        ):
            return None
        if (parsed.netloc and not direct) or any(part in (".", "..") for part in path.split("/")):
            raise ValueError("Unsupported conversation file path")
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


class ClaudeProvider(Provider):
    provider_type = ProviderType.CLAUDE
    async_provider_type = AsyncClaudeProvider
    chat_wrapper_type = ClaudeChat
    project_wrapper_type = ClaudeProject
    message_wrapper_type = ClaudeMessage
