"""Claude provider using a saved web session over HTTPS."""

from __future__ import annotations

import asyncio
import time

from ...accounts import delete_session, session_exists
from ...accounts.account import Account
from ...async_provider import AsyncProvider
from ...errors import AccountPaused, FeatureUnavailable
from ...models import Capability, Page, SessionState, SessionStatus
from ...provider import Provider, ProviderType
from ...resources.chat.chat import Chat, MovableChat
from ...streams import (
    ConversationBound,
    MessageMetadata,
    ResponseComplete,
    TextDelta,
    TextReplacement,
)
from ...utils.timing import current_send_timing
from .client import ClaudeClient, ClaudeProjectEditor
from .errors import ClaudeCompatibilityError, ClaudeStreamError
from .records import conversation, conversation_attachment_count, project
from .resources import AsyncClaudeChat


class AsyncClaudeProvider(AsyncProvider):
    provider_type = ProviderType.CLAUDE

    _chat_type = AsyncClaudeChat

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
        self, account: Account, *, type: ProviderType | str | None = None, transport=None
    ) -> None:
        super().__init__(account, type=type)
        self.credential_file = account.store()
        self.client = ClaudeClient(self.credential_file, transport=transport)
        self._gate = asyncio.Lock()
        self._rename_gate = asyncio.Lock()
        self._transport = self.client.http
        self.editor = ClaudeProjectEditor(self.client)

    async def _close_provider(self):
        async with self._rename_gate:
            try:
                await self.editor.close()
            except BaseException as editor_error:
                try:
                    await self.client.close()
                except BaseException as close_error:
                    raise editor_error from close_error
                raise
            await self.client.close()

    async def prepare(self):
        await self.client.prepare()

    async def upload_file(self, path):
        async with self._gate:
            return await self.client.upload_file(path)

    async def authenticate(self):
        async with self._gate:
            await self.client.request("status")
        return SessionStatus(SessionState.AUTHENTICATED, True)

    async def session_status(self):
        return SessionStatus(
            SessionState.AUTHENTICATED
            if self.client.authenticated
            else SessionState.UNKNOWN
            if session_exists(self.credential_file)
            else SessionState.LOGIN_REQUIRED,
            session_exists(self.credential_file),
        )

    async def resolve_download(self, url, conversation_id=None, message_id=None):
        from urllib.parse import parse_qs, quote, unquote, urlencode, urlsplit

        from ...accounts import read_session
        from ...utils.files import DownloadSource, NativeDownloadClient, cookie_header
        from .client import remote_id

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
        saved = read_session(self.credential_file)
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

    async def forget_session(self):
        async with self._gate:
            await self.close()
            delete_session(self.credential_file)
            self.client.authenticated = False
        return SessionStatus(SessionState.LOGIN_REQUIRED)

    async def _load_model_catalog(self, *, force: bool = False):
        async with self._gate:
            return await self.client.request("models", force=force)

    async def list_chats(self, cursor=None, limit=50):
        values = await self.client.request("chats", cursor=cursor, limit=limit)
        if not isinstance(values, list):
            raise ClaudeCompatibilityError("Claude conversation list response changed")
        return Page(
            tuple(conversation(x, self.account_id) for x in values),
            str(int(cursor or 0) + limit) if len(values) == limit else None,
        )

    async def get_chat(self, conversation_id):
        raw = await self.client.request("history", conversation_id=conversation_id)
        return await asyncio.to_thread(
            conversation,
            raw,
            self.account_id,
        )

    async def conversation_attachment_count(self, conversation_id):
        value = await self.client.request("history", conversation_id=conversation_id)
        return conversation_attachment_count(value)

    async def list_projects(self, cursor=None):
        if cursor:
            raise ValueError("Claude project listing does not use a cursor")
        async with self._gate:
            values = await self.client.request("projects")
            if not isinstance(values, list):
                raise ClaudeCompatibilityError("Claude project list response changed")
            return Page(tuple(project(x, self.account_id) for x in values))

    async def get_project(self, project_id):
        return project(await self.client.request("project", project_id=project_id), self.account_id)

    async def list_project_chats(self, project_id, cursor=None):
        if cursor:
            raise ValueError("Claude project conversation listing does not use a cursor")
        async with self._gate:
            values = await self.client.request("project_chats", project_id=project_id)
            if not isinstance(values, list):
                raise ClaudeCompatibilityError("Claude project conversation response changed")
            return Page(
                tuple(
                    conversation(dict(x, project_uuid=project_id), self.account_id) for x in values
                )
            )

    async def create_project(self, name, instructions=""):
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 200:
            raise ValueError("Project name must contain 1–200 characters")
        async with self._gate:
            return project(
                await self.client.request(
                    "create_project", name=name.strip(), instructions=instructions
                ),
                self.account_id,
            )

    async def rename_chat(self, conversation_id, title):
        if not isinstance(title, str) or not 1 <= len(title.strip()) <= 200:
            raise ValueError("Chat title must contain 1–200 characters")
        async with self._rename_gate:
            await self.client.request(
                "rename_chat", conversation_id=conversation_id, title=title.strip()
            )

    async def delete_chat(self, conversation_id):
        async with self._gate:
            await self.client.request("delete_chat", conversation_id=conversation_id)

    async def delete_project(self, project_id):
        async with self._gate:
            await self.client.request("delete_project", project_id=project_id)

    async def move_chat(self, conversation_id, project_id):
        async with self._gate:
            await self.client.request(
                "move_chat", conversation_id=conversation_id, project_id=project_id
            )

    async def _stream_request(self, request):
        if request.temporary and request.project_id:
            raise FeatureUnavailable(
                Capability.TEMPORARY,
                "projects_not_supported",
            )
        if not session_exists(self.credential_file) and not self.client.authenticated:
            raise AccountPaused("Verify the Claude account in the Accounts section")
        timing = current_send_timing()
        gate_started = time.perf_counter_ns() if timing is not None else None
        async with self._gate:
            if gate_started is not None:
                timing.record("provider_send_gate_wait_ms", time.perf_counter_ns() - gate_started)
            completed = False
            async for event in self.client.events(
                "send",
                prompt=request.prompt,
                conversation_id=request.conversation_id,
                project_id=request.project_id,
                model=request.model,
                effort=request.effort,
                temporary=request.temporary,
                attachments=request.attachments,
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
                raise ClaudeStreamError(
                    "Claude response ended before completion; it was not resent"
                )


class ClaudeChat(MovableChat, Chat):
    pass


class ClaudeProvider(Provider):
    provider_type = ProviderType.CLAUDE
    async_provider_type = AsyncClaudeProvider
    chat_wrapper_type = ClaudeChat
