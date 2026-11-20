from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from io import StringIO
from typing import TYPE_CHECKING, Any

from ...accounts import delete_session, session_exists
from ...accounts.status import SessionState, SessionStatus
from ...async_provider import AsyncProvider
from ...base import ProviderType
from ...collections import Page
from ...core import MultiDimensionBoundedCache, gather_bounded
from ...errors import AccountPaused, ProviderError, RateLimited
from ...resources.chat.types import Conversation
from ...resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    ProviderRequest,
    ProviderStreamEvent,
    ResponseComplete,
    TextDelta,
)
from ...resources.message.types import MessageAction
from ...resources.model.model import ModelCatalog
from ...resources.project.mutations import ProjectMutationsMixin
from ...resources.project.types import ProjectData
from ...resources.response.source import sources_from
from ...transport import AsyncTransport
from .base import (
    BaseGrokProvider,
    _append_web_search_results,
    _flush_grok_filters,
    _grok_attachment_entry,
    _inherit_grok_edit_attachments,
    _reconcile_grok_native,
    _validate_grok_request,
    _validated_grok_attachment,
)
from .catalogs import parse_model_catalog
from .client import GrokClient, remote_id
from .projects import GrokProjectEditor
from .records import GrokTextFilter, active_responses, conversation, project
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


class AsyncGrokProvider(ProjectMutationsMixin, BaseGrokProvider, AsyncProvider):
    _chat_type = AsyncGrokChat
    _project_type = AsyncGrokProject
    _message_type = AsyncGrokMessage
    _response_type = GrokResponse
    _source_type = GrokSource
    _artifact_type = GrokArtifact
    _attachment_type = GrokAttachment

    _chat_page_size = 100

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
        self._continuations = MultiDimensionBoundedCache(
            max_entries=_CONTINUATION_LIMIT,
            default_ttl=_CONTINUATION_TTL,
        )

    def _active_transport(self) -> AsyncTransport | None:
        return self.client.http

    def _take_continuation(self, conversation_id: str) -> str | None:
        return self._continuations.pop(conversation_id, None)

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

    async def _session_status(self) -> SessionStatus:
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
        query = {"pageSize": max(1, min(int(limit), 100))}
        if cursor:
            query["pageToken"] = cursor
        data = await self.client.request("GET", "/rest/app-chat/conversations", params=query)
        if not isinstance(data, dict):
            raise ProviderError("Grok conversation list response contract changed")
        conversations = data.get("conversations")
        if conversations is not None and not isinstance(conversations, list):
            raise ProviderError("Grok conversation list response contract changed")
        return Page(
            tuple(conversation(item, self.account_id) for item in (conversations or ())),
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
        return conversation(await self._conversation_metadata(conversation_id), self.account_id)

    async def _get_chat_data(self, conversation_id: str) -> Conversation:
        metadata, history = await asyncio.gather(
            self._conversation_metadata(conversation_id),
            self._history(conversation_id),
        )
        active = active_responses(history)
        chat = conversation(metadata, self.account_id, active_rows=active)
        inflight = history.get("inflightResponses") if isinstance(history, dict) else None
        if isinstance(inflight, list) and inflight:
            return Conversation(
                chat.id,
                chat.title,
                chat.messages,
                chat.project,
                {**chat.metadata, "generation": {"active": True}},
            )
        if active and isinstance(active[-1], dict) and active[-1].get("responseId"):
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
        self._continuations.pop(conversation_id, None)

    async def _move_chat_data(self, conversation_id: str, project_id: str | None) -> None:
        metadata = await self._conversation_metadata(conversation_id)
        chat = conversation(metadata, self.account_id)
        old_project = chat.project.id if chat.project is not None else None
        if old_project == project_id:
            return
        conversation_path = "/conversations/" + remote_id(conversation_id)
        if project_id:
            await self.client.request(
                "POST",
                "/rest/workspaces/" + remote_id(project_id) + "/conversations",
                json={"conversationId": conversation_id},
            )
        if old_project:
            try:
                await self.client.request(
                    "DELETE",
                    "/rest/workspaces/" + remote_id(old_project) + conversation_path,
                )
            except Exception:
                if project_id:
                    try:
                        await self.client.request(
                            "DELETE",
                            "/rest/workspaces/" + remote_id(project_id) + conversation_path,
                        )
                    except Exception:
                        raise ProviderError(
                            "The Grok conversation move did not complete; the conversation may remain in multiple workspaces."
                        ) from None
                raise
        for _ in range(5):
            metadata = await self._conversation_metadata(conversation_id)
            updated = conversation(metadata, self.account_id)
            updated_project = updated.project.id if updated.project is not None else None
            if updated_project == project_id:
                return
            await asyncio.sleep(0.1)
        raise ProviderError(
            "Grok conversation project membership could not be confirmed by the server"
        )

    async def _list_projects_data(self, *, cursor: str | None = None) -> Page[ProjectData]:
        query = {"pageSize": 50}
        if cursor:
            query["pageToken"] = cursor
        data = await self.client.request("GET", "/rest/workspaces", params=query)
        if not isinstance(data, dict):
            raise ProviderError("Grok workspace list response contract changed")
        workspaces = data.get("workspaces")
        if workspaces is not None and not isinstance(workspaces, list):
            raise ProviderError("Grok workspace list response contract changed")
        return Page(
            tuple(project(item, self.account_id) for item in (workspaces or ())),
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
        return project(workspace, self.account_id, files or [])

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
                    self.account_id,
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
        return project(data, self.account_id)

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
            self._continuations.pop(conversation_id, None)
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

    async def _stream_provider_events(
        self, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamEvent]:
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

        response_id = user_response_id = None
        streamed_text, streamed_analysis = StringIO(), StringIO()
        raw_native_text = raw_native_analysis = None
        sources = ()
        raw_search_results: list[dict] = []
        seen_search_keys: set[str] = set()
        response_model = model
        complete = False
        completion_notified = False
        bound = False
        text_filter, analysis_filter = GrokTextFilter(), GrokTextFilter()

        def finalize_turn():
            nonlocal sources
            if raw_search_results and not sources:
                sources = sources_from({"sources": raw_search_results})
            yield from _flush_grok_filters(
                text_filter, streamed_text, analysis_filter, streamed_analysis
            )
            yield from _reconcile_grok_native(
                raw_native_text, raw_native_analysis, streamed_text, streamed_analysis
            )

        async for chunk in self.client.events(path, body):
            result = chunk.get("result")
            if not isinstance(result, dict):
                result = chunk
            native_conversation = result.get("conversation")
            response = result.get("response")
            if not isinstance(response, dict):
                response = result
            if not conversation_id and isinstance(native_conversation, dict):
                conversation_id = native_conversation.get("conversationId")
                if conversation_id and not bound:
                    yield ConversationBound(conversation_id)
                    bound = True
            if not conversation_id and not request.conversation_id:
                conversation_id = result.get("conversationId") or response.get("conversationId")
                if conversation_id and not bound:
                    yield ConversationBound(conversation_id)
                    bound = True
            error = response.get("error") or result.get("error") or chunk.get("error")
            if error:
                self._raise_stream_error(error)
            user = response.get("userResponse")
            if isinstance(user, dict) and user.get("responseId"):
                user_response_id = user["responseId"]
            native = response.get("modelResponse")
            if isinstance(native, dict):
                response_id = native.get("responseId") or response_id
                response_model = native.get("model") or response_model
                _append_web_search_results(native, raw_search_results, seen_search_keys)
                if isinstance(native.get("message"), str):
                    raw_native_text = native["message"]
                if native.get("thinkingTrace") is not None:
                    raw_native_analysis = native["thinkingTrace"]
                if native.get("partial") is False:
                    complete = True
            token = response.get("token")
            if isinstance(token, str) and token:
                analysis = response.get("isThinking") or response.get("messageTag") in (
                    "header",
                    "summary",
                )
                cleaner = analysis_filter if analysis else text_filter
                visible = cleaner.feed(token)
                if visible:
                    (streamed_analysis if analysis else streamed_text).write(visible)
                    yield TextDelta(visible, "analysis") if analysis else TextDelta(visible)
            if response.get("responseId") and not response_id:
                response_id = response["responseId"]
            if response.get("done") is True or response.get("finished") is True:
                complete = True
            if (
                complete
                and not completion_notified
                and (request.conversation_id or conversation_id)
            ):
                for event in finalize_turn():
                    yield event
                yield ResponseComplete()
                completion_notified = True

        for event in finalize_turn():
            yield event
        if not conversation_id:
            raise ProviderError("Grok did not return a conversation ID")
        if not complete:
            raise ProviderError("Grok stream ended before response completion; it was not resent")
        if response_id:
            self._remember_continuation(conversation_id, response_id)
        if not bound and not request.conversation_id:
            yield ConversationBound(conversation_id)
        if response_id and streamed_text.tell():
            yield MessageMetadata(
                message_id=response_id,
                user_message_id=user_response_id,
                model=response_model,
                effort=None,
                sources=sources,
            )
        if not completion_notified:
            yield ResponseComplete()

    async def _upload_file(self, path: Any) -> dict[str, Any]:
        return await self.client.upload_file(path)
