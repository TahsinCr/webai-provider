"""Grok web-session adapter."""

import asyncio
import time
from io import StringIO

from ...api import (
    Capability,
    ConversationBound,
    MessageAction,
    MessageMetadata,
    ReplaceText,
    ResponseComplete,
    TextDelta,
    validate_request,
)
from ...base import ProviderAdapter
from ...core import MultiDimensionBoundedCache
from ...errors import AccountPaused, FeatureUnavailable, ProviderError, RateLimited
from ...messages import sources_from
from ...models import (
    Conversation,
    FeatureState,
    Model,
    ModelCatalog,
    Page,
    SessionState,
    SessionStatus,
)
from ...projects import ProjectEditing
from ...session import delete_session, session_exists
from ...utils.attachments import GROK_ATTACHMENTS
from .records import GrokTextFilter, active_responses, clean_text, conversation, project
from .transport import GrokProjectEditor, GrokTransport, remote_id

_CATALOG_TTL = 300
_CONTINUATION_TTL = 60
_CONTINUATION_LIMIT = 128
_MODEL_MODES = {
    "auto": "MODEL_MODE_AUTO",
    "fast": "MODEL_MODE_FAST",
    "expert": "MODEL_MODE_EXPERT",
    "heavy": "MODEL_MODE_HEAVY",
}


class GrokProvider(ProjectEditing, ProviderAdapter):
    id = "grok"
    capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.TEMPORARY,
            Capability.AUTH,
            Capability.ATTACHMENTS,
            Capability.HISTORY,
            Capability.RENAME,
            Capability.DELETE,
            Capability.MODELS,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.PROJECT_DELETE,
            Capability.PROJECT_MEMBERSHIP,
            Capability.RESOURCES,
            Capability.MESSAGE_RETRY,
            Capability.MESSAGE_EDIT,
        }
    )

    def __init__(self, credential_file, account_id="default", *, client_factory=None):
        self.credential_file = credential_file
        self.account_id = account_id
        self.transport = GrokTransport(credential_file, client_factory=client_factory)
        self.editor = GrokProjectEditor(self.transport)
        self._gate = asyncio.Lock()
        self._catalog = None
        self._catalog_at = 0.0
        self._continuations = MultiDimensionBoundedCache(
            max_entries=_CONTINUATION_LIMIT,
            default_ttl=_CONTINUATION_TTL,
        )

    def _take_continuation(self, conversation_id):
        return self._continuations.pop(conversation_id, None)

    def _remember_continuation(self, conversation_id, response_id):
        if (
            not isinstance(conversation_id, str)
            or not isinstance(response_id, str)
            or not response_id
        ):
            return
        self._continuations.put(conversation_id, response_id)

    async def prepare(self):
        await self.authenticate()

    async def close(self):
        async with self._gate:
            self._continuations.clear()
            await self.transport.close()

    async def authenticate(self):
        await self.transport.authenticate()
        return SessionStatus(SessionState.AUTHENTICATED, True)

    async def session_status(self):
        return SessionStatus(
            SessionState.AUTHENTICATED
            if self.transport.session.authenticated
            else SessionState.UNKNOWN
            if session_exists(self.credential_file)
            else SessionState.LOGIN_REQUIRED,
            session_exists(self.credential_file),
        )

    async def forget_session(self):
        async with self._gate:
            await self.transport.close()
            delete_session(self.credential_file)
            self._catalog = None
            self._continuations.clear()
            self.transport.session.authenticated = False
        return SessionStatus(SessionState.LOGIN_REQUIRED)

    async def upload_file(self, path):
        return await self.transport.upload_file(path)

    async def models(self, *, force=False):
        now = time.monotonic()
        if not force and self._catalog and now - self._catalog_at < _CATALOG_TTL:
            return self._catalog
        async with self._gate:
            now = time.monotonic()
            if not force and self._catalog and now - self._catalog_at < _CATALOG_TTL:
                return self._catalog
            data, details = await asyncio.gather(
                self.transport.request("POST", "/rest/modes", json={"locale": "en"}),
                self.transport.request("POST", "/rest/models", json={"locale": "en"}),
            )
            if (
                not isinstance(data, dict)
                or not isinstance(data.get("modes"), list)
                or not isinstance(details, dict)
                or not isinstance(details.get("models"), list)
            ):
                raise ProviderError("Grok model catalog response contract changed")

            by_mode, by_id = {}, {}
            for item in details["models"]:
                if not isinstance(item, dict):
                    continue
                identifier, mode = item.get("modelId"), item.get("modelMode")
                if isinstance(identifier, str) and identifier and isinstance(mode, str) and mode:
                    by_mode.setdefault(mode, {})[identifier] = item
                    by_id[identifier] = item

            models = []
            for item in data["modes"]:
                if not isinstance(item, dict):
                    continue
                model_id = item.get("id")
                if (
                    not isinstance(model_id, str)
                    or not model_id
                    or model_id in ("build", "imagine", "voice")
                ):
                    continue
                availability = item.get("availability")
                if (
                    not isinstance(availability, dict)
                    or not isinstance(availability.get("available"), dict)
                    or any(
                        availability.get(key) is not None
                        for key in ("requiresUpgrade", "unavailable", "comingSoon")
                    )
                ):
                    continue
                title = item.get("title")
                title = title.strip() if isinstance(title, str) and title.strip() else model_id
                metadata = {
                    key: item[key]
                    for key in ("description", "badgeText", "iconHint", "tags")
                    if item.get(key) is not None
                }
                metadata["modeName"] = title
                description = item.get("description")
                advertised = (
                    description.rsplit("·", 1)[-1].strip() if isinstance(description, str) else ""
                )
                advertised = advertised if advertised.casefold().startswith("grok ") else ""
                model_mode = item.get("modelMode")
                if not isinstance(model_mode, str) or not model_mode:
                    model_mode = _MODEL_MODES.get(model_id.casefold()) or by_id.get(
                        model_id, {}
                    ).get("modelMode")
                if isinstance(model_mode, str) and model_mode:
                    metadata["modelMode"] = model_mode
                matches = by_mode.get(model_mode, {})
                detail = matches.get(model_id)
                if detail is None and len(matches) == 1:
                    detail = next(iter(matches.values()))
                if detail is not None:
                    metadata["modelName"] = detail["modelId"]
                    name = detail.get("name")
                    title = (
                        name.strip()
                        if isinstance(name, str) and name.strip()
                        else advertised
                        or detail["modelId"].replace("-", " ").replace("_", " ").title()
                    )
                elif advertised:
                    title = advertised
                chat_title = (
                    title
                    if model_id.casefold().startswith("grok-")
                    or title.casefold().startswith("grok")
                    else f"Grok · {title}"
                )
                if metadata.get("modeName") and metadata["modeName"] != title:
                    chat_title += " · " + metadata["modeName"]
                models.append(Model(model_id, title, metadata=metadata, chat_title=chat_title))

            default = data.get("defaultModeId")
            if not any(model.id == default for model in models):
                default = models[0].id if models else None
            self._catalog = ModelCatalog(
                FeatureState.AVAILABLE if models else FeatureState.UNAVAILABLE,
                tuple(models),
                default,
            )
            self._catalog_at = time.monotonic()
            return self._catalog

    async def list_chats(self, cursor=None, limit=50):
        query = {"pageSize": max(1, min(int(limit), 100))}
        if cursor:
            query["pageToken"] = cursor
        data = await self.transport.request("GET", "/rest/app-chat/conversations", params=query)
        if not isinstance(data, dict):
            raise ProviderError("Grok conversation list response contract changed")
        conversations = data.get("conversations")
        if conversations is not None and not isinstance(conversations, list):
            raise ProviderError("Grok conversation list response contract changed")
        return Page(
            tuple(conversation(item, self.account_id) for item in (conversations or ())),
            data.get("nextPageToken"),
        )

    async def _history(self, conversation_id):
        return await self.transport.request(
            "GET",
            "/rest/app-chat/conversations/" + remote_id(conversation_id) + "/responses",
            params={"includeThreads": True},
        )

    async def _response_nodes(self, conversation_id):
        return await self.transport.request(
            "GET",
            "/rest/app-chat/conversations/" + remote_id(conversation_id) + "/response-node",
            params={"includeThreads": False},
        )

    async def _conversation_metadata(self, conversation_id):
        return await self.transport.request(
            "GET",
            "/rest/app-chat/conversations/" + remote_id(conversation_id),
            params={"includeWorkspaces": True},
        )

    async def get_chat(self, conversation_id):
        metadata, history = await asyncio.gather(
            self._conversation_metadata(conversation_id),
            self._history(conversation_id),
        )
        chat = conversation(metadata, self.account_id, history)
        inflight = history.get("inflightResponses") if isinstance(history, dict) else None
        if isinstance(inflight, list) and inflight:
            return Conversation(
                chat.origin,
                chat.title,
                chat.messages,
                chat.project_id,
                {**chat.metadata, "generation": {"active": True}},
            )
        return chat

    async def conversation_attachment_count(self, conversation_id):
        return None

    async def rename_chat(self, conversation_id, title):
        if not isinstance(title, str) or not 1 <= len(title.strip()) <= 200:
            raise ValueError("Sohbet başlığı 1–200 karakter olmalıdır")
        await self.transport.request(
            "PUT",
            "/rest/app-chat/conversations/" + remote_id(conversation_id),
            json={"title": title.strip()},
        )

    async def delete_chat(self, conversation_id):
        await self.transport.request(
            "DELETE", "/rest/app-chat/conversations/" + remote_id(conversation_id)
        )
        self._continuations.pop(conversation_id, None)

    async def list_projects(self, cursor=None):
        query = {"pageSize": 50}
        if cursor:
            query["pageToken"] = cursor
        data = await self.transport.request("GET", "/rest/workspaces", params=query)
        if not isinstance(data, dict):
            raise ProviderError("Grok workspace list response contract changed")
        workspaces = data.get("workspaces")
        if workspaces is not None and not isinstance(workspaces, list):
            raise ProviderError("Grok workspace list response contract changed")
        return Page(
            tuple(project(item, self.account_id) for item in (workspaces or ())),
            data.get("nextPageToken"),
        )

    async def get_project(self, project_id):
        path = "/rest/workspaces/" + remote_id(project_id)
        workspace, listing = await asyncio.gather(
            self.transport.request("GET", path),
            self.transport.request("GET", path + "/files", params={"recursive": "true"}),
        )
        if not isinstance(listing, dict):
            raise ProviderError("Grok workspace file response contract changed")
        files = listing.get("files")
        if files is not None and not isinstance(files, list):
            raise ProviderError("Grok workspace file response contract changed")
        return project(workspace, self.account_id, files or [])

    async def list_project_chats(self, project_id, cursor=None):
        query = {"pageSize": 50, "workspaceId": project_id}
        if cursor:
            query["pageToken"] = cursor
        data = await self.transport.request("GET", "/rest/app-chat/conversations", params=query)
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

    async def create_project(
        self, name, instructions="", *, visibility="private", memory="default"
    ):
        if visibility != "private" or memory != "default":
            raise FeatureUnavailable(
                Capability.PROJECT_CREATE, "Grok privacy or memory options are unavailable"
            )
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 200:
            raise ValueError("Proje adı 1–200 karakter olmalıdır")
        if not isinstance(instructions, str):
            raise ValueError("Proje talimatları metin olmalıdır")
        data = await self.transport.request(
            "POST",
            "/rest/workspaces",
            json={"name": name.strip(), "customPersonality": instructions or ""},
        )
        return project(data, self.account_id)

    async def delete_project(self, project_id):
        await self.transport.request("DELETE", "/rest/workspaces/" + remote_id(project_id))

    async def move_chat(self, conversation_id, project_id):
        metadata = await self._conversation_metadata(conversation_id)
        chat = conversation(metadata, self.account_id)
        if chat.project_id == project_id:
            return
        conversation_path = "/conversations/" + remote_id(conversation_id)
        old_project = chat.project_id
        if project_id:
            await self.transport.request(
                "POST",
                "/rest/workspaces/" + remote_id(project_id) + "/conversations",
                json={"conversationId": conversation_id},
            )
        if old_project:
            try:
                await self.transport.request(
                    "DELETE",
                    "/rest/workspaces/" + remote_id(old_project) + conversation_path,
                )
            except Exception:
                if project_id:
                    try:
                        await self.transport.request(
                            "DELETE",
                            "/rest/workspaces/" + remote_id(project_id) + conversation_path,
                        )
                    except Exception:
                        raise ProviderError(
                            "Grok sohbet taşıma işlemi tamamlanmadı; sohbet birden fazla çalışma alanında kalmış olabilir"
                        ) from None
                raise
        for _ in range(5):
            metadata = await self._conversation_metadata(conversation_id)
            if conversation(metadata, self.account_id).project_id == project_id:
                return
            await asyncio.sleep(0.2)
        raise ProviderError("Grok sohbetin proje üyeliği sunucuda doğrulanamadı")

    async def stream(self, request):
        validate_request(self, request)
        if request.effort is not None:
            raise FeatureUnavailable(Capability.EFFORT, "Grok does not expose shared effort levels")
        if len(request.attachments) > GROK_ATTACHMENTS.max_files:
            raise ValueError("Too many Grok chat attachments")
        file_attachments = []
        image_attachments = []
        total_attachment_bytes = 0
        for attachment in request.attachments:
            metadata = attachment.get("grok_file") if isinstance(attachment, dict) else None
            asset_id = metadata.get("assetId") if isinstance(metadata, dict) else None
            size = attachment.get("size_bytes") if isinstance(attachment, dict) else None
            if (
                not isinstance(asset_id, str)
                or not asset_id
                or asset_id != attachment.get("id")
                or not isinstance(size, int)
                or isinstance(size, bool)
                or not 0 < size <= GROK_ATTACHMENTS.max_bytes
            ):
                raise ValueError("Grok chat attachment metadata is invalid")
            total_attachment_bytes += size
            mime = str(
                attachment.get("mime_type")
                or (metadata.get("mimeType") if isinstance(metadata, dict) else "")
                or ""
            ).lower()
            if mime.startswith("image/"):
                image_attachments.append(metadata)
            else:
                file_attachments.append(metadata)
        if (
            GROK_ATTACHMENTS.max_total_bytes
            and total_attachment_bytes > GROK_ATTACHMENTS.max_total_bytes
        ):
            raise ValueError("Grok chat attachments exceed the total size limit")
        if request.branch:
            raise FeatureUnavailable(
                Capability.CHAT_BRANCH, "Grok native chat branching is unavailable"
            )
        if request.temporary and request.project_id:
            raise FeatureUnavailable(
                Capability.TEMPORARY, "Temporary chats cannot belong to a project"
            )
        if request.action not in (None, MessageAction.RETRY, MessageAction.EDIT):
            raise FeatureUnavailable(request.action.capability)
        if request.action == MessageAction.RETRY and not request.message_id:
            raise ValueError("Grok retry requires the selected assistant response")
        if request.action == MessageAction.EDIT and not request.message_id:
            raise ValueError("Grok edit requires the selected user prompt")

        catalog = await self.models()
        selected = catalog.chat_model(request.model) if request.model else None
        if selected is None and request.model:
            catalog = await self.models(force=True)
            selected = catalog.chat_model(request.model)
        if selected is None and not request.model:
            selected = catalog.chat_model(catalog.default_model)
        if selected is None:
            raise ProviderError("Selected Grok model is unavailable for this account")
        model = selected.metadata.get("modelName")
        model_mode = selected.metadata.get("modelMode")
        if not isinstance(model_mode, str) or not model_mode:
            model_mode = None

        async with self._gate:
            conversation_id = request.conversation_id
            parent_response_id = ""
            is_retry = request.action == MessageAction.RETRY
            is_edit = request.action == MessageAction.EDIT
            if conversation_id:
                cached_parent = None
                if is_retry or is_edit:
                    self._continuations.pop(conversation_id, None)
                    history = await self._history(conversation_id)
                else:
                    cached_parent = self._take_continuation(conversation_id)
                    history = (
                        None
                        if cached_parent is not None
                        else await self._response_nodes(conversation_id)
                    )
                if cached_parent is not None:
                    parent_response_id = cached_parent
                else:
                    if not isinstance(history, dict):
                        raise ProviderError("Grok conversation history response contract changed")
                    if (
                        not is_retry
                        and not is_edit
                        and not isinstance(history.get("responseNodes"), list)
                    ):
                        raise ProviderError("Grok conversation node response contract changed")
                    if history.get("inflightResponses"):
                        raise ProviderError("Grok conversation already has a response in progress")
                    active = active_responses(history)
                    if is_retry:
                        target = next(
                            (
                                item
                                for item in active
                                if item.get("responseId") == request.message_id
                            ),
                            None,
                        )
                        if target is None or str(target.get("sender", "")).lower() not in (
                            "assistant",
                            "model",
                            "grok",
                        ):
                            raise ProviderError(
                                "Grok retry target is no longer on the active conversation path"
                            )
                        parent_response_id = target.get("parentResponseId") or ""
                    elif is_edit:
                        target = next(
                            (
                                item
                                for item in active
                                if item.get("responseId") == request.message_id
                            ),
                            None,
                        )
                        if target is None or str(target.get("sender", "")).lower() not in (
                            "human",
                            "user",
                        ):
                            raise ProviderError(
                                "Grok edit target is no longer on the active conversation path"
                            )
                        parent_response_id = target.get("parentResponseId") or ""
                        if not file_attachments and not image_attachments:
                            seen_assets = set()
                            for item in list(target.get("fileAttachments") or ()) + list(
                                target.get("imageAttachments") or ()
                            ):
                                if isinstance(item, dict):
                                    aid = item.get("assetId") or item.get("id")
                                    if aid and aid in seen_assets:
                                        continue
                                    if aid:
                                        seen_assets.add(aid)
                                    mime = str(
                                        item.get("mimeType") or item.get("fileMimeType") or ""
                                    ).lower()
                                    if mime.startswith("image/"):
                                        image_attachments.append(item)
                                    else:
                                        file_attachments.append(item)
                    elif active:
                        parent_response_id = active[-1].get("responseId") or ""

            body = {
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
                body["modelName"] = model or selected.id
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
        raw_search_results = []
        seen_search_keys = set()
        response_model = model
        complete = False
        completion_notified = False
        bound = False
        text_filter, analysis_filter = GrokTextFilter(), GrokTextFilter()

        def resolve_sources():
            nonlocal sources
            if raw_search_results and not sources:
                sources = sources_from({"sources": raw_search_results})

        def flush_text_filters():
            for cleaner, output, channel in (
                (text_filter, streamed_text, None),
                (analysis_filter, streamed_analysis, "analysis"),
            ):
                remainder = cleaner.feed(final=True)
                if remainder:
                    output.write(remainder)
                    yield TextDelta(remainder, channel) if channel else TextDelta(remainder)

        def reconcile_native():
            resolved_analysis = (
                clean_text(raw_native_analysis) if raw_native_analysis is not None else None
            )
            resolved_text = clean_text(raw_native_text) if raw_native_text is not None else None
            for native, output, channel in (
                (resolved_analysis, streamed_analysis, "analysis"),
                (resolved_text, streamed_text, None),
            ):
                if native is not None and native != output.getvalue():
                    output.seek(0)
                    output.truncate()
                    output.write(native)
                    yield ReplaceText(native, channel) if channel else ReplaceText(native)

        async for chunk in self.transport.events(path, body):
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
                code = str(error.get("code") or "").upper() if isinstance(error, dict) else ""
                msg = str(error.get("message") or "") if isinstance(error, dict) else str(error)
                if code in ("UNAUTHORIZED", "AUTH_REQUIRED"):
                    self.transport.session.authenticated = False
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
            user = response.get("userResponse")
            if isinstance(user, dict) and user.get("responseId"):
                user_response_id = user["responseId"]
            native = response.get("modelResponse")
            if isinstance(native, dict):
                response_id = native.get("responseId") or response_id
                response_model = native.get("model") or response_model
                if isinstance(native.get("webSearchResults"), list):
                    for item in native["webSearchResults"]:
                        if isinstance(item, dict):
                            key = item.get("url") or item.get("title")
                            if key and key not in seen_search_keys:
                                seen_search_keys.add(key)
                                raw_search_results.append(item)
                            elif not key and item not in raw_search_results:
                                raw_search_results.append(item)
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
                resolve_sources()
                for event in flush_text_filters():
                    yield event
                for event in reconcile_native():
                    yield event
                yield ResponseComplete()
                completion_notified = True
        resolve_sources()
        for event in flush_text_filters():
            yield event
        for event in reconcile_native():
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
                actions=() if request.temporary else (MessageAction.RETRY.value,),
            )
        if not completion_notified:
            yield ResponseComplete()
