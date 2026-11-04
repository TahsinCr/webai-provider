"""Grok web-session adapter."""

import asyncio
import time

from ..api import (
    Capability,
    ConversationBound,
    MessageAction,
    MessageMetadata,
    ReplaceText,
    ResponseComplete,
    TextDelta,
    validate_request,
)
from ..contracts import OptionalFeatures
from ..errors import FeatureUnavailable, ProviderError
from ..messages import sources_from
from ..models import (
    Feature,
    FeatureState,
    Model,
    ModelCatalog,
    Origin,
    Page,
    ProviderDescription,
    SessionState,
    SessionStatus,
)
from ..projects import ProjectEditing
from ..session import delete_session, session_exists
from .records import _text, active_responses, conversation, project
from .transport import GrokProjectEditor, GrokTransport, remote_id

_CATALOG_TTL = 300


class GrokProvider(ProjectEditing, OptionalFeatures):
    id = "grok"
    capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.TEMPORARY,
            Capability.AUTH,
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

    def _origin(self, remote):
        return Origin(self.id, self.account_id, remote)

    async def prepare(self):
        await self.authenticate()

    async def close(self):
        async with self._gate:
            await self.transport.close()

    async def authenticate(self):
        await self.transport.authenticate()
        return SessionStatus(SessionState.AUTHENTICATED, True)

    async def login(self):
        return await self.authenticate()

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
            self.transport.session.authenticated = False
        return SessionStatus(SessionState.LOGIN_REQUIRED)

    async def describe(self):
        return ProviderDescription(
            self.id,
            tuple(
                Feature(
                    capability,
                    FeatureState.AVAILABLE
                    if capability in self.capabilities
                    else FeatureState.UNSUPPORTED,
                )
                for capability in Capability
            ),
        )

    async def models(self, *, force=False):
        now = time.monotonic()
        if not force and self._catalog and now - self._catalog_at < _CATALOG_TTL:
            return self._catalog
        async with self._gate:
            now = time.monotonic()
            if not force and self._catalog and now - self._catalog_at < _CATALOG_TTL:
                return self._catalog
            data = await self.transport.request("POST", "/rest/models", json={"locale": "en"})
            if not isinstance(data, dict) or not isinstance(data.get("models", []), list):
                raise ProviderError("Grok model catalog response contract changed")
            models = []
            for available, selectable in (
                (data.get("models", []), True),
                (data.get("unavailableModels", []), False),
            ):
                if not isinstance(available, list):
                    continue
                for item in available:
                    if not isinstance(item, dict):
                        continue
                    model_id = item.get("modelId")
                    title = item.get("name")
                    if not isinstance(model_id, str) or not model_id or not isinstance(title, str):
                        continue
                    models.append(
                        Model(
                            model_id,
                            title,
                            metadata={
                                key: item[key]
                                for key in (
                                    "description",
                                    "modeName",
                                    "modeDescription",
                                    "modelMode",
                                    "promptingBackend",
                                    "badgeText",
                                )
                                if item.get(key) is not None
                            },
                            chat_selectable=selectable,
                        )
                    )
            ids = {item.id for item in models if item.chat_selectable}
            default = next(
                (
                    data.get(key)
                    for key in (
                        "defaultProModel",
                        "defaultFreeModel",
                        "defaultHeavyModel",
                        "defaultAnonModel",
                    )
                    if data.get(key) in ids
                ),
                None,
            )
            self._catalog = ModelCatalog(FeatureState.AVAILABLE, tuple(models), default)
            self._catalog_at = time.monotonic()
            return self._catalog

    async def list_chats(self, cursor=None, limit=50):
        query = {"pageSize": max(1, min(int(limit), 100))}
        if cursor:
            query["pageToken"] = cursor
        data = await self.transport.request("GET", "/rest/app-chat/conversations", params=query)
        if not isinstance(data, dict) or not isinstance(data.get("conversations", []), list):
            raise ProviderError("Grok conversation list response contract changed")
        return Page(
            tuple(conversation(item, self.account_id) for item in data.get("conversations", [])),
            data.get("nextPageToken"),
        )

    async def _history(self, conversation_id):
        return await self.transport.request(
            "GET",
            "/rest/app-chat/conversations/" + remote_id(conversation_id) + "/responses",
            params={"includeThreads": True},
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
        if not isinstance(metadata, dict):
            metadata = {"conversationId": conversation_id}
        return conversation(metadata, self.account_id, history)

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

    async def list_projects(self, cursor=None):
        query = {"pageSize": 50}
        if cursor:
            query["pageToken"] = cursor
        data = await self.transport.request("GET", "/rest/workspaces", params=query)
        if not isinstance(data, dict) or not isinstance(data.get("workspaces", []), list):
            raise ProviderError("Grok workspace list response contract changed")
        return Page(
            tuple(project(item, self.account_id) for item in data.get("workspaces", [])),
            data.get("nextPageToken"),
        )

    async def get_project(self, project_id):
        path = "/rest/workspaces/" + remote_id(project_id)
        workspace, listing = await asyncio.gather(
            self.transport.request("GET", path),
            self.transport.request("GET", path + "/files", params={"recursive": "true"}),
        )
        files = listing.get("files", []) if isinstance(listing, dict) else []
        return project(workspace, self.account_id, files)

    async def list_project_chats(self, project_id, cursor=None):
        query = {"pageSize": 50, "workspaceId": project_id}
        if cursor:
            query["pageToken"] = cursor
        data = await self.transport.request("GET", "/rest/app-chat/conversations", params=query)
        if not isinstance(data, dict) or not isinstance(data.get("conversations", []), list):
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
                for item in data.get("conversations", [])
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
        metadata = await self._conversation_metadata(conversation_id)
        if conversation(metadata, self.account_id).project_id != project_id:
            raise ProviderError("Grok sohbetin proje üyeliği sunucuda doğrulanamadı")

    async def stream(self, request):
        validate_request(self, request)
        if request.effort is not None:
            raise FeatureUnavailable(Capability.EFFORT, "Grok does not expose shared effort levels")
        if request.attachments:
            raise FeatureUnavailable(
                Capability.ATTACHMENTS, "Grok chat attachments are not implemented"
            )
        if request.branch:
            raise FeatureUnavailable(
                Capability.CHAT_BRANCH, "Grok native chat branching is unavailable"
            )
        if request.temporary and request.project_id:
            raise FeatureUnavailable(
                Capability.TEMPORARY, "Temporary chats cannot belong to a project"
            )
        if request.action not in (None, MessageAction.RETRY):
            raise FeatureUnavailable(
                Capability.MESSAGE_EDIT, "Grok does not support this message action"
            )
        if request.action == MessageAction.RETRY and not request.message_id:
            raise ValueError("Grok retry requires the selected assistant response")

        model = request.model
        if not model:
            model = (await self.models()).default_model
        if not model:
            raise ProviderError("Grok did not provide a selectable default model")

        async with self._gate:
            conversation_id = request.conversation_id
            history = None
            parent_response_id = ""
            is_retry = request.action == MessageAction.RETRY
            if conversation_id:
                history = await self._history(conversation_id)
                if not isinstance(history, dict):
                    raise ProviderError("Grok conversation history response contract changed")
                if history.get("inflightResponses"):
                    raise ProviderError("Grok conversation already has a response in progress")
                active = active_responses(history)
                if is_retry:
                    target = next(
                        (item for item in active if item.get("responseId") == request.message_id),
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
                elif active:
                    parent_response_id = active[-1].get("responseId") or ""

            body = {
                "message": "" if is_retry else request.prompt,
                "modelName": model,
                "parentResponseId": parent_response_id,
                "disableSearch": False,
                "isReasoning": False,
                "temporary": request.temporary,
                "isRegenRequest": is_retry,
            }
            if is_retry:
                body["requestType"] = "regenerate"
            if request.project_id:
                body["workspaceIds"] = [request.project_id]
            path = (
                "/rest/app-chat/conversations/new"
                if not conversation_id
                else "/rest/app-chat/conversations/" + remote_id(conversation_id) + "/responses"
            )
            response_id = user_response_id = None
            streamed_text = streamed_analysis = native_text = native_analysis = ""
            sources = ()
            response_model = model
            complete = False
            completion_notified = False
            bound = False

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
                error = response.get("error") or chunk.get("error")
                if error:
                    if isinstance(error, dict) and error.get("code") in (
                        "UNAUTHORIZED",
                        "AUTH_REQUIRED",
                    ):
                        self.transport.session.authenticated = False
                    raise ProviderError("Grok stream reported an error; the request was not resent")
                user = response.get("userResponse")
                if isinstance(user, dict) and user.get("responseId"):
                    user_response_id = user["responseId"]
                native = response.get("modelResponse")
                if isinstance(native, dict):
                    response_id = native.get("responseId") or response_id
                    response_model = native.get("model") or response_model
                    sources = sources_from(
                        {"sources": [*sources, *(native.get("webSearchResults") or [])]}
                    )
                    if isinstance(native.get("message"), str):
                        native_text = native["message"]
                    if native.get("thinkingTrace"):
                        native_analysis = _text(native["thinkingTrace"])
                    if native.get("partial") is False:
                        complete = True
                token = response.get("token")
                if isinstance(token, str) and token:
                    if response.get("isThinking") or response.get("messageTag") in (
                        "header",
                        "summary",
                    ):
                        streamed_analysis += token
                        yield TextDelta(token, "analysis")
                    else:
                        streamed_text += token
                        yield TextDelta(token)
                if response.get("responseId") and not response_id:
                    response_id = response["responseId"]
                if response.get("done") is True or response.get("finished") is True:
                    complete = True
                if (
                    complete
                    and not completion_notified
                    and (request.conversation_id or conversation_id)
                ):
                    if native_analysis and native_analysis != streamed_analysis:
                        yield ReplaceText(native_analysis, "analysis")
                    if native_text and native_text != streamed_text:
                        yield ReplaceText(native_text)
                        streamed_text = native_text
                    yield ResponseComplete()
                    completion_notified = True
            if not conversation_id:
                raise ProviderError("Grok did not return a conversation ID")
            if not complete:
                raise ProviderError(
                    "Grok stream ended before response completion; it was not resent"
                )
            if not bound and not request.conversation_id:
                yield ConversationBound(conversation_id)
            if not completion_notified and native_analysis and native_analysis != streamed_analysis:
                yield ReplaceText(native_analysis, "analysis")
            if not completion_notified and native_text and native_text != streamed_text:
                yield ReplaceText(native_text)
                streamed_text = native_text
            if response_id and streamed_text:
                yield MessageMetadata(
                    message_id=response_id,
                    user_message_id=user_response_id,
                    model=response_model,
                    effort=None,
                    sources=sources,
                    actions=(MessageAction.RETRY.value,),
                )
            if not completion_notified:
                yield ResponseComplete()
