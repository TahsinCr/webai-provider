from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from typing import TYPE_CHECKING, Any, TypeVar, cast

from eventflow import AsyncEventBus, EventEnvelope

from .accounts.status import SessionState, SessionStatus
from .base import BaseProvider, Capability, ProviderType, _construct_provider
from .collections import _publish_collection_change
from .core.hooks import OperationHooks
from .core.operations import run_pipeline_unwrapped
from .core.runtime import ProviderRuntime
from .core.state import LifecycleState
from .core.telemetry import ProviderTelemetry
from .errors import FeatureUnavailable, ProviderError
from .resources.chat.chat import AsyncChat
from .resources.chat.collections import AsyncChatsCollection
from .resources.chat.types import Conversation
from .resources.message.attachment import Attachment
from .resources.message.message import AsyncMessage
from .resources.message.operation import (
    ArtifactDiscovered,
    OutputBegin,
    OutputDelta,
    OutputEnd,
    OutputReplace,
    SourceDiscovered,
    StreamEvent,
    ThinkingBegin,
    ThinkingDelta,
    ThinkingEnd,
    ThinkingReplace,
    _close_async_iterator,
)
from .resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    TextDelta,
    TextReplacement,
    ThoughtDelta,
)
from .resources.model.collections import AsyncModelsCollection
from .resources.model.model import EffortRef, ModelRef, ModelSelection
from .resources.model.types import Models, _model_refs_from_catalog
from .resources.project.collections import AsyncProjectsCollection
from .resources.project.project import AsyncProject
from .resources.project.types import ProjectData, ProjectOptions
from .resources.response.artifact import Artifact
from .resources.response.response import Response
from .resources.response.source import Source
from .transport import AsyncTransport

if TYPE_CHECKING:
    from typing_extensions import TypeVar as DefaultTypeVar

    from .accounts.account import Account
    from .resources.message.protocol import ProviderRequest, ProviderStreamEvent
    from .resources.model.model import ModelCatalog


if TYPE_CHECKING:
    _ChatsT = DefaultTypeVar(
        "_ChatsT",
        bound=AsyncChatsCollection[Any],
        covariant=True,
        default=AsyncChatsCollection[AsyncChat],
    )
    _ProjectsT = DefaultTypeVar(
        "_ProjectsT",
        bound=AsyncProjectsCollection[Any],
        covariant=True,
        default=AsyncProjectsCollection[AsyncProject],
    )
else:
    _ChatsT = TypeVar("_ChatsT", covariant=True)
    _ProjectsT = TypeVar("_ProjectsT", covariant=True)


_ProviderT = TypeVar("_ProviderT", bound="AsyncProvider[Any, Any]")


class AsyncProvider(BaseProvider[_ChatsT, _ProjectsT, AsyncModelsCollection]):
    """Asynchronous provider session backed by one ProviderRuntime."""

    def __new__(cls: type[_ProviderT], *args: Any, **options: Any) -> _ProviderT:
        return _construct_provider(cls, AsyncProvider, args, options, asynchronous=True)

    _chat_page_size = 50
    _chat_initial_page_floor = 1
    _chat_followup_page_size: int | None = None
    _message_type = AsyncMessage
    _response_type = Response
    _source_type = Source
    _artifact_type = Artifact
    _attachment_type = Attachment

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        transport: AsyncTransport | None = None,
    ) -> None:
        super().__init__(account, type=type)
        self._transport = transport
        self.runtime = ProviderRuntime(
            provider_id=self.id,
            account_id=self.account_id,
        )
        self.events: AsyncEventBus[EventEnvelope[str]] = self.runtime.events
        self.hooks: OperationHooks = self.runtime.hooks
        self.telemetry: ProviderTelemetry = self.runtime.telemetry
        self._chats = self._make_chats()
        self._projects = self._make_projects()
        self.models = AsyncModelsCollection(self._load_models, provider=self)
        self._send_pipeline = self.runtime.build_send_pipeline(self.models.resolve)

    def _make_chats(self) -> _ChatsT:
        return cast("_ChatsT", AsyncChatsCollection(self, AsyncChat))

    def _make_projects(self) -> _ProjectsT:
        return cast("_ProjectsT", AsyncProjectsCollection(self, AsyncProject))

    def _active_transport(self) -> AsyncTransport | None:
        return None

    @property
    def transport(self) -> AsyncTransport:
        self._require_provider()
        active = self._active_transport()
        if active is None:
            active = self._transport
        if active is None:
            raise ProviderError("This provider has no configured transport")
        return active

    @property
    def lifecycle_state(self) -> LifecycleState:
        return self.runtime.lifecycle.state

    def _prime_models(self, catalog: ModelCatalog) -> None:
        self.models._items = tuple(_model_refs_from_catalog(self.id, catalog))

    async def _load_models(self, refresh: bool = False) -> list[ModelRef]:
        async with self.runtime.activity():
            catalog = await self._load_catalog(force=refresh)
            return _model_refs_from_catalog(self.type.value, catalog)

    async def open(self: _ProviderT) -> _ProviderT:
        await self.runtime.open(self._prepare_session, self._close_session_resources)
        return self

    def _require_provider(self) -> AsyncProvider[Any, Any]:
        if not self.runtime.opened:
            raise ProviderError("Open the provider before using its resources")
        return self

    async def _close_session_resources(self) -> None:
        self.models._clear()
        try:
            await self._close_session()
        finally:
            if (
                type(self)._close_session is AsyncProvider._close_session
                and self._transport is not None
            ):
                await self._transport.close()

    async def close(self) -> None:
        await self.runtime.close(self._close_session_resources)

    async def execute_stream(self, operation: Any) -> AsyncGenerator[StreamEvent, None]:
        chat = operation.chat
        conversation_id = chat.remote_id or chat.id
        lock = self.runtime.send_lock(conversation_id)
        lock_id = id(lock)
        events = self._execute_stream_events(operation, lock)
        async with self.runtime.activity(send_lock_id=lock_id):
            try:
                async with lock:
                    async for event in events:
                        yield event
            finally:
                await _close_async_iterator(events)

    async def _execute_stream_events(
        self, operation: Any, send_lock: asyncio.Lock
    ) -> AsyncGenerator[StreamEvent, None]:
        prep_context = self.runtime.new_context(
            "chat.send.prepare",
            operation_id=getattr(operation, "operation_id", None),
            resource_id=operation.chat.remote_id or operation.chat.id,
            operation=operation,
            prompt=operation.message.content,
            model=operation.message.model,
            attachments=tuple(operation.message.attachments),
            parameters={"chat": operation.chat, "message": operation.message},
        )
        prepared = await run_pipeline_unwrapped(self._send_pipeline, prep_context)
        request = prepared.request
        output_started = thinking_started = False
        thinking_text = ""
        stream = self._stream_provider_events(request)
        try:
            async for event in stream:
                if isinstance(event, ConversationBound):
                    had_remote_id = bool(operation.chat.remote_id)
                    operation.chat.remote_id = event.conversation_id
                    self.runtime.bind_send_lock(event.conversation_id, send_lock)
                    chat_metadata = getattr(operation.chat, "metadata", None)
                    was_pending_branch = (
                        isinstance(chat_metadata, dict)
                        and chat_metadata.pop("pending_branch", None) is not None
                    )
                    if (
                        event.conversation_id
                        and not operation.chat.temporary
                        and (not had_remote_id or was_pending_branch)
                    ):
                        await _publish_collection_change(
                            self.runtime,
                            "chats",
                            "create",
                            operation.chat,
                            operation_id=getattr(operation, "operation_id", None),
                        )
                    if not had_remote_id and event.conversation_id:
                        await self.runtime.emit_safe(
                            "chat.created",
                            operation_id=getattr(operation, "operation_id", None),
                            resource_id=event.conversation_id,
                            payload={"title": operation.chat.title, "local_id": operation.chat.id},
                        )
                elif isinstance(event, ThoughtDelta) or (
                    isinstance(event, (TextDelta, TextReplacement)) and event.channel == "analysis"
                ):
                    delta = event.text
                    replaced = False
                    if isinstance(event, TextReplacement):
                        replaced = not event.text.startswith(thinking_text)
                        delta = event.text[len(thinking_text) :] if not replaced else ""
                        thinking_text = event.text
                    else:
                        thinking_text += delta
                    if not thinking_started:
                        thinking_started = True
                        yield ThinkingBegin()
                    if isinstance(event, TextReplacement) and replaced:
                        yield ThinkingReplace(text=thinking_text)
                    elif delta:
                        yield ThinkingDelta(delta=delta)
                elif isinstance(event, TextReplacement):
                    if not event.text and not output_started:
                        continue
                    if thinking_started:
                        completed_thinking = thinking_text
                        thinking_text = ""
                        thinking_started = False
                        yield ThinkingEnd(text=completed_thinking)
                    if not output_started:
                        output_started = True
                        yield OutputBegin()
                    yield OutputReplace(text=event.text)
                elif isinstance(event, TextDelta) and event.text:
                    if thinking_started:
                        completed_thinking = thinking_text
                        thinking_text = ""
                        thinking_started = False
                        yield ThinkingEnd(text=completed_thinking)
                    if not output_started:
                        output_started = True
                        yield OutputBegin()
                    yield OutputDelta(delta=event.text)
                elif isinstance(event, MessageMetadata):
                    if event.user_message_id:
                        operation.message.id = event.user_message_id
                        operation.message.remote_id = event.user_message_id
                    if event.message_id:
                        operation._assistant_message_id = event.message_id
                    for record in event.sources:
                        yield SourceDiscovered(
                            self._source_type(
                                id=str(record.get("id") or record.get("source_id") or ""),
                                url=str(record.get("url") or ""),
                                title=str(record.get("title") or ""),
                                snippet=str(record.get("snippet") or ""),
                                kind=str(record.get("kind") or "web"),
                                metadata=record,
                            )
                        )
                    for record in event.files:
                        size = record.get("size")
                        yield ArtifactDiscovered(
                            self._artifact_type(
                                id=str(record.get("id") or record.get("file_id") or ""),
                                kind=str(record.get("kind") or "file"),
                                name=str(
                                    record.get("name")
                                    or record.get("filename")
                                    or record.get("file_name")
                                    or ""
                                ),
                                mime_type=str(
                                    record.get("mime_type") or "application/octet-stream"
                                ),
                                size=size if isinstance(size, int) else None,
                                download_url=record.get("download_url") or record.get("url"),
                                content=record.get("content"),
                                metadata=record,
                            )
                        )

            if thinking_started:
                yield ThinkingEnd(text=thinking_text)
            if output_started:
                yield OutputEnd()
        except (ProviderError, ValueError):
            raise
        except Exception as exc:
            raise ProviderError("Provider request failed") from exc
        finally:
            await _close_async_iterator(stream)

    def attachment_policy(self) -> dict | None:
        from .resources.message.attachment import policy_for

        policy = policy_for(getattr(self, "id", ""))
        return policy.as_dict() if policy else None

    async def _prepare_session(self) -> None:
        return None

    async def _close_session(self) -> None:
        return None

    async def _authenticate(self) -> SessionStatus:
        raise FeatureUnavailable(Capability.AUTH, "not_implemented")

    async def _session_status(self) -> SessionStatus:
        return SessionStatus(SessionState.UNKNOWN, reason="not_implemented")

    async def _forget_session(self) -> SessionStatus:
        raise FeatureUnavailable(Capability.AUTH, "forget_session_not_implemented")

    async def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
        raise FeatureUnavailable(Capability.MODELS, "not_implemented")

    def _stream_provider_events(
        self, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamEvent]:
        raise NotImplementedError

    async def _conversation_attachment_count(self, conversation_id: str) -> int | None:
        raise FeatureUnavailable(Capability.ATTACHMENTS, "not_implemented")

    async def _upload_file(self, path: Any) -> dict[str, Any]:
        raise FeatureUnavailable(Capability.ATTACHMENTS, "not_implemented")

    async def _resolve_download(
        self,
        url: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
    ) -> Any | None:
        return None

    async def _project_options(self, project_id: str | None = None) -> ProjectOptions:
        return ProjectOptions()

    async def _patch_project(
        self,
        project_id: str,
        *,
        name: str | None = None,
        instructions: str | None = None,
        current: ProjectData | None = None,
    ) -> ProjectData:
        raise FeatureUnavailable(Capability.PROJECT_UPDATE, "not_implemented")

    async def _add_project_resource(
        self,
        project_id: str,
        kind: str,
        *,
        current: ProjectData | None = None,
        **values: Any,
    ) -> ProjectData:
        raise FeatureUnavailable(Capability.RESOURCES, "not_implemented")

    async def _remove_project_resource(
        self,
        project_id: str,
        resource_id: str,
        *,
        current: ProjectData | None = None,
    ) -> ProjectData:
        raise FeatureUnavailable(Capability.RESOURCES, "not_implemented")

    def _invalidate_project_cache(self, project_id: str) -> None:
        del project_id

    async def authenticate(self) -> SessionStatus:
        async with self.runtime.activity():
            return await self._authenticate()

    async def session_status(self) -> SessionStatus:
        async with self.runtime.activity():
            return await self._session_status()

    async def forget_session(self) -> SessionStatus:
        async with self.runtime.activity(), self.models._lock:
            try:
                return await self._forget_session()
            finally:
                self.models._clear()

    async def conversation_attachment_count(self, conversation_id: str) -> int | None:
        async with self.runtime.activity():
            return await self._conversation_attachment_count(conversation_id)

    async def upload_file(self, path: Any) -> dict[str, Any]:
        async with self.runtime.activity():
            return await self._upload_file(path)

    async def project_options(self, project_id: str | None = None) -> ProjectOptions:
        async with self.runtime.activity():
            return await self._project_options(project_id)

    async def update_project(self, project: ProjectData) -> ProjectData:
        async with self.runtime.activity():
            updated = await self._patch_project_data(
                project.id,
                name=project.name,
                instructions=project.instructions or "",
            )
        await _publish_collection_change(self.runtime, "projects", "update", updated)
        return updated

    async def _list_chats_data(self, *, cursor: str | None = None, limit: int = 50) -> Any:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    async def _get_chat_data(self, conversation_id: str) -> Conversation:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    async def _get_chat_metadata(self, conversation_id: str) -> Conversation:
        return await self._get_chat_data(conversation_id)

    async def _rename_chat_data(self, conversation_id: str, title: str) -> None:
        raise FeatureUnavailable(Capability.RENAME, "not_implemented")

    async def _delete_chat_data(self, conversation_id: str) -> None:
        raise FeatureUnavailable(Capability.DELETE, "not_implemented")

    async def _branch_chat_data(self, conversation_id: str, message_id: str) -> Conversation:
        raise FeatureUnavailable(Capability.CHAT_BRANCH, "not_implemented")

    async def _move_chat_data(self, conversation_id: str, project_id: str | None) -> None:
        raise FeatureUnavailable(Capability.PROJECT_MEMBERSHIP, "not_implemented")

    async def _list_projects_data(self, *, cursor: str | None = None) -> Any:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    async def _get_project_data(self, project_id: str) -> ProjectData:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    async def _list_project_chats_data(
        self, project_id: str, *, cursor: str | None = None, limit: int | None = None
    ) -> Any:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    async def _create_project_data(
        self, name: str, instructions: str = "", **options: Any
    ) -> ProjectData:
        raise FeatureUnavailable(Capability.PROJECT_CREATE, "not_implemented")

    async def _patch_project_data(
        self,
        project_id: str,
        *,
        name: str | None = None,
        instructions: str | None = None,
        current: ProjectData | Callable[[], ProjectData | None] | None = None,
    ) -> ProjectData:
        async with self.runtime.project_lock(project_id):
            staged = current() if callable(current) else current
            return await self._patch_project(
                project_id, name=name, instructions=instructions, current=staged
            )

    async def _delete_project_data(self, project_id: str) -> None:
        raise FeatureUnavailable(Capability.PROJECT_DELETE, "not_implemented")

    async def _add_project_resource_data(
        self,
        project_id: str,
        kind: str,
        *,
        current: ProjectData | Callable[[], ProjectData | None] | None = None,
        **values: Any,
    ) -> ProjectData:
        async with self.runtime.project_lock(project_id):
            staged = current() if callable(current) else current
            return await self._add_project_resource(project_id, kind, current=staged, **values)

    async def _remove_project_resource_data(
        self,
        project_id: str,
        resource_id: str,
        *,
        current: ProjectData | Callable[[], ProjectData | None] | None = None,
    ) -> ProjectData:
        async with self.runtime.project_lock(project_id):
            staged = current() if callable(current) else current
            return await self._remove_project_resource(project_id, resource_id, current=staged)

    async def _resolve_download_data(
        self,
        url: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
    ) -> Any | None:
        async with self.runtime.activity():
            return await self._resolve_download(
                url, conversation_id=conversation_id, message_id=message_id
            )

    async def ask(
        self,
        prompt: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Any = None,
    ) -> Response:
        opened_here = False
        try:
            async with self.runtime.activity():
                opened_here = await self.runtime.ensure_open(
                    self._prepare_session,
                    self._close_session_resources,
                )
                chat = await self.chats.create()
                return await chat.responses.create(
                    prompt,
                    model=model,
                    attachments=attachments,
                ).start()
        finally:
            if opened_here:
                await self.close()

    async def __aenter__(self: _ProviderT) -> _ProviderT:
        return await self.open()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
