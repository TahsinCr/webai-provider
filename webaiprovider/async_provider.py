from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any

from eventflow import AsyncEventBus, EventEnvelope

from .accounts.status import SessionStatus
from .base import BaseProvider, BaseProviderAdapter, BaseSessionAdapter, Capability
from .core.hooks import OperationHooks
from .core.operations import run_pipeline_unwrapped
from .core.runtime import ProviderRuntime
from .core.state import LifecycleState
from .core.telemetry import ProviderTelemetry
from .errors import ProviderError
from .provider import ProviderType, _construct_provider
from .resources.artifact import Artifact
from .resources.chat.chat import AsyncChat
from .resources.chat.collections import AsyncChatsCollection
from .resources.chat.types import Conversation
from .resources.common import Origin
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
from .resources.message.response import Response
from .resources.model.collections import (
    AsyncModelsCollection,
    Models,
    _model_refs_from_catalog,
)
from .resources.model.model import EffortRef, ModelRef, ModelSelection
from .resources.project.collections import AsyncProjectsCollection
from .resources.project.project import AsyncProject
from .resources.project.types import ProjectData, ProjectOptions
from .resources.source import Source
from .transport import AsyncTransport

if TYPE_CHECKING:
    from .accounts.account import Account


class AsyncProvider(BaseProvider):
    """Asynchronous provider session backed by one ProviderRuntime and one BaseProviderAdapter."""

    def __new__(cls, *args: Any, **options: Any):
        return _construct_provider(cls, AsyncProvider, args, options, asynchronous=True)

    _chat_type = AsyncChat
    _project_type = AsyncProject
    _projects_collection_type = AsyncProjectsCollection
    capabilities: frozenset[Capability] = frozenset()

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        transport: AsyncTransport | None = None,
        adapter: BaseProviderAdapter | None = None,
    ) -> None:
        super().__init__(account, type=type)
        self._transport = transport
        self.adapter: BaseProviderAdapter = (
            adapter if adapter is not None else self._create_adapter()
        )
        if adapter is not None and adapter.capabilities and not self.__class__.capabilities:
            self.capabilities = adapter.capabilities
        self.runtime = ProviderRuntime(
            provider_id=self.id,
            account_id=self.account_id,
            adapter=self.adapter,
        )
        self.events: AsyncEventBus[EventEnvelope[str]] = self.runtime.events
        self.hooks: OperationHooks = self.runtime.hooks
        self.telemetry: ProviderTelemetry = self.runtime.telemetry
        self.chats = AsyncChatsCollection(self)
        self.projects = self._projects_collection_type(self)
        self.models = AsyncModelsCollection(self._load_models)
        self._send_pipeline = self.runtime.build_send_pipeline(self.models.resolve)

    def _create_adapter(self) -> BaseProviderAdapter:
        return BaseProviderAdapter(
            provider_id=self.id,
            account_id=self.account_id,
            capabilities=self.capabilities,
            transport=self._transport,
        )

    @property
    def transport(self) -> AsyncTransport:
        self._require_provider()
        active = self.adapter.transport if self.adapter.transport is not None else self._transport
        if active is None:
            raise ProviderError("This provider has no configured transport")
        return active

    @property
    def id(self) -> str:
        return self.type.value

    @property
    def account_id(self) -> str:
        return self.account.id

    @property
    def lifecycle_state(self) -> LifecycleState:
        return self.runtime.lifecycle.state

    async def _load_models(self, refresh: bool = False) -> list[ModelRef]:
        async with self.runtime.activity():
            catalog = await self.adapter.models.load_catalog(force=refresh)
            return _model_refs_from_catalog(self.type.value, catalog)

    async def open(self) -> AsyncProvider:
        await self.runtime.open(self.adapter.session.prepare, self._close_session_resources)
        return self

    def _require_provider(self) -> AsyncProvider:
        if not self.runtime.opened:
            raise ProviderError("Open the provider before using its resources")
        return self

    async def _close_session_resources(self) -> None:
        try:
            await self.adapter.session.close()
        finally:
            if (
                type(self.adapter.session).close is BaseSessionAdapter.close
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
        stream = self.adapter.messages.stream(request)
        try:
            async for event in stream:
                if isinstance(event, ConversationBound):
                    operation.chat.remote_id = event.conversation_id
                    self.runtime.bind_send_lock(event.conversation_id, send_lock)
                    chat_metadata = getattr(operation.chat, "metadata", None)
                    if isinstance(chat_metadata, dict):
                        chat_metadata.pop("pending_branch", None)
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
                            Source(
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
                            Artifact(
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
        from .utils.attachments import policy_for

        policy = policy_for(getattr(self, "id", ""))
        return policy.as_dict() if policy else None

    def _origin(self, remote: str) -> Origin:
        return Origin(getattr(self, "id", ""), getattr(self, "account_id", "default"), remote)

    async def authenticate(self) -> SessionStatus:
        async with self.runtime.activity():
            return await self.adapter.session.authenticate()

    async def session_status(self) -> SessionStatus:
        async with self.runtime.activity():
            return await self.adapter.session.session_status()

    async def forget_session(self) -> SessionStatus:
        async with self.runtime.activity():
            return await self.adapter.session.forget_session()

    async def conversation_attachment_count(self, conversation_id: str) -> int | None:
        async with self.runtime.activity():
            return await self.adapter.chats.conversation_attachment_count(conversation_id)

    async def upload_file(self, path: Any) -> dict[str, Any]:
        async with self.runtime.activity():
            return await self.adapter.attachments.upload_file(path)

    async def project_options(self, project_id: str | None = None) -> ProjectOptions:
        async with self.runtime.activity():
            return await self.adapter.projects.project_options(project_id)

    async def update_project(self, project: ProjectData) -> ProjectData:
        if project.origin.provider_id != self.id or project.origin.account_id != self.account_id:
            raise ValueError("Project belongs to a different provider account")
        async with self.runtime.activity():
            return await self._patch_project_data(
                project.origin.remote_id,
                name=project.name,
                instructions=project.instructions or "",
            )

    async def _list_chats_data(self, *, cursor: str | None = None, limit: int = 50) -> Any:
        return await self.adapter.chats.list_chats(cursor=cursor, limit=limit)

    async def _get_chat_data(self, conversation_id: str) -> Conversation:
        return await self.adapter.chats.get_chat(conversation_id)

    async def _rename_chat_data(self, conversation_id: str, title: str) -> None:
        await self.adapter.chats.rename_chat(conversation_id, title)

    async def _delete_chat_data(self, conversation_id: str) -> None:
        await self.adapter.chats.delete_chat(conversation_id)

    async def _branch_chat_data(self, conversation_id: str, message_id: str) -> Conversation:
        return await self.adapter.chats.branch_chat(conversation_id, message_id)

    async def _move_chat_data(self, conversation_id: str, project_id: str | None) -> None:
        await self.adapter.chats.move_chat(conversation_id, project_id)

    async def _list_projects_data(self, *, cursor: str | None = None) -> Any:
        return await self.adapter.projects.list_projects(cursor=cursor)

    async def _get_project_data(self, project_id: str) -> ProjectData:
        return await self.adapter.projects.get_project(project_id)

    async def _list_project_chats_data(self, project_id: str, *, cursor: str | None = None) -> Any:
        return await self.adapter.projects.list_project_chats(project_id, cursor=cursor)

    async def _create_project_data(
        self, name: str, instructions: str = "", **options: Any
    ) -> ProjectData:
        return await self.adapter.projects.create_project(
            name, instructions=instructions, **options
        )

    async def _patch_project_data(
        self,
        project_id: str,
        *,
        name: str | None = None,
        instructions: str | None = None,
    ) -> ProjectData:
        async with self.runtime.project_lock(project_id):
            return await self.adapter.projects.patch_project(
                project_id, name=name, instructions=instructions
            )

    async def _delete_project_data(self, project_id: str) -> None:
        await self.adapter.projects.delete_project(project_id)

    async def _add_project_resource_data(
        self, project_id: str, kind: str, **values: Any
    ) -> ProjectData:
        async with self.runtime.project_lock(project_id):
            return await self.adapter.projects.add_project_resource(project_id, kind, **values)

    async def _remove_project_resource_data(self, project_id: str, resource_id: str) -> ProjectData:
        async with self.runtime.project_lock(project_id):
            return await self.adapter.projects.remove_project_resource(project_id, resource_id)

    async def _resolve_download_data(
        self,
        url: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
    ) -> Any | None:
        async with self.runtime.activity():
            return await self.adapter.attachments.resolve_download(
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
                    self.adapter.session.prepare,
                    self.adapter.session.close,
                )
                chat = await self.chats.create()
                return await chat.messages.create(
                    prompt,
                    model=model,
                    attachments=attachments,
                ).start()
        finally:
            if opened_here:
                await self.close()

    async def __aenter__(self) -> AsyncProvider:
        return await self.open()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
