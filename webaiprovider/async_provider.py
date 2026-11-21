from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar, cast
from weakref import WeakValueDictionary

from eventflow import AsyncEventBus, ErrorPolicy, EventEnvelope, StateMachine

from .accounts.status import SessionStatus
from .base import (
    BaseProvider,
    Capability,
    ProviderType,
    _construct_provider,
    _StreamEventTranslator,
)
from .collections import _publish_collection_change
from .core.async_hooks import AsyncOperationHooks
from .core.state import _LIFECYCLE_TERMINAL_STATES, _LIFECYCLE_TRANSITIONS, LifecycleState
from .errors import FeatureUnavailable, ProviderError
from .resources.chat.chat import AsyncChat
from .resources.chat.collections import AsyncChatsCollection
from .resources.chat.types import Conversation
from .resources.message.attachment import Attachment
from .resources.message.message import AsyncMessage
from .resources.message.protocol import ConversationBound
from .resources.message.request import build_message_request_pipeline, run_pipeline_unwrapped
from .resources.model.collections import AsyncModelsCollection
from .resources.model.model import EffortRef, ModelRef, ModelSelection
from .resources.model.types import Models, _model_refs_from_catalog
from .resources.project.collections import AsyncProjectsCollection
from .resources.project.project import AsyncProject
from .resources.project.types import ProjectData, ProjectOptions
from .resources.response.artifact import Artifact
from .resources.response.operation import StreamEvent, _close_async_iterator
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


@dataclass(slots=True, eq=False)
class _ActiveOperation:
    provider_id: int
    send_lock_id: int | None = None
    active: bool = True


_ACTIVE_OPERATIONS: contextvars.ContextVar[tuple[_ActiveOperation, ...]] = contextvars.ContextVar(
    "webai_active_operations", default=()
)
_CLOSING_PROVIDER: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "webai_closing_provider", default=None
)
_log = logging.getLogger(__name__)


class AsyncProvider(BaseProvider[_ChatsT, _ProjectsT, AsyncModelsCollection]):
    """Asynchronous provider session owning its lifecycle, locks, hooks, and events."""

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
        self.events: AsyncEventBus[EventEnvelope[str]] = AsyncEventBus(
            error_policy=ErrorPolicy.ISOLATE
        )
        self._event_sequence = 0
        self.hooks: AsyncOperationHooks = AsyncOperationHooks()
        self.lifecycle = StateMachine(
            initial=LifecycleState.CREATED,
            transitions=_LIFECYCLE_TRANSITIONS,
            terminal=_LIFECYCLE_TERMINAL_STATES,
        )
        self._opened = False
        self._lifecycle_lock = asyncio.Lock()
        self._lifecycle_condition = asyncio.Condition()
        self._close_task: asyncio.Task[None] | None = None
        self._active_operations = 0
        self._locks: WeakValueDictionary[tuple[str, str], asyncio.Lock] = WeakValueDictionary()
        self._chats = self._make_chats()
        self._projects = self._make_projects()
        self.models = AsyncModelsCollection(self._load_models, provider=self)
        self._send_pipeline = build_message_request_pipeline(
            provider_id=self.id,
            account_id=self.account_id,
            model_resolver=self.models.resolve,
        )

    def _make_chats(self) -> _ChatsT:
        return cast("_ChatsT", AsyncChatsCollection(self, AsyncChat))

    def _make_projects(self) -> _ProjectsT:
        return cast("_ProjectsT", AsyncProjectsCollection(self, AsyncProject))

    @property
    def transport(self) -> AsyncTransport:
        self._require_provider()
        active = self._active_transport()
        if active is None:
            active = self._transport
        if active is None:
            raise ProviderError("This provider has no configured transport")
        return active

    def _has_active_operation(self, *, send_lock_id: int | None = None) -> bool:
        pid = id(self)
        return any(
            entry.active
            and entry.provider_id == pid
            and (send_lock_id is None or entry.send_lock_id == send_lock_id)
            for entry in _ACTIVE_OPERATIONS.get()
        )

    def _lock(self, kind: str, remote_id: str) -> asyncio.Lock:
        key = (kind, remote_id)
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    def _send_lock(self, conversation_id: str) -> asyncio.Lock:
        return self._lock("chat", conversation_id)

    def _bind_send_lock(self, conversation_id: str, lock: asyncio.Lock) -> None:
        self._locks["chat", conversation_id] = lock

    def _project_lock(self, project_id: str) -> asyncio.Lock:
        return self._lock("project", project_id)

    @contextlib.contextmanager
    def _operation_marker(self, *, send_lock_id: int | None = None):
        marker = _ActiveOperation(id(self), send_lock_id)
        token = _ACTIVE_OPERATIONS.set((*_ACTIVE_OPERATIONS.get(), marker))
        try:
            yield
        finally:
            marker.active = False
            _ACTIVE_OPERATIONS.reset(token)

    @contextlib.asynccontextmanager
    async def _activity(self, *, send_lock_id: int | None = None):
        if send_lock_id is not None and self._has_active_operation(send_lock_id=send_lock_id):
            raise ProviderError("A send hook cannot await another send in the same conversation")
        nested = self._has_active_operation()
        with self._operation_marker(send_lock_id=send_lock_id):
            await self._begin_activity(nested=nested)
            try:
                yield
            finally:
                await self._end_activity()

    async def _begin_activity(self, *, nested: bool = False) -> None:
        async with self._lifecycle_lock:
            if self.closed or (self.closing and not nested):
                raise ProviderError("Provider is closing or is closed")
            async with self._lifecycle_condition:
                self._active_operations += 1

    async def _end_activity(self) -> None:
        async with self._lifecycle_condition:
            self._active_operations -= 1
            self._lifecycle_condition.notify_all()

    async def _emit(
        self,
        name: str,
        *,
        operation_id: str | None = None,
        resource_id: str | None = None,
        payload: object | None = None,
    ) -> None:
        """Publish an observer event to async subscribers."""
        self._event_sequence += 1
        if not self.events.subscriber_count:
            return
        event = EventEnvelope(
            event=name,
            correlation_id=operation_id,
            source=self.id,
            metadata={
                "account_id": self.account_id,
                "resource_id": resource_id,
                "sequence": self._event_sequence,
                "payload": payload,
            },
        )
        await self.events.publish(event)

    async def _emit_safe(
        self,
        name: str,
        *,
        operation_id: str | None = None,
        resource_id: str | None = None,
        payload: object | None = None,
    ) -> None:
        """Publish an observer event without letting a dispatch failure affect the caller."""
        try:
            await self._emit(
                name, operation_id=operation_id, resource_id=resource_id, payload=payload
            )
        except Exception as error:
            _log.warning("Event dispatch failed: event=%s error=%s", name, type(error).__name__)

    async def _load_models(self, refresh: bool = False) -> list[ModelRef]:
        async with self._activity():
            catalog = await self._load_catalog(force=refresh)
            return _model_refs_from_catalog(self.type.value, catalog)

    async def _open_session(self, *, reuse_open: bool = False) -> bool:
        pending: list[tuple[str, object | None]] = []
        opened_here = False
        try:
            async with self._lifecycle_lock:
                if reuse_open and self._opened:
                    return False
                opened_here = await self._open_unlocked(pending)
        finally:
            for name, payload in pending:
                await self._emit_safe(name, payload=payload)
        return opened_here

    async def _open_unlocked(self, pending: list[tuple[str, object | None]]) -> bool:
        if self.lifecycle.state == LifecycleState.CLOSE_FAILED:
            raise ProviderError("Provider cleanup failed; call close() again before reopening")
        if self.closed or (self.closing and not self._has_active_operation()):
            raise ProviderError("A closed provider cannot be reopened")
        if self._opened:
            return False
        if self.lifecycle.can_transition(LifecycleState.OPENING):
            self.lifecycle.transition(LifecycleState.OPENING)
        pending.append(("provider.opening", None))
        try:
            await self._prepare_session()
        except BaseException as error:
            if self.lifecycle.can_transition(LifecycleState.OPEN_FAILED):
                self.lifecycle.transition(LifecycleState.OPEN_FAILED)
            pending.append(("provider.open_failed", {"error_type": type(error).__name__}))
            with contextlib.suppress(Exception):
                await self._close_session_resources()
            raise
        self._opened = True
        if self.lifecycle.can_transition(LifecycleState.OPEN):
            self.lifecycle.transition(LifecycleState.OPEN)
        pending.append(("provider.opened", None))
        return True

    async def open(self: _ProviderT) -> _ProviderT:
        await self._open_session()
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
        if _CLOSING_PROVIDER.get() == id(self):
            raise ProviderError("close() cannot be awaited from a provider closing event callback")
        if self._has_active_operation():
            raise ProviderError("Cannot close a provider from one of its active operations")
        async with self._lifecycle_lock:
            if self.closed:
                return
            if self._close_task is None:
                if self.lifecycle.can_transition(LifecycleState.CLOSING):
                    self.lifecycle.transition(LifecycleState.CLOSING)
                self._close_task = asyncio.create_task(self._finish_close())
            task = self._close_task

        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        task.result()
        if cancelled:
            raise asyncio.CancelledError

    async def _finish_close(self) -> None:
        token = _CLOSING_PROVIDER.set(id(self))
        try:
            await self._emit_safe("provider.closing")
            async with self._lifecycle_condition:
                await self._lifecycle_condition.wait_for(lambda: self._active_operations == 0)
            self._opened = False
            try:
                await self._close_session_resources()
            except BaseException as error:
                if self.lifecycle.can_transition(LifecycleState.CLOSE_FAILED):
                    self.lifecycle.transition(LifecycleState.CLOSE_FAILED)
                await self._emit_safe(
                    "provider.close_failed", payload={"error_type": type(error).__name__}
                )
                raise
            if self.lifecycle.can_transition(LifecycleState.CLOSED):
                self.lifecycle.transition(LifecycleState.CLOSED)
            await self._emit_safe("provider.closed")
            await self.events.close()
        finally:
            _CLOSING_PROVIDER.reset(token)
            self._close_task = None
            async with self._lifecycle_condition:
                self._lifecycle_condition.notify_all()

    async def _execute_stream(self, operation: Any) -> AsyncGenerator[StreamEvent, None]:
        chat = operation.chat
        conversation_id = chat.remote_id or chat.id
        lock = self._send_lock(conversation_id)
        lock_id = id(lock)
        events = self._execute_stream_events(operation, lock)
        async with self._activity(send_lock_id=lock_id):
            try:
                async with lock:
                    async for event in events:
                        yield event
            finally:
                await _close_async_iterator(events)

    async def _execute_stream_events(
        self, operation: Any, send_lock: asyncio.Lock
    ) -> AsyncGenerator[StreamEvent, None]:
        prep_context = self._new_context(
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
        translator = _StreamEventTranslator(self, operation)
        stream = self._stream_provider_events(prepared.request)
        try:
            async for event in stream:
                if isinstance(event, ConversationBound):
                    had_remote_id = bool(operation.chat.remote_id)
                    operation.chat.remote_id = event.conversation_id
                    self._bind_send_lock(event.conversation_id, send_lock)
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
                            self,
                            "chats",
                            "create",
                            operation.chat,
                            operation_id=getattr(operation, "operation_id", None),
                        )
                    if not had_remote_id and event.conversation_id:
                        await self._emit_safe(
                            "chat.created",
                            operation_id=getattr(operation, "operation_id", None),
                            resource_id=event.conversation_id,
                            payload={"title": operation.chat.title, "local_id": operation.chat.id},
                        )
                else:
                    for item in translator.feed(event):
                        yield item
            for item in translator.finish():
                yield item
        except (ProviderError, ValueError):
            raise
        except Exception as exc:
            raise ProviderError("Provider request failed") from exc
        finally:
            await _close_async_iterator(stream)

    async def _prepare_session(self) -> None:
        return None

    async def _close_session(self) -> None:
        return None

    async def _authenticate(self) -> SessionStatus:
        raise FeatureUnavailable(Capability.AUTH, "not_implemented")

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

    async def authenticate(self) -> SessionStatus:
        return await self._authenticate_session()

    async def _authenticate_session(self, **options: Any) -> SessionStatus:
        async with self._activity(), self.models._lock:
            try:
                return await self._authenticate(**options)
            finally:
                self.models._clear()

    async def session_status(self) -> SessionStatus:
        async with self._activity():
            return self._session_status()

    async def forget_session(self) -> SessionStatus:
        async with self._activity(), self.models._lock:
            try:
                return await self._forget_session()
            finally:
                self.models._clear()

    async def conversation_attachment_count(self, conversation_id: str) -> int | None:
        async with self._activity():
            return await self._conversation_attachment_count(conversation_id)

    async def upload_file(self, path: Any) -> dict[str, Any]:
        async with self._activity():
            return await self._upload_file(path)

    async def project_options(self, project_id: str | None = None) -> ProjectOptions:
        async with self._activity():
            return await self._project_options(project_id)

    async def update_project(self, project: ProjectData) -> ProjectData:
        async with self._activity():
            updated = await self._patch_project_data(
                project.id,
                name=project.name,
                instructions=project.instructions or "",
            )
        await _publish_collection_change(self, "projects", "update", updated)
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

    async def _move_chat_data(
        self, conversation_id: str, project_id: str | None, *, source_project_id: str | None
    ) -> None:
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
        async with self._project_lock(project_id):
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
        async with self._project_lock(project_id):
            staged = current() if callable(current) else current
            return await self._add_project_resource(project_id, kind, current=staged, **values)

    async def _remove_project_resource_data(
        self,
        project_id: str,
        resource_id: str,
        *,
        current: ProjectData | Callable[[], ProjectData | None] | None = None,
    ) -> ProjectData:
        async with self._project_lock(project_id):
            staged = current() if callable(current) else current
            return await self._remove_project_resource(project_id, resource_id, current=staged)

    async def _resolve_download_data(
        self,
        url: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
    ) -> Any | None:
        async with self._activity():
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
            async with self._activity():
                opened_here = await self._open_session(reuse_open=True)
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
