from __future__ import annotations

import contextlib
import contextvars
import logging
import threading
from collections.abc import Callable, Generator, Iterator
from typing import TYPE_CHECKING, Any, TypeVar, cast
from weakref import WeakValueDictionary

from eventflow import ErrorPolicy, EventBus, EventEnvelope, StateMachine

from .accounts.status import SessionStatus
from .base import (
    BaseProvider,
    Capability,
    ProviderType,
    _construct_provider,
    _StreamEventTranslator,
)
from .collections import _publish_collection_change_sync
from .core.hooks import OperationHooks
from .core.state import _LIFECYCLE_TERMINAL_STATES, _LIFECYCLE_TRANSITIONS, LifecycleState
from .errors import FeatureUnavailable, ProviderError
from .resources.chat.chat import Chat
from .resources.chat.collections import ChatsCollection
from .resources.chat.types import Conversation
from .resources.message.attachment import Attachment
from .resources.message.message import Message
from .resources.message.protocol import ConversationBound
from .resources.message.request import build_message_request_pipeline_sync, run_pipeline_sync
from .resources.model.collections import ModelsCollection
from .resources.model.model import EffortRef, ModelRef, ModelSelection
from .resources.model.types import Models, _model_refs_from_catalog
from .resources.project.collections import ProjectsCollection
from .resources.project.project import Project
from .resources.project.types import ProjectData, ProjectOptions
from .resources.response.artifact import Artifact
from .resources.response.operation import StreamEvent, _close_sync_iterator
from .resources.response.response import Response
from .resources.response.source import Source
from .transport import StreamResponse, Transport, TransportConfig, TransportResponse

if TYPE_CHECKING:
    from typing_extensions import TypeVar as DefaultTypeVar

    from .accounts.account import Account
    from .resources.message.protocol import ProviderRequest, ProviderStreamEvent
    from .resources.model.model import ModelCatalog
    from .resources.response.operation import MessageOperation


if TYPE_CHECKING:
    _ChatsT = DefaultTypeVar(
        "_ChatsT", bound=ChatsCollection[Any], covariant=True, default=ChatsCollection[Chat]
    )
    _ProjectsT = DefaultTypeVar(
        "_ProjectsT",
        bound=ProjectsCollection[Any],
        covariant=True,
        default=ProjectsCollection[Project],
    )
else:
    _ChatsT = TypeVar("_ChatsT", covariant=True)
    _ProjectsT = TypeVar("_ProjectsT", covariant=True)


_ProviderT = TypeVar("_ProviderT", bound="Provider[Any, Any]")


_CLOSING_PROVIDER: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "webai_sync_closing_provider", default=None
)
_log = logging.getLogger(__name__)


class _TrackedTransport(Transport):
    """Lifecycle-tracked view of a sync Provider's active transport."""

    def __init__(self, provider: Provider[Any, Any], transport: Transport) -> None:
        self._provider = provider
        self._transport = transport

    @property
    def origin(self) -> str:
        return self._transport.origin

    @property
    def config(self) -> TransportConfig:
        return self._transport.config

    def resolve(self, path: str) -> str:
        return self._transport.resolve(path)

    def send(self, method: str, path: str, **options: Any) -> TransportResponse:
        with self._provider._activity():
            return self._transport.send(method, path, **options)

    def request(self, method: str, path: str, **options: Any) -> Any:
        with self._provider._activity():
            return self._transport.request(method, path, **options)

    @contextlib.contextmanager
    def stream(self, method: str, path: str, **options: Any) -> Iterator[StreamResponse]:
        with self._provider._activity(), self._transport.stream(method, path, **options) as resp:
            yield resp

    def record_rate_limit(self, delay: float) -> None:
        self._transport.record_rate_limit(delay)

    def close(self) -> None:
        self._provider.close()


class Provider(BaseProvider[_ChatsT, _ProjectsT, ModelsCollection]):
    """Native blocking provider session running all operations on the caller thread."""

    def __new__(cls: type[_ProviderT], *args: Any, **options: Any) -> _ProviderT:
        return _construct_provider(cls, Provider, args, options, asynchronous=False)

    _chat_page_size = 50
    _chat_initial_page_floor = 1
    _chat_followup_page_size: int | None = None
    _message_type = Message
    _response_type = Response
    _source_type = Source
    _artifact_type = Artifact
    _attachment_type = Attachment

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        transport: Transport | None = None,
    ) -> None:
        super().__init__(account, type=type)
        self._transport = transport
        self.events: EventBus[EventEnvelope[str]] = EventBus(error_policy=ErrorPolicy.ISOLATE)
        self._event_sequence = 0
        self._event_lock = threading.Lock()
        self.hooks: OperationHooks = OperationHooks()
        self.lifecycle = StateMachine(
            initial=LifecycleState.CREATED,
            transitions=_LIFECYCLE_TRANSITIONS,
            terminal=_LIFECYCLE_TERMINAL_STATES,
        )
        self._opened = False
        self._lifecycle_lock = threading.RLock()
        self._lifecycle_condition = threading.Condition(self._lifecycle_lock)
        self._closing_in_progress = False
        self._close_error: BaseException | None = None
        self._active_operations = 0
        self._parked_sends: dict[MessageOperation, int] = {}
        self._locks_guard = threading.Lock()
        self._locks: WeakValueDictionary[tuple[str, str], threading.Lock] = WeakValueDictionary()
        self._chats = self._make_chats()
        self._projects = self._make_projects()
        self.models = ModelsCollection(self._load_models, provider=self)
        self._send_pipeline = build_message_request_pipeline_sync(
            provider_id=self.id,
            account_id=self.account_id,
            model_resolver=self.models.resolve,
        )

    def _make_chats(self) -> _ChatsT:
        return cast("_ChatsT", ChatsCollection(self, Chat))

    def _make_projects(self) -> _ProjectsT:
        return cast("_ProjectsT", ProjectsCollection(self, Project))

    @property
    def transport(self) -> Transport:
        self._require_provider()
        active = self._active_transport()
        if active is None:
            active = self._transport
        if active is None:
            raise ProviderError("This provider has no configured transport")
        return _TrackedTransport(self, active)

    def _lock(self, kind: str, remote_id: str) -> threading.Lock:
        key = (kind, remote_id)
        with self._locks_guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

    def _send_lock(self, conversation_id: str) -> threading.Lock:
        return self._lock("chat", conversation_id)

    def _bind_send_lock(self, conversation_id: str, lock: threading.Lock) -> None:
        with self._locks_guard:
            self._locks["chat", conversation_id] = lock

    def _project_lock(self, project_id: str) -> threading.Lock:
        return self._lock("project", project_id)

    @contextlib.contextmanager
    def _activity(self, *, send_lock_id: int | None = None):
        if send_lock_id is not None and self._has_active_operation(send_lock_id=send_lock_id):
            raise ProviderError("A send hook cannot start another send in the same conversation")
        nested = self._has_active_operation()
        with self._operation_marker(send_lock_id=send_lock_id):
            self._begin_activity(nested=nested)
            try:
                yield
            finally:
                self._end_activity()

    def _set_send_parked(self, operation: MessageOperation, parked: bool) -> bool:
        with self._lifecycle_condition:
            if not parked:
                self._parked_sends.pop(operation, None)
            elif self.closing or self.closed:
                return False
            else:
                self._parked_sends[operation] = threading.get_ident()
            return True

    def _begin_activity(self, *, nested: bool = False) -> None:
        with self._lifecycle_condition:
            if self.closed or (self.closing and not nested):
                raise ProviderError("Provider is closing or is closed")
            self._active_operations += 1

    def _end_activity(self) -> None:
        with self._lifecycle_condition:
            self._active_operations -= 1
            if self._active_operations == 0:
                self._lifecycle_condition.notify_all()

    def _emit(
        self,
        name: str,
        *,
        operation_id: str | None = None,
        resource_id: str | None = None,
        payload: object | None = None,
    ) -> None:
        with self._event_lock:
            self._event_sequence += 1
            seq = self._event_sequence
        if not self.events.subscriber_count:
            return
        event = EventEnvelope(
            event=name,
            correlation_id=operation_id,
            source=self.id,
            metadata={
                "account_id": self.account_id,
                "resource_id": resource_id,
                "sequence": seq,
                "payload": payload,
            },
        )
        self.events.publish(event)

    def _emit_safe(
        self,
        name: str,
        *,
        operation_id: str | None = None,
        resource_id: str | None = None,
        payload: object | None = None,
    ) -> None:
        try:
            self._emit(name, operation_id=operation_id, resource_id=resource_id, payload=payload)
        except Exception as error:
            _log.warning("Event dispatch failed: event=%s error=%s", name, type(error).__name__)

    def _load_models(self, refresh: bool = False) -> list[ModelRef]:
        with self._activity():
            catalog = self._load_catalog(force=refresh)
            return _model_refs_from_catalog(self.type.value, catalog)

    def _open_session(self, *, reuse_open: bool = False) -> bool:
        pending: list[tuple[str, object | None]] = []
        opened_here = False
        try:
            with self._lifecycle_lock:
                if reuse_open and self._opened:
                    return False
                opened_here = self._open_unlocked(pending)
        finally:
            for name, payload in pending:
                self._emit_safe(name, payload=payload)
        return opened_here

    def _open_unlocked(self, pending: list[tuple[str, object | None]]) -> bool:
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
            self._prepare_session()
        except BaseException as error:
            if self.lifecycle.can_transition(LifecycleState.OPEN_FAILED):
                self.lifecycle.transition(LifecycleState.OPEN_FAILED)
            pending.append(("provider.open_failed", {"error_type": type(error).__name__}))
            with contextlib.suppress(Exception):
                self._close_session_resources()
            raise
        self._opened = True
        if self.lifecycle.can_transition(LifecycleState.OPEN):
            self.lifecycle.transition(LifecycleState.OPEN)
        pending.append(("provider.opened", None))
        return True

    def open(self: _ProviderT) -> _ProviderT:
        self._open_session()
        return self

    def _close_session_resources(self) -> None:
        self.models._clear()
        try:
            self._close_session()
        finally:
            if type(self)._close_session is Provider._close_session and self._transport is not None:
                self._transport.close()

    def close(self) -> None:
        if _CLOSING_PROVIDER.get() == id(self):
            raise ProviderError("close() cannot be called from a provider closing event callback")
        if self._has_active_operation():
            raise ProviderError("Cannot close a provider from one of its active operations")
        with self._lifecycle_condition:
            if self.closed:
                return
            if self._closing_in_progress:
                self._lifecycle_condition.wait_for(lambda: not self._closing_in_progress)
                if self._close_error is not None:
                    raise self._close_error
                return
            if self.lifecycle.can_transition(LifecycleState.CLOSING):
                self.lifecycle.transition(LifecycleState.CLOSING)
            self._closing_in_progress = True
            self._close_error = None
            parked = tuple(self._parked_sends)

        token = _CLOSING_PROVIDER.set(id(self))
        try:
            self._emit_safe("provider.closing")
            for operation in parked:
                operation.cancel()
            with self._lifecycle_condition:
                self._lifecycle_condition.wait_for(lambda: self._active_operations == 0)
            self._opened = False
            try:
                self._close_session_resources()
            except BaseException as error:
                if self.lifecycle.can_transition(LifecycleState.CLOSE_FAILED):
                    self.lifecycle.transition(LifecycleState.CLOSE_FAILED)
                self._close_error = error
                self._emit_safe(
                    "provider.close_failed", payload={"error_type": type(error).__name__}
                )
                raise
            if self.lifecycle.can_transition(LifecycleState.CLOSED):
                self.lifecycle.transition(LifecycleState.CLOSED)
            self._emit_safe("provider.closed")
            self.events.close()
        finally:
            _CLOSING_PROVIDER.reset(token)
            with self._lifecycle_condition:
                self._closing_in_progress = False
                self._lifecycle_condition.notify_all()

    def _execute_stream(self, operation: Any) -> Generator[StreamEvent, None, None]:
        chat = operation.chat
        conversation_id = chat.remote_id or chat.id
        caller = threading.get_ident()
        with self._lifecycle_condition:
            if any(
                owner == caller and (pending.chat.remote_id or pending.chat.id) == conversation_id
                for pending, owner in self._parked_sends.items()
            ):
                raise ProviderError(
                    "Resume or cancel the paused send before sending in the same conversation"
                )
        lock = self._send_lock(conversation_id)
        lock_id = id(lock)
        events = self._execute_stream_events(operation, lock)
        with self._activity(send_lock_id=lock_id):
            try:
                with lock:
                    yield from events
            finally:
                _close_sync_iterator(events)

    def _execute_stream_events(
        self, operation: Any, send_lock: threading.Lock
    ) -> Generator[StreamEvent, None, None]:
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
        prepared = run_pipeline_sync(self._send_pipeline, prep_context)
        translator = _StreamEventTranslator(self, operation)
        stream = self._stream_provider_events(prepared.request)
        try:
            for event in stream:
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
                        _publish_collection_change_sync(
                            self,
                            "chats",
                            "create",
                            operation.chat,
                            operation_id=getattr(operation, "operation_id", None),
                        )
                    if not had_remote_id and event.conversation_id:
                        self._emit_safe(
                            "chat.created",
                            operation_id=getattr(operation, "operation_id", None),
                            resource_id=event.conversation_id,
                            payload={"title": operation.chat.title, "local_id": operation.chat.id},
                        )
                else:
                    yield from translator.feed(event)
            yield from translator.finish()
        except (ProviderError, ValueError):
            raise
        except Exception as exc:
            raise ProviderError("Provider request failed") from exc
        finally:
            _close_sync_iterator(stream)

    def _prepare_session(self) -> None:
        return None

    def _close_session(self) -> None:
        return None

    def _authenticate(self) -> SessionStatus:
        raise FeatureUnavailable(Capability.AUTH, "not_implemented")

    def _forget_session(self) -> SessionStatus:
        raise FeatureUnavailable(Capability.AUTH, "forget_session_not_implemented")

    def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
        raise FeatureUnavailable(Capability.MODELS, "not_implemented")

    def _stream_provider_events(self, request: ProviderRequest) -> Iterator[ProviderStreamEvent]:
        raise NotImplementedError

    def _conversation_attachment_count(self, conversation_id: str) -> int | None:
        raise FeatureUnavailable(Capability.ATTACHMENTS, "not_implemented")

    def _upload_file(self, path: Any) -> dict[str, Any]:
        raise FeatureUnavailable(Capability.ATTACHMENTS, "not_implemented")

    def _resolve_download(
        self,
        url: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
    ) -> Any | None:
        return None

    def _project_options(self, project_id: str | None = None) -> ProjectOptions:
        return ProjectOptions()

    def _patch_project(
        self,
        project_id: str,
        *,
        name: str | None = None,
        instructions: str | None = None,
        current: ProjectData | None = None,
    ) -> ProjectData:
        raise FeatureUnavailable(Capability.PROJECT_UPDATE, "not_implemented")

    def _add_project_resource(
        self,
        project_id: str,
        kind: str,
        *,
        current: ProjectData | None = None,
        **values: Any,
    ) -> ProjectData:
        raise FeatureUnavailable(Capability.RESOURCES, "not_implemented")

    def _remove_project_resource(
        self,
        project_id: str,
        resource_id: str,
        *,
        current: ProjectData | None = None,
    ) -> ProjectData:
        raise FeatureUnavailable(Capability.RESOURCES, "not_implemented")

    def authenticate(self) -> SessionStatus:
        return self._authenticate_session()

    def _authenticate_session(self, **options: Any) -> SessionStatus:
        with self._activity(), self.models._lock:
            try:
                return self._authenticate(**options)
            finally:
                self.models._clear()

    def session_status(self) -> SessionStatus:
        with self._activity():
            return self._session_status()

    def forget_session(self) -> SessionStatus:
        with self._activity(), self.models._lock:
            try:
                return self._forget_session()
            finally:
                self.models._clear()

    def conversation_attachment_count(self, conversation_id: str) -> int | None:
        with self._activity():
            return self._conversation_attachment_count(conversation_id)

    def upload_file(self, path: Any) -> dict[str, Any]:
        with self._activity():
            return self._upload_file(path)

    def project_options(self, project_id: str | None = None) -> ProjectOptions:
        with self._activity():
            return self._project_options(project_id)

    def update_project(self, project: ProjectData) -> ProjectData:
        with self._activity():
            updated = self._patch_project_data(
                project.id,
                name=project.name,
                instructions=project.instructions or "",
            )
        _publish_collection_change_sync(self, "projects", "update", updated)
        return updated

    def _list_chats_data(self, *, cursor: str | None = None, limit: int = 50) -> Any:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    def _get_chat_data(self, conversation_id: str) -> Conversation:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    def _get_chat_metadata(self, conversation_id: str) -> Conversation:
        return self._get_chat_data(conversation_id)

    def _rename_chat_data(self, conversation_id: str, title: str) -> None:
        raise FeatureUnavailable(Capability.RENAME, "not_implemented")

    def _delete_chat_data(self, conversation_id: str) -> None:
        raise FeatureUnavailable(Capability.DELETE, "not_implemented")

    def _branch_chat_data(self, conversation_id: str, message_id: str) -> Conversation:
        raise FeatureUnavailable(Capability.CHAT_BRANCH, "not_implemented")

    def _move_chat_data(
        self, conversation_id: str, project_id: str | None, *, source_project_id: str | None
    ) -> None:
        raise FeatureUnavailable(Capability.PROJECT_MEMBERSHIP, "not_implemented")

    def _list_projects_data(self, *, cursor: str | None = None) -> Any:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    def _get_project_data(self, project_id: str) -> ProjectData:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    def _list_project_chats_data(
        self, project_id: str, *, cursor: str | None = None, limit: int | None = None
    ) -> Any:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    def _create_project_data(
        self, name: str, instructions: str = "", **options: Any
    ) -> ProjectData:
        raise FeatureUnavailable(Capability.PROJECT_CREATE, "not_implemented")

    def _patch_project_data(
        self,
        project_id: str,
        *,
        name: str | None = None,
        instructions: str | None = None,
        current: ProjectData | Callable[[], ProjectData | None] | None = None,
    ) -> ProjectData:
        with self._project_lock(project_id):
            staged = current() if callable(current) else current
            return self._patch_project(
                project_id, name=name, instructions=instructions, current=staged
            )

    def _delete_project_data(self, project_id: str) -> None:
        raise FeatureUnavailable(Capability.PROJECT_DELETE, "not_implemented")

    def _add_project_resource_data(
        self,
        project_id: str,
        kind: str,
        *,
        current: ProjectData | Callable[[], ProjectData | None] | None = None,
        **values: Any,
    ) -> ProjectData:
        with self._project_lock(project_id):
            staged = current() if callable(current) else current
            return self._add_project_resource(project_id, kind, current=staged, **values)

    def _remove_project_resource_data(
        self,
        project_id: str,
        resource_id: str,
        *,
        current: ProjectData | Callable[[], ProjectData | None] | None = None,
    ) -> ProjectData:
        with self._project_lock(project_id):
            staged = current() if callable(current) else current
            return self._remove_project_resource(project_id, resource_id, current=staged)

    def _resolve_download_data(
        self,
        url: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
    ) -> Any | None:
        with self._activity():
            return self._resolve_download(
                url, conversation_id=conversation_id, message_id=message_id
            )

    def ask(
        self,
        prompt: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Any = None,
    ) -> Response:
        opened_here = False
        try:
            with self._activity():
                opened_here = self._open_session(reuse_open=True)
                chat = self.chats.create()
                return chat.responses.create(
                    prompt,
                    model=model,
                    attachments=attachments,
                ).start()
        finally:
            if opened_here:
                self.close()

    def __enter__(self: _ProviderT) -> _ProviderT:
        return self.open()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
