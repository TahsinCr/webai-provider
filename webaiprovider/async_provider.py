from __future__ import annotations

import asyncio
import contextlib
import contextvars
from collections.abc import AsyncGenerator
from dataclasses import dataclass, replace
from functools import wraps
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Concatenate, ParamSpec, TypeVar
from weakref import WeakValueDictionary

from .accounts.status import SessionState, SessionStatus
from .base import BaseProvider, Capability
from .collections import Page
from .core.scope import ProviderResourceScope
from .core.telemetry import ProviderTelemetry
from .errors import FeatureUnavailable, ProviderError, RemoteNotFound, UnsupportedOperationError
from .provider import ProviderType, _construct_provider
from .resources.chat.chat import AsyncChat
from .resources.chat.collections import AsyncChatsCollection
from .resources.chat.types import Conversation
from .resources.common import Origin, ResourceData
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
    ProviderRequest,
    TextDelta,
    TextReplacement,
    ThoughtDelta,
)
from .resources.message.response import (
    Artifact,
    Response,
    Source,
)
from .resources.message.types import MessageAction
from .resources.model.collections import (
    AsyncModelsCollection,
    Models,
    _model_refs_from_catalog,
)
from .resources.model.model import (
    EffortRef,
    ModelCatalog,
    ModelRef,
    ModelSelection,
)
from .resources.project.collections import AsyncProjectsCollection
from .resources.project.project import AsyncProject
from .resources.project.types import (
    ProjectData,
    ProjectOptions,
)
from .transport import AsyncTransport

if TYPE_CHECKING:
    from .accounts.account import Account

_P = ParamSpec("_P")
_R = TypeVar("_R")


async def _prepare_operation(operation: Any, provider: Any) -> ProviderRequest:
    message = operation.message
    chat = operation.chat
    raw_action = message.metadata.get("action")
    try:
        action = MessageAction(raw_action) if raw_action is not None else None
    except ValueError:
        raise UnsupportedOperationError("Unsupported message action") from None

    message_id = message.metadata.get("parent_message_id")
    if action is not None:
        if action not in (MessageAction.EDIT, MessageAction.RETRY):
            raise UnsupportedOperationError("Unsupported message action")
        if not chat.remote_id or not message_id:
            raise UnsupportedOperationError("Message actions require remote message identities")

    session_account = getattr(getattr(chat, "session", None), "account", None)
    if session_account is not None and (
        session_account.provider != provider.id or session_account.id != provider.account_id
    ):
        raise ValueError("message operation belongs to a different provider account")

    model: str | None = None
    effort: str | None = None
    if message.model is not None:
        if isinstance(message.model, ModelSelection):
            target_provider = message.model.model.provider
            model = message.model.model.id
            effort = message.model.effort.id if message.model.effort is not None else None
        elif isinstance(message.model, ModelRef):
            target_provider = message.model.provider
            model = message.model.id
        elif isinstance(message.model, EffortRef):
            target_provider = message.model.provider
            model = message.model.model_id
            effort = message.model.id
        else:
            target_provider = None
        if target_provider is not None:
            if target_provider != provider.id:
                raise RemoteNotFound(f"{target_provider} model is not available in this catalog")
        else:
            if chat.session is None:
                raise UnsupportedOperationError(
                    "Model selection requires an active provider session"
                )
            selection = await chat.session.models.resolve(message.model)
            model = selection.model.id
            effort = selection.effort.id if selection.effort is not None else None

    attachments = tuple(
        {
            key: value
            for key, value in {
                "id": item.id,
                "path": str(item.path) if item.path is not None else None,
                "bytes": item._content,
                "name": item.name,
                "mime_type": item.mime_type,
                "size_bytes": getattr(item, "size", None),
                "grok_file": getattr(item, "grok_file", None),
            }.items()
            if value is not None
        }
        for item in message.attachments
    )
    return ProviderRequest(
        prompt=message.content,
        model=model,
        effort=effort,
        attachments=attachments,
        action=action,
        message_id=message_id,
        temporary=bool(chat.temporary or message.metadata.get("temporary", False)),
        conversation_id=chat.remote_id,
        project_id=(chat.project.remote_id or chat.project.id)
        if chat.project is not None
        else None,
    )


@dataclass(slots=True, eq=False)
class _ActiveOperation:
    provider_id: int
    send_lock_id: int | None = None
    active: bool = True


_ACTIVE_OPERATIONS: contextvars.ContextVar[tuple[_ActiveOperation, ...]] = contextvars.ContextVar(
    "webai_active_operations", default=()
)


def _has_active_operation(provider_id: int, *, send_lock_id: int | None = None) -> bool:
    return any(
        operation.active
        and operation.provider_id == provider_id
        and (send_lock_id is None or operation.send_lock_id == send_lock_id)
        for operation in _ACTIVE_OPERATIONS.get()
    )


def _provider_activity(
    method: Callable[Concatenate[AsyncProvider, _P], Awaitable[_R]],
) -> Callable[Concatenate[AsyncProvider, _P], Awaitable[_R]]:
    @wraps(method)
    async def wrapped(self: AsyncProvider, *args: _P.args, **kwargs: _P.kwargs) -> _R:
        async with self._activity():
            return await method(self, *args, **kwargs)

    return wrapped


class AsyncProvider(BaseProvider):
    def __new__(cls, *args, **options):
        return _construct_provider(cls, AsyncProvider, args, options, asynchronous=True)

    _chat_type = AsyncChat
    _project_type = AsyncProject
    _projects_collection_type = AsyncProjectsCollection
    capabilities: frozenset[Capability] = frozenset()
    _gate: asyncio.Lock
    editor: Any

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        transport: AsyncTransport | None = None,
    ) -> None:
        super().__init__(account, type=type)
        self._transport = transport
        self._loop = None
        self.telemetry = ProviderTelemetry()
        self._scope: ProviderResourceScope | None = None
        self._opened = False
        self._closed = False
        self._closing = False
        self._lifecycle_lock = asyncio.Lock()
        self._lifecycle_condition = asyncio.Condition()
        self._close_task: asyncio.Task[None] | None = None
        self._active_operations = 0
        self._send_locks: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()
        self._project_locks: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()
        self.chats = AsyncChatsCollection(self)
        self.projects = self._projects_collection_type(self)
        self.models = AsyncModelsCollection(self._load_models)

    @property
    def transport(self) -> AsyncTransport:
        self._require_provider()
        if self._transport is None:
            raise ProviderError("This provider has no configured transport")
        return self._transport

    @property
    def id(self) -> str:
        return self.type.value

    @property
    def account_id(self) -> str:
        return self.account.id

    async def _load_models(self, refresh: bool = False) -> list[ModelRef]:
        async with self._activity():
            catalog = await self._load_model_catalog(force=refresh)
            return _model_refs_from_catalog(self.type.value, catalog)

    async def prepare(self) -> None:
        return None

    async def _close_provider(self) -> None:
        if self._transport is not None:
            await self._transport.close()

    async def _open_locked(self) -> AsyncProvider:
        if self._closed or (self._closing and not _has_active_operation(id(self))):
            raise ProviderError("A closed provider cannot be reopened")
        if self._opened:
            return self
        self._loop = asyncio.get_running_loop()
        scope = ProviderResourceScope()
        try:
            await self.prepare()
        except BaseException:
            with contextlib.suppress(Exception):
                await self._close_provider()
            await scope.aclose()
            raise
        scope.defer(self._close_provider)
        self._scope = scope
        self._opened = True
        self._closed = False
        return self

    async def open(self) -> AsyncProvider:
        async with self._lifecycle_lock:
            return await self._open_locked()

    def _require_provider(self) -> AsyncProvider:
        if not self._opened:
            raise ProviderError("Open the provider before using its resources")
        return self

    async def close(self) -> None:
        if _has_active_operation(id(self)):
            raise ProviderError("Cannot close a provider from one of its active operations")
        async with self._lifecycle_lock:
            if self._closed:
                return
            if self._close_task is None:
                async with self._lifecycle_condition:
                    self._closing = True
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
        async with self._lifecycle_condition:
            await self._lifecycle_condition.wait_for(lambda: self._active_operations == 0)
        scope, self._scope = self._scope, None
        self._opened = False
        try:
            if scope is not None:
                await scope.aclose()
            else:
                await self._close_provider()
        finally:
            self._closed = True
            async with self._lifecycle_condition:
                self._closing = False
                self._lifecycle_condition.notify_all()

    def _send_lock(self, conversation_id: str) -> asyncio.Lock:
        lock = self._send_locks.get(conversation_id)
        if lock is None:
            lock = asyncio.Lock()
            self._send_locks[conversation_id] = lock
        return lock

    def _project_lock(self, project_id: str) -> asyncio.Lock:
        lock = self._project_locks.get(project_id)
        if lock is None:
            lock = asyncio.Lock()
            self._project_locks[project_id] = lock
        return lock

    @contextlib.contextmanager
    def _operation_marker(self, *, send_lock_id: int | None = None):
        operation = _ActiveOperation(id(self), send_lock_id)
        token = _ACTIVE_OPERATIONS.set((*_ACTIVE_OPERATIONS.get(), operation))
        try:
            yield
        finally:
            operation.active = False
            _ACTIVE_OPERATIONS.reset(token)

    @contextlib.asynccontextmanager
    async def _activity(self, *, send_lock_id: int | None = None):
        if send_lock_id is not None and _has_active_operation(id(self), send_lock_id=send_lock_id):
            raise ProviderError("A send hook cannot await another send in the same conversation")
        nested = _has_active_operation(id(self))
        with self._operation_marker(send_lock_id=send_lock_id):
            async with self._lifecycle_lock:
                if self._closed or (self._closing and not nested):
                    raise ProviderError("Provider is closing or is closed")
                async with self._lifecycle_condition:
                    self._active_operations += 1
            try:
                yield
            finally:
                async with self._lifecycle_condition:
                    self._active_operations -= 1
                    self._lifecycle_condition.notify_all()

    async def execute_stream(self, operation: Any) -> AsyncGenerator[StreamEvent, None]:
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
        request = await _prepare_operation(operation, self)
        output_started = thinking_started = False
        thinking_text = ""
        stream = self._stream_request(request)
        try:
            async for event in stream:
                if isinstance(event, ConversationBound):
                    operation.chat.remote_id = event.conversation_id
                    self._send_locks[event.conversation_id] = send_lock
                    chat_metadata = getattr(operation.chat, "metadata", None)
                    if isinstance(chat_metadata, dict):
                        chat_metadata.pop("pending_branch", None)
                elif isinstance(event, ThoughtDelta) or (
                    isinstance(event, (TextDelta, TextReplacement)) and event.channel == "analysis"
                ):
                    delta = event.text
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

    async def _load_model_catalog(self, *, force: bool = False) -> ModelCatalog:
        raise FeatureUnavailable(Capability.MODELS, "not_implemented")

    def attachment_policy(self) -> dict | None:
        from .utils.attachments import policy_for

        policy = policy_for(getattr(self, "id", ""))
        return policy.as_dict() if policy else None

    def _origin(self, remote: str) -> Origin:
        return Origin(getattr(self, "id", ""), getattr(self, "account_id", "default"), remote)

    @_provider_activity
    async def authenticate(self) -> SessionStatus:
        raise FeatureUnavailable(Capability.AUTH, "not_implemented")

    @_provider_activity
    async def session_status(self) -> SessionStatus:
        return SessionStatus(SessionState.UNKNOWN, reason="not_implemented")

    @_provider_activity
    async def forget_session(self) -> SessionStatus:
        raise FeatureUnavailable(Capability.AUTH, "forget_session_not_implemented")

    @_provider_activity
    async def list_chats(self, cursor: str | None = None, limit: int = 28) -> Page[Conversation]:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    @_provider_activity
    async def get_chat(self, conversation_id: str) -> Conversation:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    @_provider_activity
    async def branch_chat(self, conversation_id: str, message_id: str) -> Conversation:
        raise FeatureUnavailable(Capability.CHAT_BRANCH, "not_implemented")

    async def conversation_attachment_count(self, conversation_id: str) -> int | None:
        raise FeatureUnavailable(Capability.ATTACHMENTS, "not_implemented")

    @_provider_activity
    async def rename_chat(self, conversation_id: str, title: str) -> None:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    @_provider_activity
    async def delete_chat(self, conversation_id: str) -> None:
        raise FeatureUnavailable(Capability.DELETE, "not_implemented")

    @_provider_activity
    async def upload_file(self, path: Any) -> dict:
        raise FeatureUnavailable(Capability.ATTACHMENTS, "not_implemented")

    @_provider_activity
    async def list_projects(self, cursor: str | None = None) -> Page[ProjectData]:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    @_provider_activity
    async def get_project(self, project_id: str) -> ProjectData:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    @_provider_activity
    async def list_project_chats(
        self, project_id: str, cursor: str | None = None
    ) -> Page[Conversation]:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    @_provider_activity
    async def create_project(
        self,
        name: str,
        instructions: str = "",
    ) -> ProjectData:
        raise FeatureUnavailable(Capability.PROJECT_CREATE, "not_implemented")

    @_provider_activity
    async def delete_project(self, project_id: str) -> None:
        raise FeatureUnavailable(Capability.PROJECT_DELETE, "not_implemented")

    @_provider_activity
    async def move_chat(self, conversation_id: str, project_id: str | None) -> None:
        raise FeatureUnavailable(Capability.PROJECT_MEMBERSHIP, "not_implemented")

    @_provider_activity
    async def resolve_download(
        self, url: str, conversation_id: str | None = None, message_id: str | None = None
    ) -> Any | None:
        return None

    def _invalidate_project_cache(self, project_id: str) -> None:
        del project_id

    async def _project_after_write(self, project_id: str, confirmed: Any) -> ProjectData:
        host = self
        host._invalidate_project_cache(project_id)
        for _ in range(12):
            project = await host.get_project(project_id)
            if confirmed(project):
                return project
            await asyncio.sleep(0.4)
        raise ProviderError(
            "Project change could not be confirmed by the server; sync before retrying"
        )

    @_provider_activity
    async def project_options(self, project_id: str | None = None) -> ProjectOptions:
        host = self
        async with host._gate:
            return await host.editor.options(project_id)

    @_provider_activity
    async def update_project(self, project: ProjectData) -> ProjectData:
        if project.origin.provider_id != self.id or project.origin.account_id != self.account_id:
            raise ValueError("Project belongs to a different provider account")
        return await self.patch_project(
            project.origin.remote_id, name=project.name, instructions=project.instructions or ""
        )

    @_provider_activity
    async def patch_project(
        self, project_id: str, *, name: str | None = None, instructions: str | None = None
    ) -> ProjectData:
        if name is not None and not name.strip():
            raise ValueError("Project name is required")
        async with self._project_lock(project_id):
            current = await self.get_project(project_id)
            changes = {}
            if name is not None and current.name != name:
                changes["name"] = name
            if instructions is not None and (current.instructions or "") != instructions:
                changes["instructions"] = instructions
            if not changes:
                return current
            async with self._gate:
                result = await self.editor.write("update", project_id, current=current, **changes)
            if isinstance(result, ProjectData):
                self._invalidate_project_cache(project_id)
                return result
            return await self._project_after_write(
                project_id,
                lambda saved: all(
                    (getattr(saved, field) or "") == value for field, value in changes.items()
                ),
            )

    @_provider_activity
    async def add_project_resource(self, project_id: str, kind: str, **values: Any) -> ProjectData:
        async with self._project_lock(project_id):
            host = self
            before = await host.get_project(project_id)
            ids = {resource.id for resource in before.resources}
            async with host._gate:
                result = await host.editor.write(
                    "add_source",
                    project_id,
                    kind=kind,
                    existing_resources=before.resources,
                    **values,
                )
            host._invalidate_project_cache(project_id)
            if isinstance(result, ResourceData):
                return replace(before, resources=(*before.resources, result))
            if isinstance(result, ProjectData):
                return result
            return await self._project_after_write(
                project_id,
                lambda project: any(resource.id not in ids for resource in project.resources),
            )

    @_provider_activity
    async def remove_project_resource(self, project_id: str, resource_id: str) -> ProjectData:
        async with self._project_lock(project_id):
            host = self
            before = await host.get_project(project_id)
            source = next(
                (resource for resource in before.resources if resource.id == resource_id), None
            )
            if source is None:
                raise ValueError("Project resource was not found")
            async with host._gate:
                result = await host.editor.write(
                    "remove_source",
                    project_id,
                    resource_id=resource_id,
                    resource_name=source.name,
                    resource=source,
                )
            if isinstance(result, ProjectData):
                host._invalidate_project_cache(project_id)
                return result
            return await host._project_after_write(
                project_id,
                lambda project: all(resource.id != resource_id for resource in project.resources),
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
                async with self._lifecycle_lock:
                    if not self._opened:
                        await self._open_locked()
                        opened_here = True
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

    def __repr__(self) -> str:
        return f"<AsyncProvider type={self.type.value!r} account={self.account.id!r}>"


async def confirm_deleted(read: Any, missing: Any = lambda value: False) -> None:
    for delay in (0, 0.2, 0.5, 1):
        if delay:
            await asyncio.sleep(delay)
        try:
            value = await read()
        except RemoteNotFound:
            return
        if missing(value):
            return
    raise ProviderError(
        "Deletion could not be confirmed by the server; the local record was preserved. Sync before retrying."
    )
