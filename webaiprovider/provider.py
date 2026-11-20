from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable
from contextlib import contextmanager
from threading import Lock, RLock
from typing import TYPE_CHECKING, Any, TypeVar, cast

from eventflow import ErrorPolicy, EventBus, EventEnvelope

from .accounts.status import SessionStatus
from .base import BaseProvider, Capability, ProviderType, _construct_provider
from .core.hooks import OperationHooks
from .core.state import LifecycleState
from .errors import ProviderError
from .resources.chat.chat import AsyncChat, Chat
from .resources.chat.collections import ChatsCollection
from .resources.message.message import AsyncMessage, Message
from .resources.model.collections import ModelsCollection
from .resources.model.model import (
    EffortRef,
    ModelRef,
    ModelSelection,
)
from .resources.model.types import Models
from .resources.project.collections import ProjectsCollection
from .resources.project.project import Project
from .resources.response.response import Response
from .transport import AsyncTransport, Transport

if TYPE_CHECKING:
    from typing_extensions import TypeVar as DefaultTypeVar

    from .accounts.account import Account
    from .async_provider import AsyncProvider
    from .resources.project.project import AsyncProject


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

_ResultT = TypeVar("_ResultT")


_ProviderT = TypeVar("_ProviderT", bound="Provider[Any, Any]")


class Provider(BaseProvider[_ChatsT, _ProjectsT, ModelsCollection]):
    """Run provider operations on the caller using a session-scoped event loop."""

    def __new__(cls: type[_ProviderT], *args: Any, **options: Any) -> _ProviderT:
        return _construct_provider(cls, Provider, args, options, asynchronous=False)

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        transport: AsyncTransport | None = None,
    ) -> None:
        super().__init__(account, type=type)
        options: dict[str, Any] = {"type": self.type}
        if transport is not None:
            options["transport"] = transport
        self._provider = self._make_async_provider(account, **options)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._call_lock = RLock()
        self.transport: Transport = _ProviderTransport(self)
        self._closed = False
        self._closing = False
        self.events: EventBus[EventEnvelope[str]] = EventBus(error_policy=ErrorPolicy.ISOLATE)
        self._pending_events: deque[EventEnvelope[str]] = deque()
        self._publishing = False
        self._provider.runtime.bind_sync_events(self.events, self._pending_events.append)
        self.hooks: OperationHooks = OperationHooks(self._provider.runtime.hooks)
        self._chats = self._make_chats()
        self._projects = self._make_projects()
        self.models = ModelsCollection(self)

    def _make_async_provider(self, account: Account, **options: Any) -> AsyncProvider[Any, Any]:
        from .async_provider import AsyncProvider

        return AsyncProvider(account, **options)

    def _make_chats(self) -> _ChatsT:
        return cast("_ChatsT", ChatsCollection(self, Chat))

    def _make_projects(self) -> _ProjectsT:
        return cast("_ProjectsT", ProjectsCollection(self, Project))

    def _wrap_chat(self, resource: AsyncChat) -> Chat:
        return self.chats._wrap(resource)

    def _wrap_project(self, resource: AsyncProject) -> Project:
        return self.projects._wrap(resource)

    def _wrap_message(self, resource: AsyncMessage) -> Message:
        return Message._from_async(self, resource)

    def _wrap_response(self, item: Response, *, chat_view: Chat | None = None) -> Response:
        message = item.message
        if isinstance(message, AsyncMessage):
            message = self._wrap_message(message)
        chat = item.chat
        if isinstance(chat, AsyncChat):
            chat = chat_view if chat_view is not None else self._wrap_chat(chat)
        return type(item)(
            item.content,
            message=message,
            id=item.id,
            remote_id=item.remote_id,
            model=item.model,
            sources=item.sources,
            artifacts=item.artifacts,
            metadata=item.metadata,
            timing=item.timing,
            chat=chat,
        )

    @property
    def capabilities(self) -> frozenset[Capability]:
        return self._provider.capabilities

    @property
    def lifecycle_state(self) -> LifecycleState:
        return self._provider.lifecycle_state

    def _ensure_blocking_call(self) -> None:
        if self._closed:
            raise RuntimeError("The provider is closed")
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        raise RuntimeError("Use AsyncProvider in async code; Provider cannot block an event loop")

    def _call(self, coroutine: Awaitable[_ResultT]) -> _ResultT:
        task = None
        try:
            self._ensure_blocking_call()
            with self._call_lock:
                self._ensure_blocking_call()
                if self._loop is None:
                    self._loop = asyncio.new_event_loop()
                task = asyncio.ensure_future(coroutine, loop=self._loop)
                try:
                    return self._loop.run_until_complete(task)
                except BaseException:
                    if not task.done():
                        task.cancel()
                        self._loop.run_until_complete(asyncio.gather(task, return_exceptions=True))
                    raise
                finally:
                    self._publish_pending_events()
        finally:
            if task is None:
                close = getattr(coroutine, "close", None)
                if close is not None:
                    close()

    def _publish_pending_events(self) -> None:
        if self._publishing:
            return
        self._publishing = True
        try:
            while self._pending_events:
                self.events.publish(self._pending_events.popleft())
        finally:
            self._publishing = False
            if self._closed:
                self.events.close()

    def open(self: _ProviderT) -> _ProviderT:
        self._ensure_blocking_call()
        with self._call_lock:
            if self._closed:
                raise RuntimeError("A closed provider cannot be reopened")
            if not self._provider.runtime.opened:
                self._call(self._provider.open())
        return self

    def close(self) -> None:
        if self._closed:
            return
        if self._loop is not None:
            self._ensure_blocking_call()
        with self._call_lock:
            if self._closed or self._closing:
                return
            loop = self._loop
            if loop is None:
                self.events.close()
                self._closed = True
                return
            self._closing = True
            try:
                self._call(self._provider.close())
            finally:
                self._closing = False
                if self._provider.runtime.closed:
                    self._closed = True
                    try:
                        loop.run_until_complete(_finish_loop())
                    finally:
                        loop.close()
                        if not self._publishing:
                            self.events.close()

    def authenticate(self) -> SessionStatus:
        return self._call(self._provider.authenticate())

    def session_status(self) -> SessionStatus:
        self._ensure_blocking_call()
        runtime = self._provider.runtime
        if runtime.closing or runtime.closed:
            raise ProviderError("Provider is closing or is closed")
        return self._provider._session_status()

    def forget_session(self) -> SessionStatus:
        return self._call(self._provider.forget_session())

    def ask(
        self,
        prompt: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Any = None,
    ) -> Response:
        response = self._call(self._provider.ask(prompt, model=model, attachments=attachments))
        return self._wrap_response(response)

    def __enter__(self: _ProviderT) -> _ProviderT:
        return self.open()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


async def _finish_loop() -> None:
    loop = asyncio.get_running_loop()
    pending = asyncio.all_tasks(loop) - {asyncio.current_task()}
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    await loop.shutdown_asyncgens()
    await loop.shutdown_default_executor()


class _ProviderTransport(Transport):
    """Expose the provider's existing HTTP session through the synchronous API."""

    def __init__(self, owner: Provider[Any, Any]) -> None:
        self._owner = owner

    @property
    def origin(self):
        return self._owner._provider.transport.origin

    @property
    def config(self):
        return self._owner._provider.transport.config

    def url(self, path):
        return self._owner._provider.transport.url(path)

    def record_rate_limit(self, retry_after: float | None) -> None:
        self._owner._ensure_blocking_call()
        with self._owner._call_lock:
            self._owner._ensure_blocking_call()
            self._owner._provider.transport.record_rate_limit(retry_after)

    def send(self, method, path, **options):
        return self._owner._call(self._owner._provider.transport.send(method, path, **options))

    def request(self, method, path, **options):
        return self._owner._call(self._owner._provider.transport.request(method, path, **options))

    @contextmanager
    def stream(self, method, path, **options):
        manager = self._owner._provider.transport.stream(method, path, **options)
        response = self._owner._call(manager.__aenter__())
        stream = _ProviderStream(self._owner, manager, response)
        try:
            yield stream
        except BaseException as error:
            if not stream._finish(type(error), error, error.__traceback__):
                raise
        else:
            stream.close()

    def close(self):
        self._owner._call(self._owner._provider.transport.close())


class _ProviderStream:
    def __init__(self, owner, manager, response):
        self.status_code = response.status_code
        self.headers = response.headers
        self._owner = owner
        self._manager = manager
        self._chunks = response.aiter_bytes()
        self._reader = None
        self._producer = None
        self._buffer: asyncio.Queue[bytes] = asyncio.Queue(maxsize=64)
        self._ready = asyncio.Event()
        self._done = False
        self._error: Exception | None = None
        self._closed = False
        self._state_lock = Lock()

    async def _fill(self):
        try:
            async for chunk in self._chunks:
                await self._buffer.put(chunk)
                self._ready.set()
        except Exception as error:
            self._error = error
        finally:
            self._done = True
            self._ready.set()

    async def _read_batch(self):
        with self._state_lock:
            if self._closed:
                return ()
            self._reader = asyncio.current_task()
        try:
            if self._producer is None:
                self._producer = asyncio.create_task(self._fill())
            if self._buffer.empty() and not self._done:
                await self._ready.wait()
            self._ready.clear()
            batch = []
            while not self._buffer.empty():
                batch.append(self._buffer.get_nowait())
            if not batch and self._error is not None:
                raise self._error
            return batch
        except asyncio.CancelledError:
            if self._closed:
                return ()
            raise
        finally:
            with self._state_lock:
                self._reader = None

    def iter_bytes(self):
        while not self._closed:
            batch = self._owner._call(self._read_batch())
            if not batch:
                return
            for chunk in batch:
                if self._closed:
                    return
                yield chunk

    async def _exit(self, exception):
        producer = self._producer
        if producer is not None and not producer.done():
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
        try:
            return await self._manager.__aexit__(*exception)
        finally:
            while not self._buffer.empty():
                self._buffer.get_nowait()
            self._error = None

    def _finish(self, *exception):
        with self._state_lock:
            if self._closed:
                return False
            self._closed = True
            reader = self._reader
            if reader is not None:
                reader.get_loop().call_soon_threadsafe(reader.cancel)
        return self._owner._call(self._exit(exception))

    def close(self):
        self._finish(None, None, None)
