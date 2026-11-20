from __future__ import annotations

import asyncio
import threading
from collections import Counter, deque
from collections.abc import Awaitable
from contextlib import contextmanager
from contextvars import ContextVar
from threading import Condition, Lock
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
from .transport.base import TransportResponse

if TYPE_CHECKING:
    from typing_extensions import TypeVar as DefaultTypeVar

    from .accounts.account import Account
    from .async_provider import AsyncProvider
    from .resources.project.project import AsyncProject
    from .resources.project.types import ProjectData, ProjectOptions


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


class _CallEvents:
    __slots__ = ("active", "items")

    def __init__(self) -> None:
        self.active = True
        self.items: list[EventEnvelope[str]] = []


class _CallWaiter:
    """Coroutine execution state shared between a caller thread and loop driver."""

    __slots__ = ("cancelled", "coroutine", "done", "error", "events", "task", "value")

    def __init__(self, coroutine: Awaitable[Any]) -> None:
        self.coroutine = coroutine
        self.events = _CallEvents()
        self.task: asyncio.Future[Any] | None = None
        self.cancelled = False
        self.done = False
        self.value: Any = None
        self.error: BaseException | None = None

    def result(self) -> Any:
        if self.error is not None:
            raise self.error
        return self.value


class Provider(BaseProvider[_ChatsT, _ProjectsT, ModelsCollection]):
    """Run overlapping provider operations on a cooperative caller-driven event loop."""

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
        self._state_lock = Condition(Lock())
        self._driver_waiter: _CallWaiter | None = None
        self._pending_calls = 0
        self._transport_stream_threads: Counter[int] = Counter()
        self.transport: Transport = _ProviderTransport(self)
        self._closed = False
        self.events: EventBus[EventEnvelope[str]] = EventBus(error_policy=ErrorPolicy.ISOLATE)
        self._pending_events: deque[EventEnvelope[str]] = deque()
        self._call_events: ContextVar[_CallEvents | None] = ContextVar(
            "webai_call_events", default=None
        )
        self._publish_lock = Lock()
        self._publishing = 0
        self._close_events_requested = False
        self._provider._bind_sync_events(self.events, self._enqueue_event)
        self.hooks: OperationHooks = OperationHooks(self._provider.hooks)
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

    def _start_waiter(self, waiter: _CallWaiter) -> None:
        token = self._call_events.set(waiter.events)
        try:
            task = asyncio.ensure_future(waiter.coroutine, loop=asyncio.get_running_loop())
        except Exception as error:
            close = getattr(waiter.coroutine, "close", None)
            if close is not None:
                close()
            self._complete_waiter(waiter, error, None)
            return
        finally:
            self._call_events.reset(token)
        waiter.task = task
        task.add_done_callback(lambda completed: self._on_waiter_done(waiter, completed))
        if waiter.cancelled:
            task.cancel()

    def _cancel_waiter(self, waiter: _CallWaiter) -> None:
        waiter.cancelled = True
        if waiter.task is not None and not waiter.task.done():
            waiter.task.cancel()

    def _on_waiter_done(self, waiter: _CallWaiter, task: asyncio.Future[Any]) -> None:
        error = asyncio.CancelledError() if task.cancelled() else task.exception()
        self._complete_waiter(waiter, error, None if error is not None else task.result())

    def _complete_waiter(
        self, waiter: _CallWaiter, error: BaseException | None, value: Any
    ) -> None:
        with self._state_lock:
            waiter.error = error
            waiter.value = value
            waiter.done = True
            waiter.events.active = False
            if self._driver_waiter is waiter:
                asyncio.get_running_loop().stop()
            self._state_lock.notify_all()

    def _drive_waiter(self, waiter: _CallWaiter) -> None:
        loop = self._loop
        assert loop is not None
        try:
            while not waiter.done:
                loop.run_forever()
        except BaseException:
            if not waiter.done:
                self._cancel_waiter(waiter)
                while not waiter.done:
                    loop.run_forever()
            raise
        finally:
            with self._state_lock:
                self._driver_waiter = None
                self._state_lock.notify_all()

    def _call(self, coroutine: Awaitable[_ResultT]) -> _ResultT:
        waiter: _CallWaiter | None = None
        try:
            self._ensure_blocking_call()
            with self._state_lock:
                self._ensure_blocking_call()
                if self._loop is None:
                    self._loop = asyncio.new_event_loop()
                pending = _CallWaiter(coroutine)
                self._loop.call_soon_threadsafe(self._start_waiter, pending)
                self._pending_calls += 1
                waiter = pending
                loop = self._loop

            while True:
                with self._state_lock:
                    if waiter.done:
                        return waiter.result()
                    is_driver = self._driver_waiter is None
                    if is_driver:
                        self._driver_waiter = waiter

                if is_driver:
                    self._drive_waiter(waiter)
                else:
                    with self._state_lock:
                        try:
                            self._state_lock.wait_for(
                                lambda: waiter.done or self._driver_waiter is None
                            )
                        except BaseException:
                            loop.call_soon_threadsafe(self._cancel_waiter, waiter)
                            while not waiter.done:
                                if self._driver_waiter is None:
                                    self._driver_waiter = waiter
                                    self._state_lock.release()
                                    try:
                                        self._drive_waiter(waiter)
                                    finally:
                                        self._state_lock.acquire()
                                else:
                                    self._state_lock.wait()
                            raise
        finally:
            if waiter is None:
                close = getattr(coroutine, "close", None)
                if close is not None:
                    close()
            else:
                self._publish_pending_events(waiter.events)

    def _enqueue_event(self, event: EventEnvelope[str]) -> None:
        call = self._call_events.get()
        if call is not None and call.active:
            call.items.append(event)
        else:
            with self._publish_lock:
                self._pending_events.append(event)

    def _publish_pending_events(
        self, call: _CallEvents | None = None, *, close: bool = False
    ) -> None:
        with self._publish_lock:
            self._close_events_requested |= close
            pending = list(self._pending_events)
            self._pending_events.clear()
            if call is not None:
                pending.extend(call.items)
                call.items.clear()
            self._publishing += 1
        if call is not None:
            with self._state_lock:
                self._pending_calls -= 1
                self._state_lock.notify_all()

        try:
            for event in pending:
                self.events.publish(event)
        finally:
            with self._publish_lock:
                self._publishing -= 1
                close_events = self._close_events_requested and self._publishing == 0
                if close_events:
                    self._close_events_requested = False
            if close_events:
                self.events.close()

    def open(self: _ProviderT) -> _ProviderT:
        self._ensure_blocking_call()
        if not self._provider.opened or self._provider.closing:
            self._call(self._provider.open())
        return self

    def close(self) -> None:
        with self._state_lock:
            if self._closed:
                return
            if self._transport_stream_threads[threading.get_ident()]:
                raise ProviderError("Close the active HTTP stream before closing its provider")
            unused = self._loop is None
            if unused:
                self._closed = True

        if unused:
            self._publish_pending_events(close=True)
            return

        try:
            self._call(self._provider.close())
        except RuntimeError:
            with self._state_lock:
                if self._closed:
                    return
            raise

        with self._state_lock:
            while self._driver_waiter is not None or self._pending_calls:
                self._state_lock.wait()
            if self._closed:
                return
            self._closed = True
            loop, self._loop = self._loop, None
        try:
            if loop is not None:
                try:
                    loop.run_until_complete(_finish_loop())
                finally:
                    loop.close()
        finally:
            self._publish_pending_events(close=True)

    def authenticate(self) -> SessionStatus:
        return self._call(self._provider.authenticate())

    def session_status(self) -> SessionStatus:
        self._ensure_blocking_call()
        if self._provider.closing or self._provider.closed:
            raise ProviderError("Provider is closing or is closed")
        return self._provider._session_status()

    def conversation_attachment_count(self, conversation_id: str) -> int | None:
        return self._call(self._provider.conversation_attachment_count(conversation_id))

    def upload_file(self, path: Any) -> dict[str, Any]:
        return self._call(self._provider.upload_file(path))

    def project_options(self, project_id: str | None = None) -> ProjectOptions:
        return self._call(self._provider.project_options(project_id))

    def update_project(self, project: ProjectData) -> ProjectData:
        return self._call(self._provider.update_project(project))

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
        with self._owner._state_lock:
            self._owner._ensure_blocking_call()
            self._owner._provider.transport.record_rate_limit(retry_after)

    async def _send(self, method: str, path: str, options: dict[str, Any]) -> TransportResponse:
        provider = self._owner._provider
        async with provider._activity():
            return await provider.transport.send(method, path, **options)

    async def _request(self, method: str, path: str, options: dict[str, Any]) -> Any:
        provider = self._owner._provider
        async with provider._activity():
            return await provider.transport.request(method, path, **options)

    def send(self, method: str, path: str, **options) -> TransportResponse:
        return self._owner._call(self._send(method, path, options))

    def request(self, method: str, path: str, **options) -> Any:
        return self._owner._call(self._request(method, path, options))

    async def _open_stream(self, caller: int, method, path, options):
        owner = self._owner
        provider = owner._provider
        await provider._begin_activity()
        try:
            manager = provider.transport.stream(method, path, **options)
            response = await manager.__aenter__()
            try:
                stream = _ProviderStream(owner, caller, manager, response)
                with owner._state_lock:
                    owner._transport_stream_threads[caller] += 1
            except BaseException as error:
                await manager.__aexit__(type(error), error, error.__traceback__)
                raise
        except BaseException:
            await provider._end_activity()
            raise
        return stream

    @contextmanager
    def stream(self, method, path, **options):
        stream = self._owner._call(self._open_stream(threading.get_ident(), method, path, options))
        try:
            yield stream
        except BaseException as error:
            if not stream._finish(type(error), error, error.__traceback__):
                raise
        else:
            stream.close()

    def close(self):
        self._owner.close()


class _ProviderStream:
    def __init__(self, owner, caller, manager, response):
        self.status_code = response.status_code
        self.headers = response.headers
        self._owner = owner
        self._manager = manager
        self._caller = caller
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
        try:
            producer = self._producer
            if producer is not None and not producer.done():
                producer.cancel()
                await asyncio.gather(producer, return_exceptions=True)
            return await self._manager.__aexit__(*exception)
        finally:
            while not self._buffer.empty():
                self._buffer.get_nowait()
            self._error = None
            owner = self._owner
            with owner._state_lock:
                owners = owner._transport_stream_threads
                owners[self._caller] -= 1
                if not owners[self._caller]:
                    del owners[self._caller]
            await owner._provider._end_activity()

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
