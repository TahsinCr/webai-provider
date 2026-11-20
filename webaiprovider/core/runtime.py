from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
import threading
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from weakref import WeakValueDictionary

from eventflow import (
    AsyncEventBus,
    AsyncResourceScope,
    BoundedWorkPool,
    ErrorPolicy,
    EventBus,
    EventEnvelope,
    StateMachine,
)

from ..errors import ProviderError
from .async_hooks import AsyncOperationHooks
from .state import (
    _LIFECYCLE_TERMINAL_STATES,
    _LIFECYCLE_TRANSITIONS,
    LifecycleState,
)
from .types import OperationContext


@dataclass(slots=True, eq=False)
class _ActiveOperation:
    runtime_id: int
    send_lock_id: int | None = None
    active: bool = True


_ACTIVE_OPERATIONS: contextvars.ContextVar[tuple[_ActiveOperation, ...]] = contextvars.ContextVar(
    "webai_active_operations", default=()
)
_CLOSING_RUNTIME: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "webai_closing_runtime", default=None
)
_log = logging.getLogger(__name__)


class ProviderRuntime:
    """Single asynchronous owner of provider lifecycle, locks, hooks, and events."""

    def __init__(
        self,
        *,
        provider_id: str,
        account_id: str,
    ) -> None:
        self.provider_id = provider_id
        self.account_id = account_id
        self.events = AsyncEventBus(error_policy=ErrorPolicy.ISOLATE)
        self.sync_events = EventBus(error_policy=ErrorPolicy.ISOLATE)
        self._sync_event_pool: BoundedWorkPool | None = None
        self._sync_event_lock = threading.RLock()
        self._event_sequence = 0
        self.hooks = AsyncOperationHooks()
        self.lifecycle = StateMachine(
            initial=LifecycleState.CREATED,
            transitions=_LIFECYCLE_TRANSITIONS,
            terminal=_LIFECYCLE_TERMINAL_STATES,
        )
        self._scope: AsyncResourceScope | None = None
        self._opened = False
        self._closed = False
        self._closing = False
        self._lifecycle_lock = asyncio.Lock()
        self._lifecycle_condition = asyncio.Condition()
        self._close_task: asyncio.Task[None] | None = None
        self._active_operations = 0
        self._locks: WeakValueDictionary[tuple[str, str], asyncio.Lock] = WeakValueDictionary()

    @property
    def opened(self) -> bool:
        return self._opened

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def closing(self) -> bool:
        return self._closing

    def new_operation_id(self) -> str:
        return str(uuid.uuid4())

    def new_context(
        self,
        name: str,
        *,
        resource_id: str | None = None,
        operation_id: str | None = None,
        **fields: Any,
    ) -> OperationContext:
        return OperationContext(
            name=name,
            operation_id=operation_id or self.new_operation_id(),
            provider_id=self.provider_id,
            account_id=self.account_id,
            resource_id=resource_id,
            **fields,
        )

    def has_active_operation(self, *, send_lock_id: int | None = None) -> bool:
        rid = id(self)
        return any(
            entry.active
            and entry.runtime_id == rid
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

    def send_lock(self, conversation_id: str) -> asyncio.Lock:
        return self._lock("chat", conversation_id)

    def bind_send_lock(self, conversation_id: str, lock: asyncio.Lock) -> None:
        self._locks["chat", conversation_id] = lock

    def project_lock(self, project_id: str) -> asyncio.Lock:
        return self._lock("project", project_id)

    @contextlib.contextmanager
    def operation_marker(self, *, send_lock_id: int | None = None):
        marker = _ActiveOperation(id(self), send_lock_id)
        token = _ACTIVE_OPERATIONS.set((*_ACTIVE_OPERATIONS.get(), marker))
        try:
            yield
        finally:
            marker.active = False
            _ACTIVE_OPERATIONS.reset(token)

    @contextlib.asynccontextmanager
    async def activity(self, *, send_lock_id: int | None = None):
        if send_lock_id is not None and self.has_active_operation(send_lock_id=send_lock_id):
            raise ProviderError("A send hook cannot await another send in the same conversation")
        nested = self.has_active_operation()
        with self.operation_marker(send_lock_id=send_lock_id):
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

    async def emit(
        self,
        name: str,
        *,
        operation_id: str | None = None,
        resource_id: str | None = None,
        payload: object | None = None,
    ) -> None:
        """Publish an event to async subscribers and bounded off-loop sync subscribers.

        Async ``self.events`` delivery is awaited directly without dropping.
        When ``self.sync_events`` has subscribers, delivery runs on a bounded
        single-worker pool. Saturated sync dispatches are logged.
        """
        self._event_sequence += 1
        sync_subscribers = self.sync_events.subscriber_count
        if not sync_subscribers and not self.events.subscriber_count:
            return
        event = EventEnvelope(
            event=name,
            correlation_id=operation_id,
            source=self.provider_id,
            metadata={
                "account_id": self.account_id,
                "resource_id": resource_id,
                "sequence": self._event_sequence,
                "payload": payload,
            },
        )
        if sync_subscribers:
            with self._sync_event_lock:
                if self._sync_event_pool is None:
                    self._sync_event_pool = BoundedWorkPool(max_workers=1, max_pending=512)
                try:
                    self._sync_event_pool.submit(self.sync_events.publish, event)
                except Exception as error:
                    _log.warning(
                        "Sync event dispatch skipped: event=%s error=%s",
                        name,
                        type(error).__name__,
                    )
        await self.events.publish(event)

    def _close_sync_events(self) -> None:
        with self._sync_event_lock:
            pool, self._sync_event_pool = self._sync_event_pool, None
        try:
            if pool is not None:
                pool.close(wait=True, cancel_futures=False)
        finally:
            self.sync_events.close()

    async def _close_events(self) -> None:
        try:
            await self.events.close()
        finally:
            if self._sync_event_pool is None:
                self.sync_events.close()
            else:
                await asyncio.to_thread(self._close_sync_events)

    async def emit_safe(
        self,
        name: str,
        *,
        operation_id: str | None = None,
        resource_id: str | None = None,
        payload: object | None = None,
    ) -> None:
        """Publish an observer event without letting a dispatch failure affect the caller."""
        try:
            await self.emit(
                name, operation_id=operation_id, resource_id=resource_id, payload=payload
            )
        except Exception as error:
            _log.warning("Event dispatch failed: event=%s error=%s", name, type(error).__name__)

    async def open(
        self,
        prepare_fn: Callable[[], Awaitable[None]],
        close_fn: Callable[[], Awaitable[None]],
        *,
        reuse_open: bool = False,
    ) -> bool:
        """Transition under the lifecycle lock; publish events after releasing it.

        Lifecycle subscribers may therefore await normal provider operations without
        waiting on the lock that is still held by the opening call.
        """
        pending: list[tuple[str, object | None]] = []
        opened_here = False
        try:
            async with self._lifecycle_lock:
                if reuse_open and self._opened:
                    return False
                opened_here = await self._open_unlocked(prepare_fn, close_fn, pending)
        finally:
            for name, payload in pending:
                await self.emit_safe(name, payload=payload)
        return opened_here

    async def _open_unlocked(
        self,
        prepare_fn: Callable[[], Awaitable[None]],
        close_fn: Callable[[], Awaitable[None]],
        pending: list[tuple[str, object | None]],
    ) -> bool:
        if self.lifecycle.state == LifecycleState.CLOSE_FAILED:
            raise ProviderError("Provider cleanup failed; call close() again before reopening")
        if self._closed or (self._closing and not self.has_active_operation()):
            raise ProviderError("A closed provider cannot be reopened")
        if self._opened:
            return False
        if self.lifecycle.can_transition(LifecycleState.OPENING):
            self.lifecycle.transition(LifecycleState.OPENING)
        pending.append(("provider.opening", None))
        try:
            await prepare_fn()
        except BaseException as error:
            if self.lifecycle.can_transition(LifecycleState.OPEN_FAILED):
                self.lifecycle.transition(LifecycleState.OPEN_FAILED)
            pending.append(("provider.open_failed", {"error_type": type(error).__name__}))
            with contextlib.suppress(Exception):
                await close_fn()
            raise
        scope = AsyncResourceScope()
        scope.defer(close_fn)
        self._scope = scope
        self._opened = True
        self._closed = False
        if self.lifecycle.can_transition(LifecycleState.OPEN):
            self.lifecycle.transition(LifecycleState.OPEN)
        pending.append(("provider.opened", None))
        return True

    async def close(self, close_fn: Callable[[], Awaitable[None]]) -> None:
        if _CLOSING_RUNTIME.get() == id(self):
            raise ProviderError("close() cannot be awaited from a provider closing event callback")
        if self.has_active_operation():
            raise ProviderError("Cannot close a provider from one of its active operations")
        async with self._lifecycle_lock:
            if self._closed:
                return
            if self._close_task is None:
                async with self._lifecycle_condition:
                    self._closing = True
                if self.lifecycle.can_transition(LifecycleState.CLOSING):
                    self.lifecycle.transition(LifecycleState.CLOSING)
                self._close_task = asyncio.create_task(self._finish_close(close_fn))
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

    async def _finish_close(self, close_fn: Callable[[], Awaitable[None]]) -> None:
        """Release resources; a failed cleanup keeps its scope so close() can be retried."""
        token = _CLOSING_RUNTIME.set(id(self))
        try:
            await self.emit_safe("provider.closing")
            async with self._lifecycle_condition:
                await self._lifecycle_condition.wait_for(lambda: self._active_operations == 0)
            self._opened = False
            try:
                if self._scope is not None:
                    await self._scope.aclose()
                else:
                    await close_fn()
            except BaseException as error:
                if self.lifecycle.can_transition(LifecycleState.CLOSE_FAILED):
                    self.lifecycle.transition(LifecycleState.CLOSE_FAILED)
                await self.emit_safe(
                    "provider.close_failed", payload={"error_type": type(error).__name__}
                )
                raise
            self._scope = None
            self._closed = True
            if self.lifecycle.can_transition(LifecycleState.CLOSED):
                self.lifecycle.transition(LifecycleState.CLOSED)
            await self.emit_safe("provider.closed")
            await self._close_events()
        finally:
            _CLOSING_RUNTIME.reset(token)
            self._close_task = None
            async with self._lifecycle_condition:
                self._closing = False
                self._lifecycle_condition.notify_all()
