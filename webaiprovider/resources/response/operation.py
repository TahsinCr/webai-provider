from __future__ import annotations

import asyncio
import contextlib
import sys
import threading
import time
from collections import deque
from collections.abc import AsyncIterator, Callable, Iterator
from contextvars import Context, copy_context
from dataclasses import dataclass, field
from math import inf, isnan
from typing import TYPE_CHECKING, Any

from eventflow import StateMachine

from ...collections import _publish_collection_change, _publish_collection_change_sync
from ...core.async_hooks import AsyncOperationHooks
from ...core.hooks import OperationHooks
from ...core.state import _OPERATION_TERMINAL_STATES, _OPERATION_TRANSITIONS, OperationState
from ...core.timing import _STREAM_PAUSED, _stream_deadline, _StreamPause
from ...core.types import HookDispatchResult, OperationContext
from ...errors import OperationCancelledError, ProviderError
from ..message.message import AsyncMessage, Message
from .artifact import Artifact
from .response import Response, TimingMetrics
from .source import Source

if TYPE_CHECKING:
    from ..chat.chat import AsyncChat, Chat


def _close_sync_iterator(stream: Iterator[Any] | None) -> None:
    close = getattr(stream, "close", None)
    if close is not None:
        original = sys.exc_info()[1]
        try:
            close()
        except BaseException as cleanup:
            if original is None:
                raise
            raise original from cleanup


async def _close_async_iterator(stream: AsyncIterator[Any] | None) -> None:
    close = getattr(stream, "aclose", None)
    if close is not None:
        original = sys.exc_info()[1]
        try:
            await close()
        except BaseException as cleanup:
            if original is None:
                raise
            raise original from cleanup


@dataclass(frozen=True, slots=True)
class StreamEvent:
    event: str = field(init=False, default="")


@dataclass(frozen=True, slots=True)
class ThinkingBegin(StreamEvent):
    event: str = field(init=False, default="thinking.begin")


@dataclass(frozen=True, slots=True)
class ThinkingDelta(StreamEvent):
    delta: str = ""
    event: str = field(init=False, default="thinking.delta")


@dataclass(frozen=True, slots=True)
class ThinkingReplace(StreamEvent):
    text: str = ""
    event: str = field(init=False, default="thinking.replace")


@dataclass(frozen=True, slots=True)
class ThinkingEnd(StreamEvent):
    text: str = ""
    event: str = field(init=False, default="thinking.end")


@dataclass(frozen=True, slots=True)
class OutputBegin(StreamEvent):
    event: str = field(init=False, default="output.begin")


@dataclass(frozen=True, slots=True)
class OutputDelta(StreamEvent):
    delta: str = ""
    event: str = field(init=False, default="output.delta")


@dataclass(frozen=True, slots=True)
class OutputReplace(StreamEvent):
    text: str = ""
    event: str = field(init=False, default="output.replace")


@dataclass(frozen=True, slots=True)
class OutputEnd(StreamEvent):
    text: str = ""
    event: str = field(init=False, default="output.end")


@dataclass(frozen=True, slots=True)
class SourceDiscovered(StreamEvent):
    source: Source = field(default_factory=lambda: Source("", ""))
    event: str = field(init=False, default="source.discovered")


@dataclass(frozen=True, slots=True)
class ArtifactDiscovered(StreamEvent):
    artifact: Artifact = field(default_factory=lambda: Artifact("", "", ""))
    event: str = field(init=False, default="artifact.discovered")


@dataclass(frozen=True, slots=True)
class TerminalStatus(StreamEvent):
    status: str = "completed"
    error: Any = None
    event: str = field(init=False, default="status.terminal")


class _StreamAccumulator:
    """Shared stream event collector and response builder for sync/async message operations."""

    def __init__(self, started_at: float) -> None:
        self.started_at = started_at
        self.first_token_at: float | None = None
        self.text: list[str] = []
        self.sources: list[Source] = []
        self.artifacts: list[Artifact] = []

    def record(self, event: StreamEvent) -> None:
        if isinstance(event, OutputDelta):
            if self.first_token_at is None:
                self.first_token_at = time.perf_counter()
            self.text.append(event.delta)
        elif isinstance(event, OutputReplace):
            if self.first_token_at is None:
                self.first_token_at = time.perf_counter()
            self.text = [event.text]
        elif isinstance(event, OutputEnd) and not self.text and event.text:
            self.text.append(event.text)
        elif isinstance(event, SourceDiscovered):
            self.sources.append(event.source)
        elif isinstance(event, ArtifactDiscovered):
            self.artifacts.append(event.artifact)

    def build_response(self, operation: _BaseMessageOperation, session: Any) -> Response:
        completed_at = time.perf_counter()
        timing = TimingMetrics(
            started_at=self.started_at,
            first_token_at=self.first_token_at,
            completed_at=completed_at,
            first_token_ms=(
                (self.first_token_at - self.started_at) * 1000.0
                if self.first_token_at is not None
                else None
            ),
            duration_ms=(completed_at - self.started_at) * 1000.0,
        )
        response = getattr(session, "_response_type", Response)(
            "".join(self.text),
            message=operation.message,
            id=operation._assistant_message_id,
            remote_id=operation._assistant_message_id,
            model=operation.message.model,
            sources=self.sources,
            artifacts=self.artifacts,
            timing=timing,
            chat=operation.chat,
        )
        operation.message.response = response
        if operation.chat is not None and hasattr(operation.chat, "responses"):
            response = operation.chat.responses._reconcile_live_response(operation, response)
        return response


class _BaseMessageOperation:
    """Shared state, hook resolution, and context construction for sync/async operations."""

    _hooks_type: type[Any]
    chat: Any
    message: Any
    hooks: Any

    def _init_operation_state(self, chat: Any, message: Any, executor: Any) -> None:
        self.chat = chat
        self.message = message
        self.hooks = self._hooks_type()
        self._executor = executor
        self._response: Response | None = None
        self._assistant_message_id: str | None = None
        self._state_machine = StateMachine(
            initial=OperationState.PENDING,
            transitions=_OPERATION_TRANSITIONS,
            terminal=_OPERATION_TERMINAL_STATES,
        )
        self._completed = False
        self._cancelled = False
        self._events: list[StreamEvent] = []
        self._hook_results: dict[str, HookDispatchResult] = {}

    @property
    def response(self) -> Response | None:
        return self._response

    @property
    def state(self) -> OperationState:
        return self._state_machine.state

    @property
    def hook_results(self) -> dict[str, HookDispatchResult]:
        return dict(self._hook_results)

    def _transition_to(self, target: OperationState) -> None:
        if self._state_machine.can_transition(target):
            self._state_machine.transition(target)

    def _provider_and_hooks(self) -> tuple[Any, tuple[Any, ...]]:
        provider = getattr(self.chat, "provider", None)
        chain: list[Any] = []
        if provider is not None and isinstance(getattr(provider, "hooks", None), self._hooks_type):
            chain.append(provider.hooks)
        responses = getattr(self.chat, "responses", None)
        if responses is not None and isinstance(
            getattr(responses, "hooks", None), self._hooks_type
        ):
            chain.append(responses.hooks)
        chain.append(self.hooks)
        return provider, tuple(chain)

    def _build_send_context(self, session: Any) -> OperationContext:
        op_id = (
            session._new_operation_id()
            if session is not None and hasattr(session, "_new_operation_id")
            else self.message.id
        )
        return OperationContext(
            name="chat.send",
            operation_id=op_id,
            provider_id=getattr(session, "id", ""),
            account_id=getattr(session, "account_id", ""),
            resource_id=self.chat.remote_id or self.chat.id,
            operation=self,
            prompt=self.message.content,
            model=self.message.model,
            attachments=tuple(self.message.attachments),
            parameters={"chat": self.chat, "message": self.message},
        )

    def _apply_before_context(self, context: OperationContext) -> None:
        if context.prompt is not None and context.prompt != self.message.content:
            self.message.content = context.prompt
        if context.model is not None and context.model is not self.message.model:
            self.message.model = context.model


class AsyncMessageOperation(_BaseMessageOperation):
    _hooks_type = AsyncOperationHooks

    def __init__(
        self,
        chat: AsyncChat,
        message: AsyncMessage,
        *,
        executor: Callable[[AsyncMessageOperation], AsyncIterator[StreamEvent]] | None = None,
    ) -> None:
        self._init_operation_state(chat, message, executor)
        self._stream_task: asyncio.Task[Response] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._condition = asyncio.Condition()

    def cancel(self) -> None:
        loop = self._loop
        if loop is not None and loop.is_running():
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                current_loop = None
            if current_loop is not loop:
                loop.call_soon_threadsafe(self.cancel)
                return
        if self._completed:
            return
        self._cancelled = True
        self._transition_to(OperationState.CANCELLED)
        task = self._stream_task
        loop = self._loop
        if task is not None and loop is not None and not task.done():
            loop.call_soon_threadsafe(task.cancel)
            return
        if task is None:
            self._events.append(
                TerminalStatus("cancelled", OperationCancelledError("Operation cancelled"))
            )
            self._completed = True
            if loop is not None and not loop.is_closed():
                loop.call_soon_threadsafe(lambda: asyncio.create_task(self._notify_waiters()))

    async def _notify_waiters(self) -> None:
        async with self._condition:
            self._condition.notify_all()

    def _ensure_started(self) -> asyncio.Task[Response]:
        if self._cancelled:
            raise OperationCancelledError("Operation cancelled")
        if self._stream_task is None:
            if self._executor is None:
                raise ProviderError("No executor is configured for this operation")
            self._loop = asyncio.get_running_loop()
            self._transition_to(OperationState.PREPARING)
            self._stream_task = self._loop.create_task(self._run_internal())
        return self._stream_task

    async def _publish(self, event: StreamEvent) -> None:
        self._events.append(event)
        async with self._condition:
            self._condition.notify_all()
        await self.hooks.emit(event)

    async def _finish(self, status: TerminalStatus) -> None:
        self._completed = True
        await self._publish(status)

    async def wait_for(
        self, stage_name: str, *, timeout: float | None = None
    ) -> StreamEvent | None:
        self._ensure_started()

        async def wait_for_event() -> StreamEvent | None:
            index = 0
            async with self._condition:
                while True:
                    while index < len(self._events):
                        event = self._events[index]
                        index += 1
                        if event.event == stage_name:
                            return event
                    if self._completed:
                        return None
                    await self._condition.wait()

        try:
            if timeout is None:
                return await wait_for_event()
            return await asyncio.wait_for(wait_for_event(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def _next_stream_batch(
        self, index: int, limit: int = 32
    ) -> tuple[list[StreamEvent], int, bool]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        task = self._ensure_started()
        while True:
            async with self._condition:
                await self._condition.wait_for(
                    lambda current=index: current < len(self._events) or self._completed
                )
                done = index >= len(self._events)
                if not done:
                    end = min(len(self._events), index + limit)
                    chunk = [
                        event
                        for event in self._events[index:end]
                        if not isinstance(event, TerminalStatus)
                    ]
                    index = end
            if done:
                await task
                return [], index, True
            if chunk:
                return chunk, index, False

    async def stream(self) -> AsyncIterator[StreamEvent]:
        index = 0
        while True:
            chunk, index, done = await self._next_stream_batch(index)
            if chunk:
                for event in chunk:
                    yield event
            if done:
                return

    async def _run_internal(self) -> Response:
        accumulator = _StreamAccumulator(time.perf_counter())
        stream: AsyncIterator[StreamEvent] | None = None
        session, hooks_chain = self._provider_and_hooks()
        emitter = session if session is not None and hasattr(session, "_emit") else None
        context = self._build_send_context(session)
        cancelled = False

        try:
            assert self._executor is not None
            context = await AsyncOperationHooks.run_before(hooks_chain, "chat.send", context)
            self._apply_before_context(context)
            self._transition_to(OperationState.SENDING)
            if emitter is not None:
                await emitter._emit(
                    "chat.send.started",
                    operation_id=context.operation_id,
                    resource_id=self.chat.remote_id or self.chat.id,
                )

            stream = self._executor(self)
            async for event in stream:
                if self._cancelled:
                    raise OperationCancelledError("Operation cancelled")
                self._transition_to(OperationState.STREAMING)
                accumulator.record(event)
                await self._publish(event)

            response = accumulator.build_response(self, session)
            if emitter is not None:
                context.resource_id = self.chat.remote_id or self.chat.id
                context.result = response
            self._response = response

            self._transition_to(OperationState.COMPLETED)
            if emitter is not None:
                if self.chat.remote_id and response.remote_id and not self.chat.temporary:
                    action = self.message.metadata.get("action")
                    await _publish_collection_change(
                        emitter,
                        "chat.responses",
                        "update" if action in ("edit", "retry") else "create",
                        response,
                        scope_id=self.chat.remote_id,
                        operation_id=context.operation_id,
                    )
                await emitter._emit_safe(
                    "chat.send.completed",
                    operation_id=context.operation_id,
                    resource_id=self.chat.remote_id or self.chat.id,
                    payload={"message_id": response.id},
                )
            self._hook_results["on_success"] = await AsyncOperationHooks.run_lifecycle(
                hooks_chain, "on_success", "chat.send", context
            )
            await self._finish(TerminalStatus("completed"))
            return self._response

        except (asyncio.CancelledError, OperationCancelledError) as exc:
            cancelled = True
            error = (
                exc
                if isinstance(exc, OperationCancelledError)
                else OperationCancelledError("Operation cancelled")
            )
            context.cancelled = True
            context.error = error
            self._cancelled = True
            self._transition_to(OperationState.CANCELLED)
            if emitter is not None:
                with contextlib.suppress(Exception):
                    await asyncio.shield(
                        emitter._emit(
                            "chat.send.cancelled",
                            operation_id=context.operation_id,
                            resource_id=self.chat.remote_id or self.chat.id,
                        )
                    )
            self._hook_results["on_cancelled"] = await asyncio.shield(
                AsyncOperationHooks.run_lifecycle(hooks_chain, "on_cancelled", "chat.send", context)
            )
            await self._finish(TerminalStatus("cancelled", error))
            raise error from None
        except Exception as exc:
            context.error = exc
            self._transition_to(OperationState.FAILED)
            if emitter is not None:
                with contextlib.suppress(Exception):
                    await emitter._emit(
                        "chat.send.failed",
                        operation_id=context.operation_id,
                        resource_id=self.chat.remote_id or self.chat.id,
                        payload={"error_type": type(exc).__name__},
                    )
            self._hook_results["on_error"] = await AsyncOperationHooks.run_lifecycle(
                hooks_chain, "on_error", "chat.send", context
            )
            await self._finish(TerminalStatus("failed", exc))
            raise
        finally:
            try:
                await _close_async_iterator(stream)
            finally:
                fin = AsyncOperationHooks.run_lifecycle(
                    hooks_chain, "on_finally", "chat.send", context
                )
                self._hook_results["on_finally"] = await (asyncio.shield(fin) if cancelled else fin)
                self._completed = True
                async with self._condition:
                    self._condition.notify_all()

    async def start(self) -> Response:
        return await asyncio.shield(self._ensure_started())

    async def wait(self) -> Response:
        return await self.start()

    def __await__(self) -> Any:
        return self.start().__await__()


class MessageOperation(_BaseMessageOperation):
    """Native blocking message send operation running directly on the caller thread."""

    _hooks_type = OperationHooks

    def __init__(
        self,
        chat: Chat,
        message: Message,
        *,
        executor: Callable[[MessageOperation], Iterator[StreamEvent | _StreamPause]] | None = None,
    ) -> None:
        self._init_operation_state(chat, message, executor)
        self._error: BaseException | None = None
        self._iterator: Iterator[StreamEvent | _StreamPause] | None = None
        self._context: Context | None = None
        self._driver_thread: int | None = None
        self._started = False
        self._condition = threading.Condition()

    def on(self, event_name: str, handler: Any = None) -> Any:
        return self.hooks.on(event_name, handler)

    def cancel(self) -> None:
        with self._condition:
            if self.state in _OPERATION_TERMINAL_STATES:
                return
            self._cancelled = True
            self._transition_to(OperationState.CANCELLED)
            provider = self.chat.provider
            if provider is not None:
                provider._set_send_parked(self, False)
            if not self._started and self._driver_thread is None:
                self._iterator = self._context = None
                self._error = OperationCancelledError("Operation cancelled")
                self._events.append(TerminalStatus("cancelled", self._error))
                self._completed = True
                self._condition.notify_all()
                return
            if self._driver_thread is not None:
                return
            self._driver_thread = threading.get_ident()
        try:
            self._close_driver()
        finally:
            self._release_driver()

    def _publish(self, event: StreamEvent) -> None:
        with self._condition:
            self._events.append(event)
            self._condition.notify_all()
        self.hooks.emit(event)

    def _finish(self, status: TerminalStatus) -> None:
        with self._condition:
            self._events.append(status)
            self._condition.notify_all()
        self.hooks.emit(status)

    def _drive(self) -> Iterator[StreamEvent | _StreamPause]:
        accumulator = _StreamAccumulator(time.perf_counter())
        stream: Iterator[StreamEvent | _StreamPause] | None = None
        session, hooks_chain = self._provider_and_hooks()
        emitter = session if session is not None and hasattr(session, "_emit") else None
        context = self._build_send_context(session)
        try:
            assert self._executor is not None
            context = OperationHooks.run_before(hooks_chain, "chat.send", context)
            self._apply_before_context(context)
            if self._cancelled:
                raise OperationCancelledError("Operation cancelled")
            self._transition_to(OperationState.SENDING)
            if emitter is not None:
                emitter._emit(
                    "chat.send.started",
                    operation_id=context.operation_id,
                    resource_id=self.chat.remote_id or self.chat.id,
                )

            stream = self._executor(self)
            for event in stream:
                if self._cancelled:
                    raise OperationCancelledError("Operation cancelled")
                if event is _STREAM_PAUSED:
                    yield event
                    continue
                self._transition_to(OperationState.STREAMING)
                accumulator.record(event)
                self._publish(event)
                yield event
                if self._cancelled:
                    raise OperationCancelledError("Operation cancelled")

            with self._condition:
                if self._cancelled:
                    raise OperationCancelledError("Operation cancelled")
                response = accumulator.build_response(self, session)
                self._response = response
                self._transition_to(OperationState.COMPLETED)
            if emitter is not None:
                context.resource_id = self.chat.remote_id or self.chat.id
                context.result = response
            if emitter is not None:
                if self.chat.remote_id and response.remote_id and not self.chat.temporary:
                    action = self.message.metadata.get("action")
                    _publish_collection_change_sync(
                        emitter,
                        "chat.responses",
                        "update" if action in ("edit", "retry") else "create",
                        response,
                        scope_id=self.chat.remote_id,
                        operation_id=context.operation_id,
                    )
                emitter._emit_safe(
                    "chat.send.completed",
                    operation_id=context.operation_id,
                    resource_id=self.chat.remote_id or self.chat.id,
                    payload={"message_id": response.id},
                )
            self._hook_results["on_success"] = OperationHooks.run_lifecycle(
                hooks_chain, "on_success", "chat.send", context
            )
            self._finish(TerminalStatus("completed"))
        except (OperationCancelledError, GeneratorExit) as cause:
            error = (
                cause
                if isinstance(cause, OperationCancelledError)
                else OperationCancelledError("Operation stream closed")
            )
            context.cancelled = True
            context.error = error
            self._cancelled = True
            self._error = error
            self._transition_to(OperationState.CANCELLED)
            if emitter is not None:
                with contextlib.suppress(Exception):
                    emitter._emit(
                        "chat.send.cancelled",
                        operation_id=context.operation_id,
                        resource_id=self.chat.remote_id or self.chat.id,
                    )
            self._hook_results["on_cancelled"] = OperationHooks.run_lifecycle(
                hooks_chain, "on_cancelled", "chat.send", context
            )
            self._finish(TerminalStatus("cancelled", error))
            raise
        except BaseException as exc:
            context.error = exc
            self._error = exc
            self._transition_to(OperationState.FAILED)
            if emitter is not None:
                with contextlib.suppress(Exception):
                    emitter._emit(
                        "chat.send.failed",
                        operation_id=context.operation_id,
                        resource_id=self.chat.remote_id or self.chat.id,
                        payload={"error_type": type(exc).__name__},
                    )
            self._hook_results["on_error"] = OperationHooks.run_lifecycle(
                hooks_chain, "on_error", "chat.send", context
            )
            self._finish(TerminalStatus("failed", exc))
            raise
        finally:
            try:
                _close_sync_iterator(stream)
            finally:
                self._hook_results["on_finally"] = OperationHooks.run_lifecycle(
                    hooks_chain, "on_finally", "chat.send", context
                )
                with self._condition:
                    self._completed = True
                    self._condition.notify_all()

    def _claim_driver(self) -> bool:
        if self._completed:
            return False
        caller = threading.get_ident()
        if self._driver_thread == caller:
            raise ProviderError("Cannot drive a message operation from its callback")
        if self._driver_thread is not None:
            return False
        if self._iterator is None:
            if self._executor is None:
                raise ProviderError("No executor is configured for this operation")
            self._transition_to(OperationState.PREPARING)
            self._context = copy_context()
            self._context.run(_stream_deadline.set, None)
            self._iterator = self._drive()
        self._driver_thread = caller
        provider = self.chat.provider
        if provider is not None:
            provider._set_send_parked(self, False)
        return True

    def _release_driver(self) -> None:
        with self._condition:
            provider = self.chat.provider
            if self._completed or (
                not self._cancelled and (provider is None or provider._set_send_parked(self, True))
            ):
                self._clear_driver()
                return
        try:
            self._close_driver()
        finally:
            with self._condition:
                self._clear_driver()

    def _clear_driver(self) -> None:
        self._driver_thread = None
        if self._completed:
            self._iterator = self._context = None
        self._condition.notify_all()

    def _close_driver(self) -> None:
        if not self._started:
            error = OperationCancelledError("Operation cancelled")
            with self._condition:
                self._error = error
                self._transition_to(OperationState.CANCELLED)
                self._events.append(TerminalStatus("cancelled", error))
                self._completed = True
            return
        assert self._context is not None
        self._context.run(_close_sync_iterator, self._iterator)

    def _next_event(self, deadline=None) -> StreamEvent | _StreamPause:
        assert self._context is not None and self._iterator is not None
        self._started = True
        if deadline is None:
            return self._context.run(next, self._iterator)
        return self._context.run(self._next_before_deadline, deadline)

    def _next_before_deadline(self, deadline: float) -> StreamEvent | _StreamPause:
        assert self._iterator is not None
        token = _stream_deadline.set(deadline)
        try:
            return next(self._iterator)
        finally:
            _stream_deadline.reset(token)

    def _result(self) -> Response:
        if self._error is not None:
            raise self._error
        assert self._response is not None
        return self._response

    def stream(self) -> Iterator[StreamEvent]:
        index = 0
        caller = threading.get_ident()
        drove = False
        try:
            while True:
                with self._condition:
                    self._condition.wait_for(
                        lambda: (
                            index < len(self._events)
                            or self._completed
                            or self._driver_thread in (None, caller)
                        )
                    )
                    chunk = self._events[index:]
                    index += len(chunk)
                    done = self._completed
                    drives = not chunk and self._claim_driver()
                if chunk:
                    for event in chunk:
                        if not isinstance(event, TerminalStatus):
                            yield event
                elif done:
                    self._result()
                    return
                elif drives:
                    drove = True
                    try:
                        event = self._next_event()
                    except StopIteration:
                        continue
                    finally:
                        self._release_driver()
                    if event is not _STREAM_PAUSED:
                        index += 1
                        yield event
        except GeneratorExit:
            if drove:
                self.cancel()
            raise

    def wait_for(self, stage_name: str, *, timeout: float | None = None) -> StreamEvent | None:
        """Wait for a stage until the deadline or message completion."""
        if timeout is not None and isnan(timeout):
            raise ValueError("Stage timeout must not be NaN")
        deadline = None if timeout is None or timeout == inf else time.monotonic() + timeout
        index = 0
        while True:
            with self._condition:
                while index < len(self._events):
                    event = self._events[index]
                    index += 1
                    if event.event == stage_name:
                        return event
                remaining = None if deadline is None else deadline - time.monotonic()
                if self._completed or (remaining is not None and remaining <= 0):
                    return None
                if not self._claim_driver():
                    self._condition.wait(
                        timeout=None if remaining is None else min(remaining, threading.TIMEOUT_MAX)
                    )
                    continue
            try:
                while deadline is None or time.monotonic() < deadline:
                    try:
                        event = self._next_event(deadline)
                    except StopIteration:
                        if deadline is not None and time.monotonic() >= deadline:
                            return None
                        break
                    if event is _STREAM_PAUSED:
                        return None
                    index += 1
                    if deadline is not None and time.monotonic() >= deadline:
                        return None
                    if event.event == stage_name:
                        return event
            finally:
                self._release_driver()

    def start(self) -> Response:
        caller = threading.get_ident()
        with self._condition:
            self._condition.wait_for(
                lambda: self._completed or self._driver_thread in (None, caller)
            )
            drives = self._claim_driver()
        if drives:
            try:
                assert self._context is not None and self._iterator is not None
                self._started = True
                self._context.run(deque, self._iterator, maxlen=0)
            finally:
                self._release_driver()
        return self._result()

    def wait(self) -> Response:
        return self.start()
