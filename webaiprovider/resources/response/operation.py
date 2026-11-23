from __future__ import annotations

import asyncio
import contextlib
import sys
import threading
import time
from collections import deque
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import AsyncExitStack, ExitStack
from contextvars import Context, copy_context
from dataclasses import dataclass, field
from enum import Enum
from math import inf, isnan
from typing import TYPE_CHECKING, Any

from eventflow import StateMachine

from ...collections import _async_publish_collection_change, _publish_collection_change
from ...core.async_hooks import AsyncOperationHooks
from ...core.hooks import OperationHooks
from ...core.timing import _STREAM_PAUSED, _stream_deadline, _StreamPause
from ...core.types import HookDispatchResult, OperationContext
from ...errors import OperationCancelledError, ProviderError
from ..message.message import AsyncMessage, Message
from .artifact import Artifact, ArtifactsCollection
from .parts import ResponsePart, TextPart, ThinkingPart
from .response import Response, TimingMetrics
from .source import Source, SourcesCollection

if TYPE_CHECKING:
    from ..chat.chat import AsyncChat, Chat


class OperationState(str, Enum):
    PENDING = "PENDING"
    PREPARING = "PREPARING"
    SENDING = "SENDING"
    STREAMING = "STREAMING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


_OPERATION_TRANSITIONS: dict[OperationState, set[OperationState]] = {
    OperationState.PENDING: {OperationState.PREPARING, OperationState.CANCELLED},
    OperationState.PREPARING: {
        OperationState.SENDING,
        OperationState.FAILED,
        OperationState.CANCELLED,
    },
    OperationState.SENDING: {
        OperationState.STREAMING,
        OperationState.COMPLETED,
        OperationState.FAILED,
        OperationState.CANCELLED,
    },
    OperationState.STREAMING: {
        OperationState.COMPLETED,
        OperationState.FAILED,
        OperationState.CANCELLED,
    },
    OperationState.COMPLETED: set(),
    OperationState.FAILED: set(),
    OperationState.CANCELLED: set(),
}
_OPERATION_TERMINAL_STATES = {
    OperationState.COMPLETED,
    OperationState.FAILED,
    OperationState.CANCELLED,
}


class OperationEvent(str, Enum):
    THINKING_BEGIN = "thinking.begin"
    THINKING_DELTA = "thinking.delta"
    THINKING_REPLACE = "thinking.replace"
    THINKING_END = "thinking.end"
    OUTPUT_BEGIN = "output.begin"
    OUTPUT_DELTA = "output.delta"
    OUTPUT_REPLACE = "output.replace"
    OUTPUT_END = "output.end"
    GENERATION_END = "generation.end"
    VERIFICATION_BEGIN = "verification.begin"
    RESPONSE_PARTS_RESOLVED = "response.parts.resolved"
    SOURCE_DISCOVERED = "source.discovered"
    ARTIFACT_DISCOVERED = "artifact.discovered"
    STATUS_TERMINAL = "status.terminal"


def _close_iterator(stream: Iterator[Any] | None) -> None:
    close = getattr(stream, "close", None)
    if close is not None:
        original = sys.exc_info()[1]
        try:
            close()
        except BaseException as cleanup:
            if original is None:
                raise
            raise original from cleanup


async def _async_close_iterator(stream: AsyncIterator[Any] | None) -> None:
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
    event: OperationEvent


@dataclass(frozen=True, slots=True)
class ThinkingBegin(StreamEvent):
    part_id: str | None = field(default=None, kw_only=True)
    event: OperationEvent = field(init=False, default=OperationEvent.THINKING_BEGIN)


@dataclass(frozen=True, slots=True)
class ThinkingDelta(StreamEvent):
    part_id: str | None = field(default=None, kw_only=True)
    delta: str = ""
    event: OperationEvent = field(init=False, default=OperationEvent.THINKING_DELTA)


@dataclass(frozen=True, slots=True)
class ThinkingReplace(StreamEvent):
    part_id: str | None = field(default=None, kw_only=True)
    text: str = ""
    event: OperationEvent = field(init=False, default=OperationEvent.THINKING_REPLACE)


@dataclass(frozen=True, slots=True)
class ThinkingEnd(StreamEvent):
    part_id: str | None = field(default=None, kw_only=True)
    text: str = ""
    event: OperationEvent = field(init=False, default=OperationEvent.THINKING_END)


@dataclass(frozen=True, slots=True)
class OutputBegin(StreamEvent):
    part_id: str | None = field(default=None, kw_only=True)
    event: OperationEvent = field(init=False, default=OperationEvent.OUTPUT_BEGIN)


@dataclass(frozen=True, slots=True)
class OutputDelta(StreamEvent):
    part_id: str | None = field(default=None, kw_only=True)
    delta: str = ""
    event: OperationEvent = field(init=False, default=OperationEvent.OUTPUT_DELTA)


@dataclass(frozen=True, slots=True)
class OutputReplace(StreamEvent):
    part_id: str | None = field(default=None, kw_only=True)
    text: str = ""
    event: OperationEvent = field(init=False, default=OperationEvent.OUTPUT_REPLACE)


@dataclass(frozen=True, slots=True)
class OutputEnd(StreamEvent):
    part_id: str | None = field(default=None, kw_only=True)
    text: str = ""
    event: OperationEvent = field(init=False, default=OperationEvent.OUTPUT_END)


@dataclass(frozen=True, slots=True)
class GenerationEnd(StreamEvent):
    event: OperationEvent = field(init=False, default=OperationEvent.GENERATION_END)


@dataclass(frozen=True, slots=True)
class VerificationBegin(StreamEvent):
    event: OperationEvent = field(init=False, default=OperationEvent.VERIFICATION_BEGIN)


@dataclass(frozen=True, slots=True)
class ResponsePartsResolved(StreamEvent):
    parts: tuple[ResponsePart, ...] = ()
    event: OperationEvent = field(init=False, default=OperationEvent.RESPONSE_PARTS_RESOLVED)


@dataclass(frozen=True, slots=True)
class SourceDiscovered(StreamEvent):
    source: Source = field(default_factory=lambda: Source("", ""))
    event: OperationEvent = field(init=False, default=OperationEvent.SOURCE_DISCOVERED)


@dataclass(frozen=True, slots=True)
class ArtifactDiscovered(StreamEvent):
    artifact: Artifact = field(default_factory=lambda: Artifact("", "", ""))
    event: OperationEvent = field(init=False, default=OperationEvent.ARTIFACT_DISCOVERED)


@dataclass(frozen=True, slots=True)
class TerminalStatus(StreamEvent):
    status: str = "completed"
    error: Any = None
    event: OperationEvent = field(init=False, default=OperationEvent.STATUS_TERMINAL)


class _StreamAccumulator:
    """Shared stream event collector and response builder for sync/async message operations."""

    def __init__(self, started_at: float) -> None:
        self.started_at = started_at
        self.first_token_at: float | None = None
        self.parts: dict[tuple[bool, str | None], list[str]] = {}
        self.resolved_parts: tuple[ResponsePart, ...] = ()
        self.sources: list[Source] = []
        self.artifacts: list[Artifact] = []

    def record(self, event: StreamEvent) -> None:
        if isinstance(event, (ThinkingBegin, OutputBegin)):
            self.parts.setdefault((isinstance(event, ThinkingBegin), event.part_id), [])
        elif isinstance(event, (ThinkingDelta, ThinkingReplace, OutputDelta, OutputReplace)):
            thinking = isinstance(event, (ThinkingDelta, ThinkingReplace))
            if not thinking and self.first_token_at is None:
                self.first_token_at = time.perf_counter()
            chunks = self.parts.setdefault((thinking, event.part_id), [])
            if isinstance(event, (ThinkingReplace, OutputReplace)):
                chunks[:] = [event.text]
            else:
                chunks.append(event.delta)
        elif isinstance(event, (ThinkingEnd, OutputEnd)) and event.text:
            chunks = self.parts.setdefault((isinstance(event, ThinkingEnd), event.part_id), [])
            if not chunks:
                chunks.append(event.text)
        elif isinstance(event, ResponsePartsResolved):
            self.resolved_parts = event.parts
        elif isinstance(event, SourceDiscovered):
            self.sources.append(event.source)
        elif isinstance(event, ArtifactDiscovered):
            self.artifacts.append(event.artifact)

    def _build_timing(self) -> TimingMetrics:
        completed_at = time.perf_counter()
        return TimingMetrics(
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

    def _build_parts(self) -> list[ResponsePart]:
        parts: list[ResponsePart] = []
        if not any(isinstance(part, TextPart) for part in self.resolved_parts):
            recovered = {part.id for part in self.resolved_parts}
            parts.extend(self.resolved_parts)
            for index, ((thinking, part_id), chunks) in enumerate(self.parts.items(), 1):
                text = "".join(chunks)
                if text and part_id not in recovered:
                    part_type = ThinkingPart if thinking else TextPart
                    parts.append(part_type(part_id or f"local:{index}", text))
        else:
            parts.extend(self.resolved_parts)
        return parts

    def build_response(self, operation: _BaseMessageOperation, session: Any) -> Response:
        timing = self._build_timing()
        parts = self._build_parts()
        existing = operation._response
        if existing is not None:
            existing._set_parts(parts)
            if operation._assistant_message_id:
                existing.id = operation._assistant_message_id
                existing.remote_id = operation._assistant_message_id
            existing.model = operation.message.model
            existing.sources = SourcesCollection(self.sources)
            existing.artifacts = ArtifactsCollection(self.artifacts)
            existing.timing = timing
            existing.chat = operation.chat
            response = existing
        else:
            response = getattr(session, "_response_type", Response)(
                parts=parts,
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

    def _event_offset(self, after: StreamEvent | None) -> int:
        if after is None:
            return 0
        for index, event in enumerate(self._events, 1):
            if event is after:
                return index
        raise ValueError("The event does not belong to this operation")

    def _init_operation_state(self, chat: Any, message: Any, executor: Any) -> None:
        self.chat = chat
        self.message = message
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
        chain.append(self.chat.hooks)
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

    def _snapshot_generation_response(
        self, accumulator: _StreamAccumulator, session: Any, event: StreamEvent
    ) -> None:
        """Materialize response.parts as soon as generation ends, before post-send checks."""
        if isinstance(event, GenerationEnd) and self._response is None:
            self._response = accumulator.build_response(self, session)


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
        self._send_scope = AsyncExitStack()

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
        if self._completed or self.state in _OPERATION_TERMINAL_STATES:
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
        await self.chat.hooks.emit(event)

    async def _record_terminal(self, status: TerminalStatus) -> None:
        # Final state is observable before a contender can acquire the chat lock.
        async with self._condition:
            self._completed = True
            self._events.append(status)
            self._condition.notify_all()
        await self._send_scope.aclose()

    async def wait_for(
        self,
        event_kind: OperationEvent,
        *,
        timeout: float | None = None,
        after: StreamEvent | None = None,
    ) -> StreamEvent | None:
        if not isinstance(event_kind, OperationEvent):
            raise TypeError("Expected an OperationEvent")
        if timeout is not None and isnan(timeout):
            raise ValueError("Stage timeout must not be NaN")
        offset = self._event_offset(after)
        if timeout is not None and timeout <= 0:
            for index in range(offset, len(self._events)):
                event = self._events[index]
                if event.event is event_kind:
                    return event
            return None
        if not self._completed:
            self._ensure_started()

        async def wait_for_event() -> StreamEvent | None:
            index = offset
            async with self._condition:
                while True:
                    while index < len(self._events):
                        event = self._events[index]
                        index += 1
                        if event.event is event_kind:
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
        task = self._stream_task if self._completed else self._ensure_started()
        async with self._condition:
            await self._condition.wait_for(lambda: index < len(self._events) or self._completed)
            end = min(len(self._events), index + limit)
            if end > index:
                return self._events[index:end], end, False
        if task is not None:
            if task is not asyncio.current_task():
                await task
        elif self._cancelled:
            raise OperationCancelledError("Operation cancelled")
        return [], index, True

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
            try:
                async for event in stream:
                    if self._cancelled:
                        raise OperationCancelledError("Operation cancelled")
                    self._transition_to(OperationState.STREAMING)
                    accumulator.record(event)
                    self._snapshot_generation_response(accumulator, session, event)
                    await self._publish(event)
            finally:
                await _async_close_iterator(stream)

            response = accumulator.build_response(self, session)
            context.resource_id = self.chat.remote_id or self.chat.id
            context.result = response
            self._response = response

            self._transition_to(OperationState.COMPLETED)
            await self._record_terminal(TerminalStatus("completed"))
            if emitter is not None:
                if self.chat.remote_id and response.remote_id and not self.chat.temporary:
                    action = self.message.metadata.get("action")
                    await _async_publish_collection_change(
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
            return self._response

        except (asyncio.CancelledError, OperationCancelledError) as exc:
            if self._completed:
                raise
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
            await self._record_terminal(TerminalStatus("cancelled", error))
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
            raise error from None
        except Exception as exc:
            if self._completed:
                raise
            context.error = exc
            self._transition_to(OperationState.FAILED)
            await self._record_terminal(TerminalStatus("failed", exc))
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
            raise
        finally:
            try:
                await self._send_scope.aclose()
                if self._events and isinstance(self._events[-1], TerminalStatus):
                    emit_terminal = self.chat.hooks.emit(self._events[-1])
                    await (asyncio.shield(emit_terminal) if cancelled else emit_terminal)
            finally:
                fin = AsyncOperationHooks.run_lifecycle(
                    hooks_chain, "on_finally", "chat.send", context
                )
                self._hook_results["on_finally"] = await (asyncio.shield(fin) if cancelled else fin)
                self._completed = True
                async with self._condition:
                    self._condition.notify_all()

    async def start(self) -> Response:
        task = self._ensure_started()
        if task is asyncio.current_task():
            if self._response is not None:
                return self._response
            raise ProviderError("Cannot wait for a message operation from its callback")
        return await asyncio.shield(task)

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
        self._send_scope = ExitStack()

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
        self.chat.hooks.emit(event)

    def _record_terminal(self, status: TerminalStatus) -> None:
        with self._condition:
            self._completed = True
            self._events.append(status)
            self._condition.notify_all()
        self._send_scope.close()

    def _drive(self) -> Iterator[StreamEvent | _StreamPause]:
        accumulator = _StreamAccumulator(time.perf_counter())
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
            try:
                for event in stream:
                    if self._cancelled:
                        raise OperationCancelledError("Operation cancelled")
                    if event is _STREAM_PAUSED:
                        yield event
                        continue
                    self._transition_to(OperationState.STREAMING)
                    accumulator.record(event)
                    self._snapshot_generation_response(accumulator, session, event)
                    self._publish(event)
                    yield event
                    if self._cancelled:
                        raise OperationCancelledError("Operation cancelled")
            finally:
                _close_iterator(stream)

            with self._condition:
                if self._cancelled:
                    raise OperationCancelledError("Operation cancelled")
                response = accumulator.build_response(self, session)
                self._response = response
                self._transition_to(OperationState.COMPLETED)
            context.resource_id = self.chat.remote_id or self.chat.id
            context.result = response
            self._record_terminal(TerminalStatus("completed"))
            if emitter is not None:
                if self.chat.remote_id and response.remote_id and not self.chat.temporary:
                    action = self.message.metadata.get("action")
                    _publish_collection_change(
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
        except (OperationCancelledError, GeneratorExit) as cause:
            if self._completed:
                raise
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
            self._record_terminal(TerminalStatus("cancelled", error))
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
            raise
        except BaseException as exc:
            if self._completed:
                raise
            context.error = exc
            self._error = exc
            self._transition_to(OperationState.FAILED)
            self._record_terminal(TerminalStatus("failed", exc))
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
            raise
        finally:
            try:
                self._send_scope.close()
                if self._events and isinstance(self._events[-1], TerminalStatus):
                    self.chat.hooks.emit(self._events[-1])
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
        self._context.run(_close_iterator, self._iterator)

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
                    yield from chunk
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

    def wait_for(
        self,
        event_kind: OperationEvent,
        *,
        timeout: float | None = None,
        after: StreamEvent | None = None,
    ) -> StreamEvent | None:
        """Wait for a stage until the deadline or message completion."""
        if not isinstance(event_kind, OperationEvent):
            raise TypeError("Expected an OperationEvent")
        if timeout is not None and isnan(timeout):
            raise ValueError("Stage timeout must not be NaN")
        deadline = None if timeout is None or timeout == inf else time.monotonic() + timeout
        with self._condition:
            index = self._event_offset(after)
        while True:
            with self._condition:
                while index < len(self._events):
                    event = self._events[index]
                    index += 1
                    if event.event is event_kind:
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
                    if event.event is event_kind:
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
