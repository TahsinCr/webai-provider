from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import AsyncIterator, Callable, Coroutine, Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ...core.state import OperationState, OperationStateMachine
from ...errors import OperationCancelledError, ProviderError
from .message import AsyncMessage, Message
from .response import Artifact, Response, Source, TimingMetrics

if TYPE_CHECKING:
    from ...provider import Provider
    from ..chat.chat import AsyncChat, Chat


@dataclass(frozen=True)
class StreamEvent:
    event: str = field(init=False, default="")


@dataclass(frozen=True)
class ThinkingBegin(StreamEvent):
    event: str = field(init=False, default="thinking.begin")


@dataclass(frozen=True)
class ThinkingDelta(StreamEvent):
    delta: str = ""
    event: str = field(init=False, default="thinking.delta")


@dataclass(frozen=True)
class ThinkingReplace(StreamEvent):
    text: str = ""
    event: str = field(init=False, default="thinking.replace")


@dataclass(frozen=True)
class ThinkingEnd(StreamEvent):
    text: str = ""
    event: str = field(init=False, default="thinking.end")


@dataclass(frozen=True)
class OutputBegin(StreamEvent):
    event: str = field(init=False, default="output.begin")


@dataclass(frozen=True)
class OutputDelta(StreamEvent):
    delta: str = ""
    event: str = field(init=False, default="output.delta")


@dataclass(frozen=True)
class OutputReplace(StreamEvent):
    text: str = ""
    event: str = field(init=False, default="output.replace")


@dataclass(frozen=True)
class OutputEnd(StreamEvent):
    text: str = ""
    event: str = field(init=False, default="output.end")


@dataclass(frozen=True)
class SourceDiscovered(StreamEvent):
    source: Source = field(default_factory=lambda: Source("", ""))
    event: str = field(init=False, default="source.discovered")


@dataclass(frozen=True)
class ArtifactDiscovered(StreamEvent):
    artifact: Artifact = field(default_factory=lambda: Artifact("", "", ""))
    event: str = field(init=False, default="artifact.discovered")


@dataclass(frozen=True)
class TerminalStatus(StreamEvent):
    status: str = "completed"
    error: Any = None
    event: str = field(init=False, default="status.terminal")


HandlerType = Callable[[StreamEvent], Any | Coroutine[Any, Any, Any]]


class OperationHooks:
    def __init__(self) -> None:
        self._handlers: dict[str, list[HandlerType]] = {}

    def on(
        self,
        event_name: str,
        handler: HandlerType | None = None,
    ) -> Any:
        def register(fn: HandlerType) -> HandlerType:
            self._handlers.setdefault(event_name, []).append(fn)
            return fn

        if handler is not None:
            return register(handler)
        return register

    def add(
        self,
        event_name: str,
        handler: HandlerType | None = None,
    ) -> Any:
        return self.on(event_name, handler)

    async def emit(self, event: StreamEvent) -> None:
        handlers = self._handlers.get(event.event, [])
        wildcards = self._handlers.get("*", [])
        all_handlers = handlers + wildcards

        for fn in all_handlers:
            try:
                res = fn(event)
                if inspect.isawaitable(res):
                    await res
            except Exception:
                pass


class AsyncMessageOperation:
    def __init__(
        self,
        chat: AsyncChat,
        message: AsyncMessage,
        *,
        executor: Callable[[AsyncMessageOperation], AsyncIterator[StreamEvent]] | None = None,
    ) -> None:
        self.chat = chat
        self.message = message
        self.hooks = OperationHooks()
        self._executor = executor
        self._response: Response | None = None
        self._assistant_message_id: str | None = None
        self._state_machine = OperationStateMachine()
        self._completed = False
        self._cancelled = False
        self._events: list[StreamEvent] = []
        self._stream_task: asyncio.Task[Response] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._condition = asyncio.Condition()

    @property
    def state(self) -> OperationState:
        return OperationState(self._state_machine.state)

    def _transition_to(self, target: OperationState) -> None:
        if self._state_machine.can_transition(target):
            self._state_machine.transition(target)

    def cancel(self) -> None:
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
        started_at = time.perf_counter()
        first_token_at: float | None = None
        collected_text: list[str] = []
        collected_sources: list[Any] = []
        collected_artifacts: list[Any] = []
        telemetry = getattr(getattr(self.chat, "session", None), "telemetry", None)
        if telemetry is not None:
            telemetry.record_call()

        try:
            assert self._executor is not None
            self._transition_to(OperationState.SENDING)
            async for event in self._executor(self):
                if self._cancelled:
                    raise OperationCancelledError("Operation cancelled")

                self._transition_to(OperationState.STREAMING)

                if isinstance(event, OutputDelta):
                    if first_token_at is None:
                        first_token_at = time.perf_counter()
                        if telemetry is not None:
                            telemetry.record_ttft(first_token_at - started_at)
                    collected_text.append(event.delta)
                elif isinstance(event, OutputReplace):
                    if first_token_at is None:
                        first_token_at = time.perf_counter()
                        if telemetry is not None:
                            telemetry.record_ttft(first_token_at - started_at)
                    collected_text = [event.text]
                elif isinstance(event, OutputEnd) and not collected_text and event.text:
                    collected_text.append(event.text)
                elif isinstance(event, SourceDiscovered):
                    collected_sources.append(event.source)
                elif isinstance(event, ArtifactDiscovered):
                    collected_artifacts.append(event.artifact)

                await self._publish(event)

            completed_at = time.perf_counter()
            duration_s = completed_at - started_at
            timing = TimingMetrics(
                started_at=started_at,
                first_token_at=first_token_at,
                completed_at=completed_at,
                first_token_ms=(
                    (first_token_at - started_at) * 1000.0 if first_token_at is not None else None
                ),
                duration_ms=duration_s * 1000.0,
            )

            from .message import AsyncMessage

            full_text = "".join(collected_text)
            response_message = AsyncMessage(
                content=full_text,
                role="assistant",
                model=self.message.model,
                id=self._assistant_message_id,
                chat=self.chat,
            )
            if self.chat is not None and hasattr(self.chat, "messages"):
                self.chat.messages._items.append(response_message)
            self._response = Response(
                response_message,
                sources=collected_sources,
                artifacts=collected_artifacts,
                timing=timing,
            )
            self._transition_to(OperationState.COMPLETED)
            if telemetry is not None:
                telemetry.record_success(duration_s)
            await self._finish(TerminalStatus("completed"))
            return self._response

        except (asyncio.CancelledError, OperationCancelledError) as exc:
            error = (
                exc
                if isinstance(exc, OperationCancelledError)
                else OperationCancelledError("Operation cancelled")
            )
            self._cancelled = True
            self._transition_to(OperationState.CANCELLED)
            if telemetry is not None:
                telemetry.record_cancelled()
            await self._finish(TerminalStatus("cancelled", error))
            raise error from None
        except Exception as exc:
            self._transition_to(OperationState.FAILED)
            if telemetry is not None:
                telemetry.record_failure(time.perf_counter() - started_at)
            await self._finish(TerminalStatus("failed", exc))
            raise
        finally:
            self._completed = True
            async with self._condition:
                self._condition.notify_all()

    async def start(self) -> Response:
        return await self._ensure_started()

    async def wait(self) -> Response:
        return await self.start()

    def __await__(self) -> Any:
        return self.start().__await__()


class MessageOperation:
    def __init__(self, owner: Provider, operation: AsyncMessageOperation) -> None:
        self._owner = owner
        self._operation = operation
        self.hooks = operation.hooks

    @property
    def message(self) -> Message:
        return Message._from_async(self._owner, self._operation.message)

    @property
    def chat(self) -> Chat:
        return self._owner._wrap_chat(self._operation.chat)

    @property
    def response(self) -> Response | None:
        response = self._operation._response
        return self._wrap_response(response) if response is not None else None

    def _wrap_response(self, response: Response) -> Response:
        message = response.message
        if isinstance(message, AsyncMessage):
            message = Message._from_async(self._owner, message)
        return Response(
            message,
            sources=response.sources,
            artifacts=response.artifacts,
            metadata=response.metadata,
            timing=response.timing,
        )

    @property
    def state(self) -> OperationState:
        return self._operation.state

    def on(self, event_name: str, handler: Any = None) -> Any:
        return self.hooks.on(event_name, handler)

    def cancel(self) -> None:
        self._operation.cancel()

    def wait_for(self, stage_name: str, *, timeout: float | None = None) -> StreamEvent | None:
        return self._owner._call(self._operation.wait_for(stage_name, timeout=timeout))

    def stream(self) -> Iterator[StreamEvent]:
        operation = self._operation
        if (
            isinstance(operation, AsyncMessageOperation)
            and type(operation).stream is AsyncMessageOperation.stream
        ):
            index = 0
            while True:
                chunk, index, done = self._owner._call(operation._next_stream_batch(index))
                if chunk:
                    yield from chunk
                if done:
                    return
        events = operation.stream()
        while True:
            try:
                yield self._owner._call(events.__anext__())
            except StopAsyncIteration:
                return

    def start(self) -> Response:
        return self._wrap_response(self._owner._call(self._operation.start()))

    def wait(self) -> Response:
        return self._wrap_response(self._owner._call(self._operation.wait()))
