from __future__ import annotations

import asyncio
import contextlib
import sys
import time
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from eventflow import StateMachine

from ...collections import _publish_collection_change
from ...core.hooks import OperationHooks, SyncOperationHooks
from ...core.state import _OPERATION_TERMINAL_STATES, _OPERATION_TRANSITIONS, OperationState
from ...core.types import HookDispatchResult, OperationContext
from ...errors import OperationCancelledError, ProviderError
from ..response.artifact import Artifact
from ..response.response import Response, TimingMetrics
from ..response.source import Source
from .message import AsyncMessage, Message

if TYPE_CHECKING:
    from ...provider import Provider
    from ..chat.chat import AsyncChat, Chat


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
        self._state_machine = StateMachine(
            initial=OperationState.PENDING,
            transitions=_OPERATION_TRANSITIONS,
            terminal=_OPERATION_TERMINAL_STATES,
        )
        self._completed = False
        self._cancelled = False
        self._events: list[StreamEvent] = []
        self._stream_task: asyncio.Task[Response] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._condition = asyncio.Condition()
        self._hook_results: dict[str, HookDispatchResult] = {}

    @property
    def state(self) -> OperationState:
        return self._state_machine.state

    @property
    def hook_results(self) -> dict[str, HookDispatchResult]:
        return dict(self._hook_results)

    def _transition_to(self, target: OperationState) -> None:
        if self._state_machine.can_transition(target):
            self._state_machine.transition(target)

    def _runtime_and_hooks(self) -> tuple[Any, tuple[OperationHooks, ...]]:
        provider = getattr(self.chat, "provider", None)
        runtime = getattr(provider, "runtime", None) if provider is not None else None
        chain: list[OperationHooks] = []
        if provider is not None and isinstance(getattr(provider, "hooks", None), OperationHooks):
            chain.append(provider.hooks)
        responses = getattr(self.chat, "responses", None)
        if responses is not None and isinstance(getattr(responses, "hooks", None), OperationHooks):
            chain.append(responses.hooks)
        chain.append(self.hooks)
        return runtime, tuple(chain)

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
        started_at = time.perf_counter()
        stream: AsyncIterator[StreamEvent] | None = None
        first_token_at: float | None = None
        collected_text: list[str] = []
        collected_sources: list[Source] = []
        collected_artifacts: list[Artifact] = []
        session = getattr(self.chat, "provider", None)
        runtime, hooks_chain = self._runtime_and_hooks()
        telemetry = getattr(session, "telemetry", None)
        if telemetry is not None:
            telemetry.record_call()

        op_id = runtime.new_operation_id() if runtime is not None else self.message.id
        context = OperationContext(
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
        cancelled = False

        try:
            assert self._executor is not None
            context = await OperationHooks.run_before(hooks_chain, "chat.send", context)
            if context.prompt is not None and context.prompt != self.message.content:
                self.message.content = context.prompt
            if context.model is not None and context.model is not self.message.model:
                self.message.model = context.model

            self._transition_to(OperationState.SENDING)
            if runtime is not None:
                await runtime.emit(
                    "chat.send.started",
                    operation_id=context.operation_id,
                    resource_id=self.chat.remote_id or self.chat.id,
                )

            stream = self._executor(self)
            async for event in stream:
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

            full_text = "".join(collected_text)
            response = getattr(session, "_response_type", Response)(
                full_text,
                message=self.message,
                id=self._assistant_message_id,
                remote_id=self._assistant_message_id,
                model=self.message.model,
                sources=collected_sources,
                artifacts=collected_artifacts,
                timing=timing,
                chat=self.chat,
            )
            self.message.response = response
            if self.chat is not None and hasattr(self.chat, "responses"):
                response = self.chat.responses._reconcile_live_response(self, response)
            if runtime is not None:
                context.resource_id = self.chat.remote_id or self.chat.id
                context.result = response
            self._response = response

            self._transition_to(OperationState.COMPLETED)
            if telemetry is not None:
                telemetry.record_success(duration_s)
            if runtime is not None:
                if self.chat.remote_id and response.remote_id and not self.chat.temporary:
                    action = self.message.metadata.get("action")
                    await _publish_collection_change(
                        runtime,
                        "chat.responses",
                        "update" if action in ("edit", "retry") else "create",
                        response,
                        scope_id=self.chat.remote_id,
                        operation_id=context.operation_id,
                    )
                await runtime.emit_safe(
                    "chat.send.completed",
                    operation_id=context.operation_id,
                    resource_id=self.chat.remote_id or self.chat.id,
                    payload={"message_id": response.id},
                )
            res_success = await OperationHooks.run_lifecycle(
                hooks_chain, "on_success", "chat.send", context
            )
            self._hook_results["on_success"] = res_success
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
            if telemetry is not None:
                telemetry.record_cancelled()
            if runtime is not None:
                with contextlib.suppress(Exception):
                    await asyncio.shield(
                        runtime.emit(
                            "chat.send.cancelled",
                            operation_id=context.operation_id,
                            resource_id=self.chat.remote_id or self.chat.id,
                        )
                    )
            res_cancel = await asyncio.shield(
                OperationHooks.run_lifecycle(hooks_chain, "on_cancelled", "chat.send", context)
            )
            self._hook_results["on_cancelled"] = res_cancel
            await self._finish(TerminalStatus("cancelled", error))
            raise error from None
        except Exception as exc:
            context.error = exc
            self._transition_to(OperationState.FAILED)
            if telemetry is not None:
                telemetry.record_failure(time.perf_counter() - started_at)
            if runtime is not None:
                with contextlib.suppress(Exception):
                    await runtime.emit(
                        "chat.send.failed",
                        operation_id=context.operation_id,
                        resource_id=self.chat.remote_id or self.chat.id,
                        payload={"error_type": type(exc).__name__},
                    )
            res_error = await OperationHooks.run_lifecycle(
                hooks_chain, "on_error", "chat.send", context
            )
            self._hook_results["on_error"] = res_error
            await self._finish(TerminalStatus("failed", exc))
            raise
        finally:
            try:
                await _close_async_iterator(stream)
            finally:
                if cancelled:
                    res_fin = await asyncio.shield(
                        OperationHooks.run_lifecycle(
                            hooks_chain, "on_finally", "chat.send", context
                        )
                    )
                else:
                    res_fin = await OperationHooks.run_lifecycle(
                        hooks_chain, "on_finally", "chat.send", context
                    )
                self._hook_results["on_finally"] = res_fin
                self._completed = True
                async with self._condition:
                    self._condition.notify_all()

    async def start(self) -> Response:
        return await asyncio.shield(self._ensure_started())

    async def wait(self) -> Response:
        return await self.start()

    def __await__(self) -> Any:
        return self.start().__await__()


class MessageOperation:
    def __init__(self, owner: Provider, operation: AsyncMessageOperation) -> None:
        self._owner = owner
        self._operation = operation
        self.hooks = SyncOperationHooks(operation.hooks)

    @property
    def message(self) -> Message:
        return self._owner.message_wrapper_type._from_async(self._owner, self._operation.message)

    @property
    def chat(self) -> Chat:
        return self._owner._wrap_chat(self._operation.chat)

    @property
    def response(self) -> Response | None:
        response = self._operation._response
        return self._owner._wrap_response(response) if response is not None else None

    @property
    def state(self) -> OperationState:
        return self._operation.state

    @property
    def hook_results(self) -> dict[str, HookDispatchResult]:
        return self._operation.hook_results

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
        return self._owner._wrap_response(self._owner._call(self._operation.start()))

    def wait(self) -> Response:
        return self._owner._wrap_response(self._owner._call(self._operation.wait()))
