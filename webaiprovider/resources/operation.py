from __future__ import annotations

import asyncio
import time
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Callable,
    Coroutine,
)

from ..contracts import (
    ArtifactDiscovered,
    OutputDelta,
    OutputEnd,
    OutputReplace,
    SourceDiscovered,
    StreamEvent,
    TerminalStatus,
    TimingMetrics,
)
from ..errors import OperationCancelledError, ProviderError
from .response import Response

if TYPE_CHECKING:
    from .chat import Chat
    from .message import Message

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
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                pass


class MessageOperation:
    def __init__(
        self,
        chat: Chat,
        message: Message,
        *,
        executor: Callable[[MessageOperation], AsyncIterator[StreamEvent]] | None = None,
    ) -> None:
        self.chat = chat
        self.message = message
        self.hooks = OperationHooks()
        self._executor = executor
        self._response: Response | None = None
        self._started = False
        self._completed = False
        self._cancelled = False
        self._events: list[StreamEvent] = []
        self._stream_task: asyncio.Task[Response] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._condition = asyncio.Condition()

    def cancel(self) -> None:
        if self._completed:
            return
        self._cancelled = True
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
            self._started = True
            self._stream_task = self._loop.create_task(self._run_internal())
        return self._stream_task

    async def _publish(self, event: StreamEvent) -> None:
        self._events.append(event)
        async with self._condition:
            self._condition.notify_all()
        await self.hooks.emit(event)

    async def _finish(self, status: TerminalStatus) -> None:
        self._events.append(status)
        self._completed = True
        async with self._condition:
            self._condition.notify_all()
        await self.hooks.emit(status)

    async def wait_for(
        self, stage_name: str, *, timeout: float | None = None
    ) -> StreamEvent | None:
        self._ensure_started()

        async def wait_for_event() -> StreamEvent | None:
            async with self._condition:
                await self._condition.wait_for(
                    lambda: (
                        any(event.event == stage_name for event in self._events) or self._completed
                    )
                )
                return next(
                    (event for event in self._events if event.event == stage_name),
                    None,
                )

        try:
            if timeout is None:
                return await wait_for_event()
            return await asyncio.wait_for(wait_for_event(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def stream(self) -> AsyncIterator[StreamEvent]:
        task = self._ensure_started()
        index = 0
        while True:
            async with self._condition:
                await self._condition.wait_for(lambda: index < len(self._events) or self._completed)
                if index >= len(self._events):
                    break
                event = self._events[index]
                index += 1

            if isinstance(event, TerminalStatus):
                continue
            yield event

        await task

    async def _run_internal(self) -> Response:
        started_at = time.perf_counter()
        first_token_at: float | None = None
        collected_text: list[str] = []
        collected_sources: list[Any] = []
        collected_artifacts: list[Any] = []

        try:
            assert self._executor is not None
            async for event in self._executor(self):
                if self._cancelled:
                    raise OperationCancelledError("Operation cancelled")

                if isinstance(event, OutputDelta):
                    if first_token_at is None:
                        first_token_at = time.perf_counter()
                    collected_text.append(event.delta)
                elif isinstance(event, OutputReplace):
                    if first_token_at is None:
                        first_token_at = time.perf_counter()
                    collected_text = [event.text]
                elif isinstance(event, OutputEnd) and not collected_text and event.text:
                    collected_text.append(event.text)
                elif isinstance(event, SourceDiscovered):
                    collected_sources.append(event.source)
                elif isinstance(event, ArtifactDiscovered):
                    collected_artifacts.append(event.artifact)

                await self._publish(event)

            completed_at = time.perf_counter()
            timing = TimingMetrics(
                started_at=started_at,
                first_token_at=first_token_at,
                completed_at=completed_at,
                first_token_ms=(
                    (first_token_at - started_at) * 1000.0 if first_token_at is not None else None
                ),
                duration_ms=(completed_at - started_at) * 1000.0,
            )

            from .message import Message

            full_text = "".join(collected_text)
            response_message = Message(
                content=full_text,
                role="assistant",
                model=self.message.model,
                chat=self.chat,
            )
            self._response = Response(
                response_message,
                sources=collected_sources,
                artifacts=collected_artifacts,
                timing=timing,
            )
            await self._finish(TerminalStatus("completed"))
            return self._response

        except asyncio.CancelledError:
            error = OperationCancelledError("Operation cancelled")
            self._cancelled = True
            await self._finish(TerminalStatus("cancelled", error))
            raise error from None
        except Exception as exc:
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
