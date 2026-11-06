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

from ..contracts import StreamEvent, TerminalStatus, TimingMetrics
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
        self._completion_future: asyncio.Future[Response] | None = None
        self._stage_waiters: dict[str, list[asyncio.Future[StreamEvent]]] = {}
        self._stream_task: asyncio.Task[None] | None = None

    def cancel(self) -> None:
        self._cancelled = True
        if self._stream_task is not None and not self._stream_task.done():
            self._stream_task.cancel()
        if self._completion_future is not None and not self._completion_future.done():
            self._completion_future.set_exception(OperationCancelledError("İşlem iptal edildi"))

    async def wait_for(
        self, stage_name: str, *, timeout: float | None = None
    ) -> StreamEvent | None:
        for ev in self._events:
            if ev.event == stage_name:
                return ev

        if self._completed or self._cancelled:
            return None

        loop = asyncio.get_running_loop()
        future: asyncio.Future[StreamEvent] = loop.create_future()
        self._stage_waiters.setdefault(stage_name, []).append(future)

        if not self._started:
            asyncio.create_task(self._run_internal())

        try:
            if timeout is not None:
                return await asyncio.wait_for(future, timeout=timeout)
            return await future
        except asyncio.TimeoutError:
            return None

    async def stream(self) -> AsyncIterator[StreamEvent]:
        if self._started:
            for ev in self._events:
                yield ev
            if self._response is not None:
                return

        self._started = True
        loop = asyncio.get_running_loop()
        if self._completion_future is None:
            self._completion_future = loop.create_future()

        if self._executor is None:
            raise ProviderError("İşlem için çalıştırıcı motor bulunamadı")

        started_at = time.perf_counter()
        first_token_at: float | None = None
        collected_text: list[str] = []
        collected_sources: list[Any] = []
        collected_artifacts: list[Any] = []

        try:
            async for event in self._executor(self):
                if self._cancelled:
                    raise OperationCancelledError("İşlem iptal edildi")

                self._events.append(event)
                await self.hooks.emit(event)

                waiters = self._stage_waiters.pop(event.event, [])
                for w in waiters:
                    if not w.done():
                        w.set_result(event)

                if event.event == "output.delta":
                    if first_token_at is None:
                        first_token_at = time.perf_counter()
                    collected_text.append(getattr(event, "delta", ""))
                elif event.event == "output.replace":
                    if first_token_at is None:
                        first_token_at = time.perf_counter()
                    collected_text = [getattr(event, "text", "")]
                elif event.event == "output.end":
                    if not collected_text:
                        collected_text.append(getattr(event, "text", ""))
                elif event.event == "source.discovered":
                    collected_sources.append(getattr(event, "source", None))
                elif event.event == "artifact.discovered":
                    collected_artifacts.append(getattr(event, "artifact", None))

                yield event

            completed_at = time.perf_counter()
            duration_ms = (completed_at - started_at) * 1000.0
            first_token_ms = (
                (first_token_at - started_at) * 1000.0 if first_token_at is not None else None
            )

            timing = TimingMetrics(
                started_at=started_at,
                first_token_at=first_token_at,
                completed_at=completed_at,
                first_token_ms=first_token_ms,
                duration_ms=duration_ms,
            )

            from .message import Message

            full_text = "".join(collected_text)
            resp_message = Message(
                content=full_text,
                role="assistant",
                model=self.message.model,
                effort=self.message.effort,
                chat=self.chat,
            )
            self._response = Response(
                resp_message,
                sources=[s for s in collected_sources if s is not None],
                artifacts=[a for a in collected_artifacts if a is not None],
                timing=timing,
            )
            self._completed = True
            if self._completion_future and not self._completion_future.done():
                self._completion_future.set_result(self._response)

            term_event = TerminalStatus("completed")
            self._events.append(term_event)
            await self.hooks.emit(term_event)

        except Exception as exc:
            self._completed = True
            term_event = TerminalStatus("failed", error=exc)
            self._events.append(term_event)
            await self.hooks.emit(term_event)

            for stage_waiters in self._stage_waiters.values():
                for w in stage_waiters:
                    if not w.done():
                        w.set_result(term_event)

            if self._completion_future and not self._completion_future.done():
                self._completion_future.set_exception(exc)
            raise

    async def _run_internal(self) -> Response:
        async for _ in self.stream():
            pass
        if self._response is None:
            raise ProviderError("Cevap nesnesi oluşturulamadı")
        return self._response

    async def start(self) -> Response:
        if self._response is not None:
            return self._response
        if self._stream_task is None:
            self._stream_task = asyncio.create_task(self._run_internal())
        return await self._stream_task

    async def wait(self) -> Response:
        return await self.start()

    def __await__(self) -> Any:
        return self.start().__await__()
