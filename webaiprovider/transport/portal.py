"""Bounded bridges between blocking and async transport contracts."""

import asyncio
import concurrent.futures
import queue
import threading
from contextlib import contextmanager, suppress

from .base import Transport


class PortalTransport(Transport):
    """Blocking facade over the provider's existing async runtime."""

    def __init__(self, call, get_transport, *, close_transport: bool = True):
        self._call = call
        self._get_transport = get_transport
        self._close_transport = close_transport

    @property
    def origin(self):
        return self._get_transport().origin

    @property
    def config(self):
        return self._get_transport().config

    def url(self, path):
        return self._get_transport().url(path)

    def send(self, method, path, **options):
        return self._call(self._get_transport().send(method, path, **options))

    def request(self, method, path, **options):
        return self._call(self._get_transport().request(method, path, **options))

    @contextmanager
    def stream(self, method, path, **options):
        manager = self._get_transport().stream(method, path, **options)
        response = self._call(manager.__aenter__())
        call = self._call
        state_lock = threading.Lock()
        closed = False
        reader_done = False
        chunk_queue: queue.SimpleQueue[tuple[str, object]] = queue.SimpleQueue()
        active_task: asyncio.Task | None = None
        task_loop: asyncio.AbstractEventLoop | None = None
        send_event: asyncio.Event | None = None
        done_event: asyncio.Event | None = None
        pending_count = 0
        waiting = False
        max_pending = 64

        def finish(exc_type=None, error=None, traceback=None):
            nonlocal closed
            with state_lock:
                if closed:
                    return False
                closed = True
                task = active_task
                loop = task_loop
                done = done_event
            if task is not None and loop is not None:
                with suppress(RuntimeError):
                    loop.call_soon_threadsafe(task.cancel)
            chunk_queue.put(("closed", None))

            async def finish_async():
                if done is not None:
                    await done.wait()
                return await manager.__aexit__(exc_type, error, traceback)

            return call(finish_async())

        class Stream:
            status_code = response.status_code
            headers = response.headers

            def close(self):
                finish()

            def iter_bytes(self):
                nonlocal reader_done, pending_count, waiting
                iterator = response.aiter_bytes()

                async def run_pump(can_send: asyncio.Event, done: asyncio.Event):
                    nonlocal active_task, task_loop, pending_count, waiting
                    try:
                        async for chunk in iterator:
                            with state_lock:
                                if closed or reader_done:
                                    break
                                pending_count += 1
                                wait_needed = pending_count >= max_pending
                                if wait_needed:
                                    can_send.clear()
                                    waiting = True
                            chunk_queue.put(("chunk", chunk))
                            if wait_needed:
                                await can_send.wait()
                        else:
                            chunk_queue.put(("end", None))
                    except asyncio.CancelledError as exc:
                        with state_lock:
                            was_stopped = closed or reader_done
                        if not was_stopped:
                            chunk_queue.put(("error", exc))
                    except BaseException as exc:
                        chunk_queue.put(("error", exc))
                    finally:
                        try:
                            with suppress(
                                asyncio.CancelledError,
                                concurrent.futures.CancelledError,
                                StopAsyncIteration,
                            ):
                                await iterator.aclose()
                        finally:
                            with state_lock:
                                if active_task is asyncio.current_task():
                                    active_task = None
                                    task_loop = None
                            done.set()

                async def start_pump() -> bool:
                    nonlocal active_task, task_loop, send_event, done_event
                    loop = asyncio.get_running_loop()
                    with state_lock:
                        if closed:
                            return False
                        if active_task is not None:
                            raise RuntimeError("Concurrent reads from one stream are unsupported")
                        can_send = asyncio.Event()
                        can_send.set()
                        done = asyncio.Event()
                        send_event = can_send
                        done_event = done
                        task_loop = loop
                        active_task = loop.create_task(run_pump(can_send, done))
                    return True

                async def wait_pump_done():
                    with state_lock:
                        done = done_event
                    if done is not None:
                        await done.wait()

                if not call(start_pump()):
                    return

                try:
                    while True:
                        kind, payload = chunk_queue.get()
                        if kind == "chunk":
                            with state_lock:
                                pending_count -= 1
                                should_wake = waiting and pending_count <= max_pending // 2
                                if should_wake:
                                    waiting = False
                                can_send = send_event
                                loop = task_loop
                            if should_wake and can_send is not None and loop is not None:
                                with suppress(RuntimeError):
                                    loop.call_soon_threadsafe(can_send.set)
                            yield payload
                        elif kind in ("end", "closed"):
                            return
                        elif kind == "error":
                            raise payload  # type: ignore[misc]
                finally:
                    with state_lock:
                        reader_done = True
                        task = active_task
                        loop = task_loop
                    if task is not None and loop is not None:
                        with suppress(RuntimeError):
                            loop.call_soon_threadsafe(task.cancel)
                    with suppress(
                        asyncio.CancelledError,
                        concurrent.futures.CancelledError,
                        StopAsyncIteration,
                    ):
                        call(wait_pump_done())

        try:
            yield Stream()
        except BaseException as error:
            if not finish(type(error), error, error.__traceback__):
                raise
        else:
            finish()

    def close(self):
        if self._close_transport:
            self._call(self._get_transport().close())
