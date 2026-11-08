"""Bounded bridges between blocking and async transport contracts."""

import asyncio
import concurrent.futures
import threading
from contextlib import asynccontextmanager, contextmanager

from .base import AsyncStreamResponse, AsyncTransport, Transport


class ThreadedTransport(AsyncTransport):
    """Run a blocking transport in its owning context without buffering the body."""

    def __init__(self, origin, scope, *, config=None, close_callback=None):
        super().__init__(origin, config=config)
        self._scope = scope
        self._close_callback = close_callback

    async def send(self, method, path, **options):
        self.url(path)
        options = self._options(options, method, path)

        def send():
            with self._scope() as transport:
                return transport.send(method, path, **options)

        worker = asyncio.create_task(asyncio.to_thread(send))
        try:
            response = await asyncio.shield(worker)
            self._check_size(response)
            return response
        except asyncio.CancelledError as error:
            try:
                await asyncio.shield(worker)
            finally:
                raise error

    @asynccontextmanager
    async def stream(self, method, path, **options):
        self.url(path)
        options = self._options(options, method, path)
        loop = asyncio.get_running_loop()
        queue = asyncio.Queue(maxsize=32)
        stopped = threading.Event()
        responses = []

        def publish(kind, value=None):
            if stopped.is_set():
                return False
            pending = asyncio.run_coroutine_threadsafe(queue.put((kind, value)), loop)
            while not stopped.is_set():
                try:
                    pending.result(timeout=0.1)
                    return True
                except concurrent.futures.TimeoutError:
                    pass
            pending.cancel()
            return False

        def run():
            try:
                with (
                    self._scope() as transport,
                    transport.stream(method, path, **options) as response,
                ):
                    responses.append(response)
                    if publish("response", response):
                        for chunk in response.iter_bytes():
                            if not publish("chunk", chunk):
                                break
                publish("end")
            except BaseException as error:
                publish("error", error)

        async def receive():
            kind, value = await queue.get()
            if kind == "error":
                raise value
            return kind, value

        async def chunks():
            while True:
                kind, value = await receive()
                if kind == "end":
                    return
                yield value

        worker = asyncio.create_task(asyncio.to_thread(run))
        try:
            _, response = await receive()
            yield AsyncStreamResponse(response, chunks(), self.config.max_stream_bytes)
        finally:
            stopped.set()
            if responses:
                await asyncio.to_thread(responses[0].close)
            await asyncio.shield(worker)

    async def close(self):
        if self._close_callback is not None:
            await self._close_callback()


class PortalTransport(Transport):
    """Blocking facade over the provider's existing async runtime."""

    def __init__(self, call, get_transport):
        self._call = call
        self._get_transport = get_transport

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

        class Stream:
            status_code = response.status_code
            headers = response.headers

            def iter_bytes(self):
                iterator = response.aiter_bytes()
                try:
                    while True:
                        try:
                            yield call(anext(iterator))
                        except StopAsyncIteration:
                            return
                finally:
                    call(iterator.aclose())

        try:
            yield Stream()
        except BaseException as error:
            if not self._call(manager.__aexit__(type(error), error, error.__traceback__)):
                raise
        else:
            self._call(manager.__aexit__(None, None, None))

    def close(self):
        self._call(self._get_transport().close())
