"""HTTP implementations with reusable async client and stream strategies."""

import asyncio
from contextlib import asynccontextmanager, contextmanager

from .base import AsyncStreamResponse, AsyncTransport, StreamResponse, Transport


class AsyncHTTPTransport(AsyncTransport):
    _redirect_option = "allow_redirects"
    _byte_iterator = "aiter_content"
    _close_method = "close"

    def __init__(
        self,
        origin,
        *,
        client_factory=None,
        client_getter=None,
        after_response=None,
        config=None,
        headers=None,
    ):
        super().__init__(origin, config=config, headers=headers)
        self.factory = client_factory
        self._client_getter = client_getter
        self._after_response = after_response
        self.client = None

    def _connect(self):
        if self._client_getter is not None:
            return self._client_getter()
        if self.client is None:
            factory = self.factory
            if factory is None:
                from curl_cffi.requests import AsyncSession

                factory = AsyncSession
            self.client = factory(timeout=self.config.timeout)
        return self.client

    async def _after(self):
        if self._after_response is not None:
            await self._after_response()

    async def _send(self, method, url, options):
        return await self._connect().request(
            method, url, **{self._redirect_option: False}, **options
        )

    async def send(self, method, path, *, check_status=True, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        response = await self._send(method, url, options)
        try:
            self._check_response(response, check_status)
            self._check_size(response)
            return response
        finally:
            await self._after()

    @asynccontextmanager
    async def _open_stream(self, method, url, options):
        async with self._connect().stream(
            method, url, **{self._redirect_option: False}, **options
        ) as response:
            yield response

    @asynccontextmanager
    async def stream(self, method, path, *, check_status=True, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        try:
            async with self._open_stream(method, url, options) as response:
                self._check_response(response, check_status)
                yield AsyncStreamResponse(
                    response, getattr(response, self._byte_iterator)(), self.config.max_stream_bytes
                )
        finally:
            await self._after()

    async def close(self):
        try:
            await self._after()
        finally:
            client, self.client = self.client, None
            if client is not None:
                await getattr(client, self._close_method)()


class AsyncHTTPXTransport(AsyncHTTPTransport):
    _redirect_option = "follow_redirects"
    _byte_iterator = "aiter_bytes"
    _close_method = "aclose"

    def __init__(self, origin, **options):
        if not options.get("client_factory") and not options.get("client_getter"):
            import httpx

            options["client_factory"] = httpx.AsyncClient
        super().__init__(origin, **options)


class AsyncRequestTransport(AsyncHTTPTransport):
    """Clients that open streaming responses with request(stream=True)."""

    async def _send(self, method, url, options):
        return await getattr(self._connect(), method.lower())(url, allow_redirects=False, **options)

    @asynccontextmanager
    async def _open_stream(self, method, url, options):
        response = await self._connect().request(
            method, url, stream=True, allow_redirects=False, **options
        )
        try:
            yield response
        finally:
            task = getattr(response, "astream_task", None)
            try:
                if isinstance(task, asyncio.Task) and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            finally:
                await response.aclose()


class HTTPTransport(Transport):
    def __init__(
        self, origin, *, client_factory=None, client_getter=None, config=None, headers=None
    ):
        super().__init__(origin, config=config, headers=headers)
        self.factory = client_factory
        self._client_getter = client_getter
        self.client = None

    def _connect(self):
        if self._client_getter is not None:
            return self._client_getter()
        if self.client is None:
            import httpx

            self.client = (self.factory or httpx.Client)(timeout=self.config.timeout)
        return self.client

    def send(self, method, path, *, check_status=True, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        response = self._connect().request(method, url, follow_redirects=False, **options)
        self._check_response(response, check_status)
        self._check_size(response)
        return response

    @contextmanager
    def stream(self, method, path, *, check_status=True, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        with self._connect().stream(method, url, follow_redirects=False, **options) as response:
            self._check_response(response, check_status)
            yield StreamResponse(response, self.config.max_stream_bytes)

    def close(self):
        client, self.client = self.client, None
        if client is not None:
            client.close()
