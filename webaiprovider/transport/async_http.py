"""Asynchronous HTTP transport and curl connection preparation."""

import asyncio
import socket
import time
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from contextvars import ContextVar
from io import BytesIO

from ..core.timing import current_send_timing
from ..errors import WebAIProviderError
from . import network
from .base import AsyncTransport, _AsyncStreamResponse, _BufferedResponse
from .network import (
    _CURL_STREAM_PHASES,
    _IDEMPOTENT_METHODS,
    _configure_curl_ipresolve,
    _extract_and_validate_ips,
    _is_curl_async_session,
    _is_curl_timeout,
    _OriginDNS,
    _pin_curl_ips,
    _should_pin_curl_host,
    create_curl_share,
    record_curl_timing,
)


async def _check_ipv6_connectivity() -> bool:
    cached = network._ipv6_connectivity_cache
    if cached is not None and time.monotonic() - cached[0] < network._IPV6_CACHE_TTL:
        return cached[1]
    return await asyncio.to_thread(network._has_ipv6_connectivity)


def _connect_timeout(timeout) -> float:
    if isinstance(timeout, tuple) and len(timeout) == 2:
        return float(timeout[0])
    return float(timeout)


async def _async_resolve_host_ips(
    hostname: str, port: int, *, use_ipv6: bool, timeout: float
) -> tuple[str, ...]:
    key = (hostname.casefold(), port, use_ipv6)
    cached = network._DNS_CACHE.get(key)
    if cached is not None:
        return cached
    family = socket.AF_UNSPEC if use_ipv6 else socket.AF_INET
    try:
        records = await asyncio.wait_for(
            asyncio.get_running_loop().getaddrinfo(
                hostname, port, family=family, type=socket.SOCK_STREAM
            ),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        from curl_cffi.requests.exceptions import Timeout as CurlTimeout

        raise CurlTimeout(f"Resolving timed out after {int(timeout * 1000)} ms") from None
    except OSError:
        raise WebAIProviderError("Provider host resolution failed") from None
    validated = _extract_and_validate_ips(hostname, records)
    network._DNS_CACHE.put(key, validated)
    return validated


async def _async_prepare_curl_session(
    client,
    hostname: str,
    port: int,
    options: dict | None = None,
    *,
    dns_state: _OriginDNS | None = None,
    timeout: float = 10.0,
) -> None:
    if not _is_curl_async_session(client):
        return
    ipv6 = await _check_ipv6_connectivity()
    _configure_curl_ipresolve(client, ipv6=ipv6)
    if not _should_pin_curl_host(client, hostname, options, dns_state=dns_state):
        return
    ips = await _async_resolve_host_ips(hostname, port, use_ipv6=ipv6, timeout=timeout)
    _pin_curl_ips(client, hostname, port, ips, dns_state=dns_state)


@asynccontextmanager
async def _open_curl_stream(client, method: str, url: str, options: dict, *, timing=None):
    if timing is not None:
        from curl_cffi import CurlInfo

        infos = tuple(CurlInfo[name] for name, _ in _CURL_STREAM_PHASES) + (CurlInfo.NUM_CONNECTS,)
        if not all(info in client.curl_infos for info in infos):
            client.curl_infos = list(dict.fromkeys((*client.curl_infos, *infos)))
    response = await client.request(method, url, stream=True, allow_redirects=False, **options)
    try:
        if timing is not None:
            record_curl_timing(timing, response.infos.get)
        yield response
    finally:
        task = getattr(response, "astream_task", None)
        try:
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        finally:
            with suppress(asyncio.CancelledError):
                await response.aclose()


class AsyncHTTPTransport(AsyncTransport):
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
        self._owned_refs: dict[int, int] = {}
        self._retired_clients: dict[int, object] = {}
        self._dns = _OriginDNS(self.origin)
        self._close_lock = asyncio.Lock()
        self._idle = asyncio.Event()
        self._idle.set()
        self._active_requests: set[object] = set()
        self._request_context: ContextVar[object | None] = ContextVar(
            "webai_http_request", default=None
        )

    @asynccontextmanager
    async def _activity(self):
        caller = asyncio.current_task()
        request = object()
        if self._request_context.get() in self._active_requests:
            self._active_requests.add(request)
        else:
            async with self._close_lock:
                self._active_requests.add(request)
                self._idle.clear()
        token = self._request_context.set(request)
        try:
            yield
        finally:
            if asyncio.current_task() is caller:
                self._request_context.reset(token)
            self._active_requests.remove(request)
            if not self._active_requests:
                self._idle.set()

    async def _connect(self):
        factory = self.factory
        timeout = self.config.timeout
        if factory is None:
            from curl_cffi.requests import AsyncSession

            self.client = AsyncSession(
                timeout=timeout,
                trust_env=False,
                curl_share=create_curl_share(),
            )
        else:
            self.client = factory(timeout=timeout)
        return self.client

    async def _acquire_client(self):
        if self._client_getter is not None:
            return self._client_getter()
        client = self.client
        if client is None:
            client = await self._connect()
        if client is not None:
            self._owned_refs[id(client)] = self._owned_refs.get(id(client), 0) + 1
        return client

    async def _release_client(self, client):
        if client is None or self._client_getter is not None:
            return
        cid = id(client)
        if cid not in self._owned_refs and cid not in self._retired_clients:
            return
        remaining = self._owned_refs.get(cid, 0) - 1
        if remaining > 0:
            self._owned_refs[cid] = remaining
            return
        self._owned_refs.pop(cid, None)
        retired = self._retired_clients.pop(cid, None)
        if retired is not None:
            with suppress(Exception):
                await retired.close()

    async def _after(self):
        if self._after_response is not None:
            await self._after_response()

    async def _prepare_curl(self, options, *, client):
        await _async_prepare_curl_session(
            client,
            self._dns.host,
            self._dns.port,
            options,
            dns_state=self._dns,
            timeout=_connect_timeout(options.get("timeout", self.config.timeout)),
        )

    async def _reset_owned_client(self, failed_client):
        if self._client_getter is not None or self.client is not failed_client:
            return
        client, self.client = self.client, None
        cid = id(client)
        if self._owned_refs.get(cid, 0) > 0:
            self._retired_clients[cid] = client
            return
        with suppress(Exception):
            await client.close()

    async def _send(self, method, url, options, *, client):
        await self._prepare_curl(options, client=client)
        sender = getattr(client, "request", None)
        if callable(sender):
            return await sender(method, url, allow_redirects=False, **options)
        return await getattr(client, method.lower())(url, allow_redirects=False, **options)

    async def _buffer_response_async(self, response) -> _BufferedResponse:
        self._check_declared_size(response)
        iterator = getattr(response, "aiter_content", None) or getattr(
            response, "aiter_bytes", None
        )
        if not callable(iterator):
            raise WebAIProviderError("Provider response cannot be streamed")
        first = b""
        buffer = None
        limit = self.config.max_response_bytes
        total = 0
        async for chunk in iterator():
            total += len(chunk)
            if total > limit:
                raise WebAIProviderError("Provider response exceeds the size limit")
            if chunk:
                if not first:
                    first = bytes(chunk)
                    continue
                if buffer is None:
                    buffer = BytesIO()
                    buffer.write(first)
                buffer.write(bytes(chunk))
        return _BufferedResponse(response, first if buffer is None else buffer.getvalue())

    async def send(self, method, path, *, check_status=True, **options):
        async with self._activity():
            return await self._send_request(method, path, check_status=check_status, **options)

    async def _send_request(self, method, path, *, check_status, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        can_retry = method.upper() in _IDEMPOTENT_METHODS
        try:
            for attempt in (0, 1):
                client = await self._acquire_client()
                streaming = callable(getattr(client, "stream", None)) or _is_curl_async_session(
                    client
                )
                if not streaming:
                    try:
                        response = await self._send(method, url, options, client=client)
                        self._check_response(response, check_status)
                        self._check_size(response)
                        return response
                    except BaseException as exc:
                        if not getattr(
                            getattr(exc, "response", None), "status_code", 0
                        ) and _is_curl_timeout(exc):
                            self._dns.invalidate(client)
                            if attempt == 0 and can_retry:
                                await self._reset_owned_client(client)
                                continue
                        raise
                    finally:
                        await self._release_client(client)

                stack = AsyncExitStack()
                try:
                    response = await stack.enter_async_context(
                        self._open_stream(method, url, options, client=client)
                    )
                    self._check_response(response, check_status)
                except BaseException as exc:
                    try:
                        await stack.aclose()
                    finally:
                        await self._release_client(client)
                    if not getattr(
                        getattr(exc, "response", None), "status_code", 0
                    ) and _is_curl_timeout(exc):
                        self._dns.invalidate(client)
                        if attempt == 0 and can_retry:
                            await self._reset_owned_client(client)
                            continue
                    raise
                try:
                    return await self._buffer_response_async(response)
                finally:
                    try:
                        await stack.aclose()
                    finally:
                        await self._release_client(client)
            raise WebAIProviderError("Provider request could not be completed")
        finally:
            await self._after()

    @asynccontextmanager
    async def _open_stream(self, method, url, options, *, client, timing=None):
        await self._prepare_curl(options, client=client)
        native = _is_curl_async_session(client)
        if native or not callable(getattr(client, "stream", None)):
            async with _open_curl_stream(
                client, method, url, options, timing=timing if native else None
            ) as response:
                yield response
            return
        async with client.stream(method, url, allow_redirects=False, **options) as response:
            yield response

    @asynccontextmanager
    async def stream(self, method, path, *, check_status=True, **options):
        async with (
            self._activity(),
            self._stream_response(method, path, check_status=check_status, **options) as response,
        ):
            yield response

    @asynccontextmanager
    async def _stream_response(self, method, path, *, check_status, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        can_retry = method.upper() in _IDEMPOTENT_METHODS
        timing = current_send_timing()
        stack = AsyncExitStack()
        client = None
        response = None
        try:
            for attempt in (0, 1):
                client = await self._acquire_client()
                try:
                    response = await stack.enter_async_context(
                        self._open_stream(method, url, options, client=client, timing=timing)
                    )
                    self._check_response(response, check_status)
                    break
                except BaseException as exc:
                    try:
                        await stack.aclose()
                    finally:
                        await self._release_client(client)
                    if not getattr(
                        getattr(exc, "response", None), "status_code", 0
                    ) and _is_curl_timeout(exc):
                        self._dns.invalidate(client)
                        if attempt == 0 and can_retry:
                            await self._reset_owned_client(client)
                            continue
                    raise
            try:
                chunks = (
                    response.aiter_content()
                    if hasattr(response, "aiter_content")
                    else response.aiter_bytes()
                )
                yield _AsyncStreamResponse(response, chunks, self.config.max_stream_bytes)
            finally:
                try:
                    await stack.aclose()
                finally:
                    await self._release_client(client)
        finally:
            await self._after()

    async def _before_close(self):
        await self._after()

    async def _close_clients(self):
        try:
            await self._before_close()
        finally:
            if self.client is not None:
                self._retired_clients[id(self.client)] = self.client
                self.client = None
            for client_id, client in tuple(self._retired_clients.items()):
                await client.close()
                self._retired_clients.pop(client_id)

    async def close(self):
        if self._request_context.get() in self._active_requests:
            raise WebAIProviderError("Cannot close the transport from an active HTTP call")
        async with self._close_lock:
            await self._idle.wait()
            cleanup = asyncio.create_task(self._close_clients())
            cancelled = False
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    cancelled = True
            cleanup.result()
            if cancelled:
                raise asyncio.CancelledError
