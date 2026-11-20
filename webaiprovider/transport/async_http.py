"""Asynchronous HTTP transport and curl connection preparation."""

import asyncio
import socket
import time
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from contextvars import ContextVar
from io import BytesIO

from ..core.timing import current_send_timing
from ..errors import ProviderError
from . import http
from .base import AsyncTransport, _AsyncStreamResponse, _BufferedResponse
from .http import (
    _IDEMPOTENT_METHODS,
    _apply_curl_resolve,
    _extract_and_validate_ips,
    _format_resolve_entry,
    _has_proxy_configured,
    _is_curl_async_session,
    _is_curl_timeout,
    _is_ip_literal,
    _OriginDNS,
    _validate_ip_literal,
)

_CURL_STREAM_PHASES = (
    ("NAMELOOKUP_TIME_T", "transport_dns_ms"),
    ("CONNECT_TIME_T", "transport_to_tcp_connected_ms"),
    ("APPCONNECT_TIME_T", "transport_to_tls_connected_ms"),
    ("PRETRANSFER_TIME_T", "transport_ready_ms"),
    ("STARTTRANSFER_TIME_T", "transport_first_byte_ms"),
)


async def _check_ipv6_connectivity() -> bool:
    cached = http._ipv6_connectivity_cache
    if cached is not None and time.monotonic() - cached[0] < http._IPV6_CACHE_TTL:
        return cached[1]
    return await asyncio.to_thread(http._has_ipv6_connectivity)


def _connect_timeout(timeout) -> float:
    if isinstance(timeout, tuple) and len(timeout) == 2:
        return float(timeout[0])
    return float(timeout)


async def _resolve_host_ips(
    hostname: str, port: int, *, use_ipv6: bool, timeout: float
) -> tuple[str, ...]:
    key = (hostname.casefold(), port, use_ipv6)
    cached = http._DNS_CACHE.get(key)
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
        raise ProviderError("Provider host resolution failed") from None
    validated = _extract_and_validate_ips(hostname, records)
    http._DNS_CACHE.put(key, validated)
    return validated


async def _prepare_curl_session_async(
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
    from curl_cffi import CurlOpt

    ipv6 = await _check_ipv6_connectivity()
    if not ipv6 and client.curl_options.get(CurlOpt.IPRESOLVE) != 1:
        client.curl_options[CurlOpt.IPRESOLVE] = 1
    if not hostname:
        return
    if dns_state.is_ip if dns_state is not None else _is_ip_literal(hostname):
        _validate_ip_literal(hostname)
        return
    if _has_proxy_configured(client, options or {}, hostname=hostname):
        return
    ips = await _resolve_host_ips(hostname, port, use_ipv6=ipv6, timeout=timeout)
    prefix = dns_state.prefix if dns_state is not None else f"{hostname}:{port}:"
    entry = (
        dns_state.resolve_entry(ips)
        if dns_state is not None
        else _format_resolve_entry(prefix, ips)
    )
    _apply_curl_resolve(client, prefix, entry)


@asynccontextmanager
async def _open_curl_stream(client, method: str, url: str, options: dict, *, timing=None):
    if timing is not None:
        from curl_cffi import CurlInfo

        phases = tuple((CurlInfo[name], metric) for name, metric in _CURL_STREAM_PHASES)
        infos = tuple(info for info, _ in phases) + (CurlInfo.NUM_CONNECTS,)
        if not all(info in client.curl_infos for info in infos):
            client.curl_infos = list(dict.fromkeys((*client.curl_infos, *infos)))
    response = await client.request(method, url, stream=True, allow_redirects=False, **options)
    try:
        if timing is not None:
            for info, metric in phases:
                elapsed = response.infos.get(info)
                if isinstance(elapsed, int):
                    timing.record(metric, elapsed * 1000)
            ready = response.infos.get(CurlInfo.PRETRANSFER_TIME_T)
            first_byte = response.infos.get(CurlInfo.STARTTRANSFER_TIME_T)
            if isinstance(ready, int) and isinstance(first_byte, int) and first_byte >= ready:
                timing.record("transport_response_wait_ms", (first_byte - ready) * 1000)
            connections = response.infos.get(CurlInfo.NUM_CONNECTS)
            if isinstance(connections, int):
                timing.increment(
                    "transport_connection_reused"
                    if connections == 0
                    else "transport_connection_opened"
                )
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

    def _connect(self):
        if self._client_getter is not None:
            return self._client_getter()
        if self.client is None:
            factory = self.factory
            is_default_curl = factory is None
            if is_default_curl:
                from curl_cffi.requests import AsyncSession

                factory = AsyncSession
            timeout = self.config.timeout
            if is_default_curl:
                self.client = factory(timeout=timeout, trust_env=False)
            else:
                self.client = factory(timeout=timeout)
        return self.client

    def _acquire_client(self):
        client = self._connect()
        if self._client_getter is None and client is self.client and client is not None:
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

    async def _prepare_curl(self, options, *, client=None):
        client = client if client is not None else self._connect()
        await _prepare_curl_session_async(
            client,
            self._dns.host,
            self._dns.port,
            options,
            dns_state=self._dns,
            timeout=_connect_timeout(options.get("timeout", self.config.timeout)),
        )

    def _invalidate_curl_dns(self, failed_client=None):
        client = (
            failed_client
            if failed_client is not None
            else (self._client_getter() if self._client_getter is not None else self.client)
        )
        self._dns.invalidate(client)

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

    async def _send(self, method, url, options, *, client=None):
        client = client if client is not None else self._connect()
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
            raise ProviderError("Provider response cannot be streamed")
        first = b""
        buffer = None
        limit = self.config.max_response_bytes
        total = 0
        async for chunk in iterator():
            total += len(chunk)
            if total > limit:
                raise ProviderError("Provider response exceeds the size limit")
            if chunk:
                if not first:
                    first = bytes(chunk)
                    continue
                if buffer is None:
                    buffer = BytesIO()
                    buffer.write(first)
                buffer.write(bytes(chunk))
        return _BufferedResponse(response, first if buffer is None else buffer.getvalue())

    async def _acquire_client_async(self):
        return self._acquire_client()

    async def send(self, method, path, *, check_status=True, **options):
        async with self._activity():
            return await self._send_request(method, path, check_status=check_status, **options)

    async def _send_request(self, method, path, *, check_status, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        can_retry = method.upper() in _IDEMPOTENT_METHODS
        try:
            for attempt in (0, 1):
                client = await self._acquire_client_async()
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
                        if attempt == 0 and can_retry and _is_curl_timeout(exc):
                            self._invalidate_curl_dns(client)
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
                    if attempt == 0 and can_retry and _is_curl_timeout(exc):
                        self._invalidate_curl_dns(client)
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
            raise ProviderError("Provider request could not be completed")
        finally:
            await self._after()

    @asynccontextmanager
    async def _open_stream(self, method, url, options, *, client=None, timing=None):
        client = client if client is not None else self._connect()
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
                client = await self._acquire_client_async()
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
                    if attempt == 0 and can_retry and _is_curl_timeout(exc):
                        self._invalidate_curl_dns(client)
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
            raise ProviderError("Cannot close the transport from an active HTTP call")
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
