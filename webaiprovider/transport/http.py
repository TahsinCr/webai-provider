"""HTTP implementations with reusable async client and stream strategies."""

import asyncio
import ipaddress
import os
import socket
import sys
import time
from collections.abc import Mapping
from contextlib import (
    AsyncExitStack,
    ExitStack,
    asynccontextmanager,
    contextmanager,
    suppress,
)
from urllib.parse import urlsplit

from ..core import MultiDimensionBoundedCache
from ..core.timing import current_send_timing
from ..errors import ProviderError
from .base import AsyncTransport, Transport, _AsyncStreamResponse, _StreamResponse


def curl_cookie_jar(cookies: Mapping[str, str]):
    """Build a cookie jar for curl_cffi with secure flags matching cookie prefixes."""
    from curl_cffi.requests.cookies import Cookies

    jar = Cookies()
    for name, value in cookies.items():
        jar.set(name, value, secure=name.startswith(("__Secure-", "__Host-")))
    return jar.jar


_PROXY_ENV_VARS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)
_IDEMPOTENT_METHODS = frozenset(("GET", "HEAD"))
_DNS_CACHE: MultiDimensionBoundedCache[tuple[str, int, bool], tuple[str, ...]] = (
    MultiDimensionBoundedCache(max_entries=64, max_bytes=32768, default_ttl=300.0)
)
_IPV6_CACHE_TTL = 180.0
_ipv6_connectivity_cache: tuple[float, bool] | None = None
_CURL_STREAM_PHASES = (
    ("NAMELOOKUP_TIME_T", "transport_dns_ms"),
    ("CONNECT_TIME_T", "transport_to_tcp_connected_ms"),
    ("APPCONNECT_TIME_T", "transport_to_tls_connected_ms"),
    ("PRETRANSFER_TIME_T", "transport_ready_ms"),
    ("STARTTRANSFER_TIME_T", "transport_first_byte_ms"),
)


def _create_ipv6_probe_socket() -> socket.socket:
    return socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)


def _has_ipv6_connectivity() -> bool:
    global _ipv6_connectivity_cache
    now = time.monotonic()
    cached = _ipv6_connectivity_cache
    if cached is not None and now - cached[0] < _IPV6_CACHE_TTL:
        return cached[1]
    reachable = False
    sock = None
    try:
        sock = _create_ipv6_probe_socket()
        sock.connect(("2606:4700:4700::1111", 53))
        reachable = True
    except OSError:
        reachable = False
    finally:
        if sock is not None:
            sock.close()
    _ipv6_connectivity_cache = (now, reachable)
    return reachable


_DEFAULT_IPV6_CHECK = _has_ipv6_connectivity


async def _check_ipv6_connectivity() -> bool:
    if _has_ipv6_connectivity is not _DEFAULT_IPV6_CHECK:
        return bool(_has_ipv6_connectivity())
    cached = _ipv6_connectivity_cache
    if cached is not None and time.monotonic() - cached[0] < _IPV6_CACHE_TTL:
        return cached[1]
    return await asyncio.to_thread(_has_ipv6_connectivity)


def _isinstance_if_type(obj, *classes) -> bool:
    real_classes = tuple(cls for cls in classes if isinstance(cls, type))
    return bool(real_classes) and isinstance(obj, real_classes)


def _is_curl_sync_session(client) -> bool:
    if client is None or "curl_cffi" not in sys.modules:
        return False
    from curl_cffi.requests import Session as CurlSession

    return _isinstance_if_type(client, CurlSession)


def _is_curl_async_session(client) -> bool:
    if client is None or "curl_cffi" not in sys.modules:
        return False
    from curl_cffi.requests import AsyncSession as CurlAsyncSession

    return _isinstance_if_type(client, CurlAsyncSession)


def _is_curl_timeout(exc: BaseException) -> bool:
    if "curl_cffi" not in sys.modules:
        return False
    from curl_cffi.requests.exceptions import Timeout as CurlTimeout

    return _isinstance_if_type(exc, CurlTimeout)


def _is_ip_literal(hostname: str) -> bool:
    try:
        ipaddress.ip_address(hostname.strip("[]").split("%", 1)[0])
        return True
    except ValueError:
        return False


def _validate_resolved_ips(hostname: str, addresses: list[str]) -> tuple[str, ...]:
    folded = hostname.casefold()
    is_localhost = folded == "localhost" or folded.endswith(".localhost")
    for addr in addresses:
        ip = ipaddress.ip_address(addr)
        if ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified:
            raise ValueError("Prohibited destination network address")
        if is_localhost:
            if not ip.is_loopback:
                raise ValueError("Prohibited destination network address")
        elif not ip.is_global:
            raise ValueError("Prohibited destination network address")
    return tuple(addresses)


def _validate_ip_literal(hostname: str) -> None:
    ip = ipaddress.ip_address(hostname.strip("[]").split("%", 1)[0])
    if ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified:
        raise ValueError("Prohibited destination network address")
    if not ip.is_global and not ip.is_loopback:
        raise ValueError("Prohibited destination network address")


def _connect_timeout(timeout) -> float:
    if isinstance(timeout, tuple) and len(timeout) == 2:
        return float(timeout[0])
    return float(timeout)


def _extract_and_validate_ips(hostname: str, records) -> tuple[str, ...]:
    addresses = list(dict.fromkeys(record[4][0].split("%", 1)[0] for record in records))
    if not addresses:
        raise ProviderError("Provider host resolution failed")
    addresses.sort(key=lambda addr: ":" in addr)
    return _validate_resolved_ips(hostname, addresses)


async def _resolve_host_ips(
    hostname: str, port: int, *, use_ipv6: bool, timeout: float
) -> tuple[str, ...]:
    key = (hostname.casefold(), port, use_ipv6)
    cached = _DNS_CACHE.get(key)
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
    _DNS_CACHE.put(key, validated)
    return validated


def _resolve_host_ips_sync(hostname: str, port: int, *, use_ipv6: bool) -> tuple[str, ...]:
    key = (hostname.casefold(), port, use_ipv6)
    cached = _DNS_CACHE.get(key)
    if cached is not None:
        return cached
    family = socket.AF_UNSPEC if use_ipv6 else socket.AF_INET
    try:
        records = socket.getaddrinfo(hostname, port, family=family, type=socket.SOCK_STREAM)
    except OSError:
        raise ProviderError("Provider host resolution failed") from None
    validated = _extract_and_validate_ips(hostname, records)
    _DNS_CACHE.put(key, validated)
    return validated


def _no_proxy_bypassed(hostname: str) -> bool:
    raw = os.environ.get("NO_PROXY") or os.environ.get("no_proxy")
    if not raw:
        return False
    host = hostname.strip("[]").casefold()
    for entry in raw.split(","):
        token = entry.strip().casefold()
        if not token:
            continue
        if token == "*":
            return True
        token = token.lstrip(".")
        if host == token or host.endswith(f".{token}"):
            return True
    return False


def _has_proxy_configured(client, options, *, hostname: str | None = None) -> bool:
    if options.get("proxies") or options.get("proxy"):
        return True
    if getattr(client, "proxies", None):
        return True
    if getattr(client, "trust_env", True):
        if any(os.environ.get(name) for name in _PROXY_ENV_VARS):
            if hostname and _no_proxy_bypassed(hostname):
                return False
            return True
    return False


def _format_resolve_entry(prefix: str, ips: tuple[str, ...]) -> str:
    formatted = [f"[{ip}]" if ":" in ip else ip for ip in ips]
    return f"{prefix}{','.join(formatted)}"


def _apply_curl_resolve(client, prefix: str, entry: str) -> None:
    from curl_cffi import CurlOpt

    existing_resolve = client.curl_options.get(CurlOpt.RESOLVE)
    if existing_resolve:
        if len(existing_resolve) == 1 and existing_resolve[0] == entry:
            return
        has_exact = False
        has_stale = False
        for item in existing_resolve:
            if item == entry:
                has_exact = True
            elif str(item).startswith(prefix):
                has_stale = True
                break
        if has_exact and not has_stale:
            return
        existing = [item for item in existing_resolve if not str(item).startswith(prefix)]
    else:
        existing = []
    existing.append(entry)
    client.curl_options[CurlOpt.RESOLVE] = existing


def _invalidate_host_dns(hostname: str, port: int, client=None) -> None:
    if not hostname:
        return
    folded = hostname.casefold()
    _DNS_CACHE.remove((folded, port, True))
    _DNS_CACHE.remove((folded, port, False))
    if _is_curl_async_session(client) or _is_curl_sync_session(client):
        from curl_cffi import CurlOpt

        prefix = f"{hostname}:{port}:"
        existing = client.curl_options.get(CurlOpt.RESOLVE)
        if existing:
            client.curl_options[CurlOpt.RESOLVE] = [
                entry for entry in existing if not str(entry).startswith(prefix)
            ]


class _OriginDNS:
    """Origin host parsing and cached CurlOpt.RESOLVE formatter."""

    def __init__(self, origin: str) -> None:
        parsed = urlsplit(origin)
        hostname = parsed.hostname or ""
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        self.host = hostname
        self.port = port
        self.prefix = f"{hostname}:{port}:" if hostname else ""
        self.is_ip = bool(hostname and _is_ip_literal(hostname))
        self._resolve_cache: tuple[tuple[str, ...], str] | None = None

    def resolve_entry(self, ips: tuple[str, ...]) -> str:
        cached = self._resolve_cache
        if cached is not None and cached[0] == ips:
            return cached[1]
        entry = _format_resolve_entry(self.prefix, ips)
        self._resolve_cache = (ips, entry)
        return entry

    def invalidate(self, client=None) -> None:
        if not self.host:
            return
        self._resolve_cache = None
        _invalidate_host_dns(self.host, self.port, client)


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


def _prepare_curl_session_sync(
    client,
    hostname: str,
    port: int,
    options: dict | None = None,
    *,
    dns_state: _OriginDNS | None = None,
) -> None:
    if not _is_curl_sync_session(client):
        return
    from curl_cffi import CurlOpt

    ipv6 = _has_ipv6_connectivity()
    if not ipv6 and client.curl_options.get(CurlOpt.IPRESOLVE) != 1:
        client.curl_options[CurlOpt.IPRESOLVE] = 1
    if not hostname:
        return
    if _is_ip_literal(hostname):
        _validate_ip_literal(hostname)
        return
    if _has_proxy_configured(client, options or {}, hostname=hostname):
        return
    ips = _resolve_host_ips_sync(hostname, port, use_ipv6=ipv6)
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
    _redirect_option = "allow_redirects"
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
        self._owned_refs: dict[int, int] = {}
        self._retired_clients: dict[int, object] = {}
        self._dns = _OriginDNS(self.origin)
        self._origin_host = self._dns.host
        self._origin_port = self._dns.port
        self._origin_prefix = self._dns.prefix
        self._origin_is_ip = self._dns.is_ip

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
                await getattr(retired, self._close_method)()

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
            await getattr(client, self._close_method)()

    async def _send(self, method, url, options, *, client=None):
        client = client if client is not None else self._connect()
        await self._prepare_curl(options, client=client)
        sender = getattr(client, "request", None)
        if callable(sender):
            return await sender(method, url, **{self._redirect_option: False}, **options)
        return await getattr(client, method.lower())(
            url, **{self._redirect_option: False}, **options
        )

    async def _acquire_client_async(self):
        return self._acquire_client()

    async def send(self, method, path, *, check_status=True, **options):
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
        async with client.stream(
            method, url, **{self._redirect_option: False}, **options
        ) as response:
            yield response

    @asynccontextmanager
    async def stream(self, method, path, *, check_status=True, **options):
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

    async def close(self):
        try:
            await self._after()
        finally:
            retired = list(self._retired_clients.values())
            self._retired_clients.clear()
            self._owned_refs.clear()
            client, self.client = self.client, None
            if client is not None:
                retired.append(client)
            for item in retired:
                await getattr(item, self._close_method)()


class HTTPTransport(Transport):
    """Blocking HTTP transport backed by ``curl_cffi.requests.Session`` by default."""

    def __init__(
        self, origin, *, client_factory=None, client_getter=None, config=None, headers=None
    ):
        super().__init__(origin, config=config, headers=headers)
        self.factory = client_factory
        self._client_getter = client_getter
        self.client = None
        self._dns = _OriginDNS(self.origin)

    def _connect(self):
        if self._client_getter is not None:
            return self._client_getter()
        if self.client is None:
            factory = self.factory
            if factory is None:
                from curl_cffi.requests import Session as CurlSession

                self.client = CurlSession(timeout=self.config.timeout, trust_env=False)
            else:
                self.client = factory(timeout=self.config.timeout)
        return self.client

    def _reset_owned_client(self, failed_client):
        if self._client_getter is not None or self.client is not failed_client:
            return
        client, self.client = self.client, None
        with suppress(Exception):
            client.close()

    def send(self, method, path, *, check_status=True, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        can_retry = method.upper() in _IDEMPOTENT_METHODS
        for attempt in (0, 1):
            client = self._connect()
            if not callable(getattr(client, "stream", None)):
                try:
                    _prepare_curl_session_sync(
                        client, self._dns.host, self._dns.port, options, dns_state=self._dns
                    )
                    response = client.request(method, url, allow_redirects=False, **options)
                    self._check_response(response, check_status)
                    self._check_size(response)
                    return response
                except BaseException as exc:
                    if attempt == 0 and can_retry and _is_curl_timeout(exc):
                        self._dns.invalidate(client)
                        self._reset_owned_client(client)
                        continue
                    raise

            stack = ExitStack()
            try:
                _prepare_curl_session_sync(
                    client, self._dns.host, self._dns.port, options, dns_state=self._dns
                )
                response = stack.enter_context(
                    client.stream(method, url, allow_redirects=False, **options)
                )
                self._check_response(response, check_status)
            except BaseException as exc:
                stack.close()
                if attempt == 0 and can_retry and _is_curl_timeout(exc):
                    self._dns.invalidate(client)
                    self._reset_owned_client(client)
                    continue
                raise
            try:
                return self._buffer_response(response)
            finally:
                stack.close()
        raise ProviderError("Provider request could not be completed")

    @contextmanager
    def stream(self, method, path, *, check_status=True, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        can_retry = method.upper() in _IDEMPOTENT_METHODS
        with ExitStack() as stack:
            response = None
            for attempt in (0, 1):
                client = self._connect()
                try:
                    _prepare_curl_session_sync(
                        client, self._dns.host, self._dns.port, options, dns_state=self._dns
                    )
                    response = stack.enter_context(
                        client.stream(method, url, allow_redirects=False, **options)
                    )
                    self._check_response(response, check_status)
                    break
                except BaseException as exc:
                    stack.close()
                    if attempt == 0 and can_retry and _is_curl_timeout(exc):
                        self._dns.invalidate(client)
                        self._reset_owned_client(client)
                        continue
                    raise
            yield _StreamResponse(response, self.config.max_stream_bytes)

    def close(self):
        client, self.client = self.client, None
        if client is not None:
            client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
