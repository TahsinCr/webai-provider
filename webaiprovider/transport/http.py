"""HTTP implementations with reusable async client and stream strategies."""

import asyncio
import ipaddress
import os
import socket
import sys
import time
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from urllib.parse import urlsplit

from ..core import MultiDimensionBoundedCache
from ..errors import ProviderError
from .base import AsyncTransport, Transport, _AsyncStreamResponse, _StreamResponse

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
    MultiDimensionBoundedCache(max_entries=64, max_bytes=32768, default_ttl=60.0)
)
_ipv6_connectivity_cache: tuple[float, bool] | None = None


def _has_ipv6_connectivity() -> bool:
    global _ipv6_connectivity_cache
    now = time.monotonic()
    cached = _ipv6_connectivity_cache
    if cached is not None and now - cached[0] < 30.0:
        return cached[1]
    reachable = False
    sock = None
    try:
        sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        sock.connect(("2606:4700:4700::1111", 53))
        reachable = True
    except OSError:
        reachable = False
    finally:
        if sock is not None:
            sock.close()
    _ipv6_connectivity_cache = (now, reachable)
    return reachable


def _is_curl_async_session(client) -> bool:
    if client is None or "curl_cffi" not in sys.modules:
        return False
    from curl_cffi.requests import AsyncSession as CurlAsyncSession

    return isinstance(client, CurlAsyncSession)


def _is_curl_timeout(exc: BaseException) -> bool:
    if "curl_cffi" not in sys.modules:
        return False
    from curl_cffi.requests.exceptions import Timeout as CurlTimeout

    return isinstance(exc, CurlTimeout)


def _is_httpx_client(client) -> bool:
    if client is None or "httpx" not in sys.modules:
        return False
    import httpx

    return isinstance(client, (httpx.Client, httpx.AsyncClient))


def _coerce_httpx_timeout(timeout):
    if isinstance(timeout, tuple) and len(timeout) == 2:
        import httpx

        return httpx.Timeout(timeout[1], connect=timeout[0])
    return timeout


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


def _connect_timeout(timeout) -> float:
    if isinstance(timeout, tuple) and len(timeout) == 2:
        return float(timeout[0])
    return float(timeout)


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
    except TimeoutError:
        from curl_cffi.requests.exceptions import Timeout as CurlTimeout

        raise CurlTimeout(f"Resolving timed out after {int(timeout * 1000)} ms") from None
    except OSError:
        raise ProviderError("Provider host resolution failed") from None
    addresses = list(dict.fromkeys(record[4][0].split("%", 1)[0] for record in records))
    if not addresses:
        raise ProviderError("Provider host resolution failed")
    validated = _validate_resolved_ips(hostname, addresses)
    _DNS_CACHE.put(key, validated)
    return validated


def _has_proxy_configured(client, options) -> bool:
    if options.get("proxies") or options.get("proxy"):
        return True
    if getattr(client, "proxies", None):
        return True
    if getattr(client, "trust_env", True):
        return any(os.environ.get(name) for name in _PROXY_ENV_VARS)
    return False


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
        self._owned_refs: dict[int, int] = {}
        self._retired_clients: dict[int, object] = {}

    def _connect(self):
        if self._client_getter is not None:
            return self._client_getter()
        if self.client is None:
            factory = self.factory
            if factory is None:
                from curl_cffi.requests import AsyncSession

                factory = AsyncSession
            timeout = self.config.timeout
            if "httpx" in sys.modules:
                import httpx

                if factory is httpx.AsyncClient:
                    timeout = _coerce_httpx_timeout(timeout)
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
            try:
                await getattr(retired, self._close_method)()
            except Exception:
                pass

    async def _after(self):
        if self._after_response is not None:
            await self._after_response()

    async def _prepare_curl(self, options, *, client=None):
        client = client if client is not None else self._connect()
        if not _is_curl_async_session(client):
            return
        from curl_cffi import CurlOpt

        ipv6 = _has_ipv6_connectivity()
        if not ipv6:
            client.curl_options[CurlOpt.IPRESOLVE] = 1
        target = urlsplit(self.origin)
        hostname = target.hostname
        if not hostname or _is_ip_literal(hostname) or _has_proxy_configured(client, options):
            return
        port = target.port or (443 if target.scheme == "https" else 80)
        ips = await _resolve_host_ips(
            hostname,
            port,
            use_ipv6=ipv6,
            timeout=_connect_timeout(options.get("timeout", self.config.timeout)),
        )
        formatted = [f"[{ip}]" if ":" in ip else ip for ip in ips]
        prefix = f"{hostname}:{port}:"
        existing = [
            entry
            for entry in (client.curl_options.get(CurlOpt.RESOLVE) or ())
            if not str(entry).startswith(prefix)
        ]
        existing.append(f"{prefix}{','.join(formatted)}")
        client.curl_options[CurlOpt.RESOLVE] = existing

    def _invalidate_curl_dns(self):
        target = urlsplit(self.origin)
        hostname = target.hostname
        if not hostname:
            return
        port = target.port or (443 if target.scheme == "https" else 80)
        folded = hostname.casefold()
        _DNS_CACHE.remove((folded, port, True))
        _DNS_CACHE.remove((folded, port, False))
        client = self._client_getter() if self._client_getter is not None else self.client
        if _is_curl_async_session(client):
            from curl_cffi import CurlOpt

            prefix = f"{hostname}:{port}:"
            existing = client.curl_options.get(CurlOpt.RESOLVE)
            if existing:
                client.curl_options[CurlOpt.RESOLVE] = [
                    entry for entry in existing if not str(entry).startswith(prefix)
                ]

    async def _reset_owned_client(self, failed_client):
        if self._client_getter is not None or self.client is not failed_client:
            return
        client, self.client = self.client, None
        cid = id(client)
        if self._owned_refs.get(cid, 0) > 0:
            self._retired_clients[cid] = client
            return
        try:
            await getattr(client, self._close_method)()
        except Exception:
            pass

    async def _send(self, method, url, options, *, client=None):
        client = client if client is not None else self._connect()
        if _is_httpx_client(client) and isinstance(options.get("timeout"), tuple):
            options = {**options, "timeout": _coerce_httpx_timeout(options["timeout"])}
        await self._prepare_curl(options, client=client)
        return await client.request(method, url, **{self._redirect_option: False}, **options)

    async def send(self, method, path, *, check_status=True, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        can_retry = method.upper() in _IDEMPOTENT_METHODS
        response = None
        client = None
        for attempt in (0, 1):
            client = self._acquire_client()
            try:
                response = await self._send(method, url, options, client=client)
                break
            except BaseException as exc:
                await self._release_client(client)
                if attempt == 0 and can_retry and _is_curl_timeout(exc):
                    self._invalidate_curl_dns()
                    await self._reset_owned_client(client)
                    continue
                raise
        try:
            try:
                self._check_response(response, check_status)
                self._check_size(response)
                return response
            finally:
                await self._release_client(client)
        finally:
            await self._after()

    @asynccontextmanager
    async def _open_stream(self, method, url, options, *, client=None):
        client = client if client is not None else self._connect()
        if _is_httpx_client(client) and isinstance(options.get("timeout"), tuple):
            options = {**options, "timeout": _coerce_httpx_timeout(options["timeout"])}
        await self._prepare_curl(options, client=client)
        async with client.stream(
            method, url, **{self._redirect_option: False}, **options
        ) as response:
            yield response

    @asynccontextmanager
    async def stream(self, method, path, *, check_status=True, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        can_retry = method.upper() in _IDEMPOTENT_METHODS
        stack = AsyncExitStack()
        client = None
        response = None
        try:
            for attempt in (0, 1):
                client = self._acquire_client()
                try:
                    response = await stack.enter_async_context(
                        self._open_stream(method, url, options, client=client)
                    )
                    self._check_response(response, check_status)
                    break
                except BaseException as exc:
                    try:
                        await stack.aclose()
                    finally:
                        await self._release_client(client)
                    if attempt == 0 and can_retry and _is_curl_timeout(exc):
                        self._invalidate_curl_dns()
                        await self._reset_owned_client(client)
                        continue
                    raise
            try:
                yield _AsyncStreamResponse(
                    response,
                    getattr(response, self._byte_iterator)(),
                    self.config.max_stream_bytes,
                )
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

    async def _send(self, method, url, options, *, client=None):
        client = client if client is not None else self._connect()
        await self._prepare_curl(options, client=client)
        return await getattr(client, method.lower())(url, allow_redirects=False, **options)

    @asynccontextmanager
    async def _open_stream(self, method, url, options, *, client=None):
        client = client if client is not None else self._connect()
        await self._prepare_curl(options, client=client)
        response = await client.request(method, url, stream=True, allow_redirects=False, **options)
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

            factory = self.factory or httpx.Client
            timeout = (
                _coerce_httpx_timeout(self.config.timeout)
                if factory is httpx.Client
                else self.config.timeout
            )
            self.client = factory(timeout=timeout)
        return self.client

    def send(self, method, path, *, check_status=True, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        client = self._connect()
        if _is_httpx_client(client) and isinstance(options.get("timeout"), tuple):
            options = {**options, "timeout": _coerce_httpx_timeout(options["timeout"])}
        response = client.request(method, url, follow_redirects=False, **options)
        self._check_response(response, check_status)
        self._check_size(response)
        return response

    @contextmanager
    def stream(self, method, path, *, check_status=True, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        client = self._connect()
        if _is_httpx_client(client) and isinstance(options.get("timeout"), tuple):
            options = {**options, "timeout": _coerce_httpx_timeout(options["timeout"])}
        with client.stream(method, url, follow_redirects=False, **options) as response:
            self._check_response(response, check_status)
            yield _StreamResponse(response, self.config.max_stream_bytes)

    def close(self):
        client, self.client = self.client, None
        if client is not None:
            client.close()
