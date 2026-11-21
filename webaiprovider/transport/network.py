"""Shared DNS resolution, address validation, and curl connection options."""

import ipaddress
import os
import socket
import sys
import time
from collections.abc import Mapping
from urllib.parse import urlsplit

from ..core.cache import BoundedCache
from ..errors import ProviderError


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
_DNS_CACHE: BoundedCache[tuple[str, int, bool], tuple[str, ...]] = BoundedCache(
    max_entries=64, max_bytes=32768, ttl=300.0
)
_IPV6_CACHE_TTL = 180.0
_ipv6_connectivity_cache: tuple[float, bool] | None = None


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


def _extract_and_validate_ips(hostname: str, records) -> tuple[str, ...]:
    addresses = list(dict.fromkeys(record[4][0].split("%", 1)[0] for record in records))
    if not addresses:
        raise ProviderError("Provider host resolution failed")
    addresses.sort(key=lambda addr: ":" in addr)
    return _validate_resolved_ips(hostname, addresses)


def _resolve_host_ips(hostname: str, port: int, *, use_ipv6: bool) -> tuple[str, ...]:
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
    if _is_curl_sync_session(client) or _is_curl_async_session(client):
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


def _prepare_curl_session(
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
    ips = _resolve_host_ips(hostname, port, use_ipv6=ipv6)
    prefix = dns_state.prefix if dns_state is not None else f"{hostname}:{port}:"
    entry = (
        dns_state.resolve_entry(ips)
        if dns_state is not None
        else _format_resolve_entry(prefix, ips)
    )
    _apply_curl_resolve(client, prefix, entry)
