"""Resolved file requests; credentials are transient and never archive metadata."""

import asyncio
import ipaddress
import socket
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from .network import _format_resolve_entry, _has_ipv6_connectivity, _is_ip_literal

_MAX_DOWNLOAD_BYTES = 100 * 1024 * 1024


def resolve_network_addresses(address_str: str, allow_private: bool = False) -> tuple[str, ...]:
    normalized = address_str.strip("[]").split("%", 1)[0]
    try:
        addresses = [ipaddress.ip_address(normalized)]
    except ValueError:
        family = socket.AF_UNSPEC if _has_ipv6_connectivity() else socket.AF_INET
        try:
            records = socket.getaddrinfo(address_str, None, family=family, type=socket.SOCK_STREAM)
        except socket.gaierror:
            raise ValueError("Download host could not be resolved") from None
        addresses = list(
            dict.fromkeys(
                ipaddress.ip_address(str(record[4][0]).split("%", 1)[0]) for record in records
            )
        )
        addresses.sort(key=lambda ip: ip.version == 6)

    if not addresses:
        raise ValueError("Download host has no usable network address")
    for ip in addresses:
        if ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified:
            raise ValueError("Prohibited destination network address")
        if not allow_private and not ip.is_global:
            raise ValueError("Prohibited destination network address")
    return tuple(str(ip) for ip in addresses)


@dataclass(frozen=True)
class DownloadSource:
    url: str
    filename: str | None = None
    force_download: bool = False
    headers: dict[str, str] = field(default_factory=dict, repr=False)
    client_factory: Callable | None = field(default=None, repr=False, compare=False)


def cookie_header(values, names):
    """Build a transient header from explicitly allowed account cookies only."""
    selected = []
    if isinstance(values.get("cookies"), dict):
        values = values["cookies"]
    for key in names:
        value = values.get(key)
        if value is None:
            continue
        if not isinstance(value, str) or any(char in value for char in "\r\n;"):
            raise ValueError("Session record is invalid")
        selected.append(f"{key}={value}")
    return "; ".join(selected)


class NativeDownloadClient:
    """Stream bytes with the same HTTP transport used by Gemini and Claude."""

    def __init__(
        self,
        *,
        timeout=None,
        trust_env=False,
        allow_private=False,
    ):
        from curl_cffi.requests import Session

        self.allow_private = allow_private
        connect_timeout = getattr(timeout, "connect", 15) if timeout is not None else 15
        read_timeout = getattr(timeout, "read", 120) if timeout is not None else 120
        self.client = Session(
            impersonate="chrome",
            timeout=(connect_timeout, read_timeout),
            allow_redirects=False,
            trust_env=trust_env,
        )

    @property
    def cookies(self):
        return self.client.cookies

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.client.close()

    @contextmanager
    def stream(self, method, url, *, headers, allow_private=None):
        from curl_cffi.requests.exceptions import RequestException

        permit_private = self.allow_private if allow_private is None else allow_private
        target = urlsplit(url)
        if target.scheme != "https" or not target.hostname or target.username or target.password:
            raise ValueError("Download URL must use HTTPS without credentials")
        pinned_addresses = resolve_network_addresses(target.hostname, allow_private=permit_private)
        if not _is_ip_literal(target.hostname):
            from curl_cffi import CurlOpt

            if not _has_ipv6_connectivity():
                self.client.curl_options[CurlOpt.IPRESOLVE] = 1
            prefix = f"{target.hostname}:{target.port or 443}:"
            self.client.curl_options[CurlOpt.RESOLVE] = [
                _format_resolve_entry(prefix, pinned_addresses)
            ]
        try:
            with self.client.stream(
                method, url, headers=headers, allow_redirects=False
            ) as response:
                yield _NativeDownloadResponse(response)
        except RequestException:
            # Transport errors may contain signed URLs or credentials.
            raise ValueError(
                "File transfer was interrupted; check your connection and try again."
            ) from None


class _NativeDownloadResponse:
    def __init__(self, response):
        self.status_code = response.status_code
        self.headers = response.headers
        self.response = response

    def iter_bytes(self, chunk_size: int = 65536):
        return self.response.iter_content(chunk_size=chunk_size)


async def download_source(
    source: DownloadSource | None, *, max_bytes: int = _MAX_DOWNLOAD_BYTES
) -> bytes:
    from ..errors import ProviderError, UnsupportedOperationError

    if not isinstance(source, DownloadSource):
        raise UnsupportedOperationError(
            "Provider did not resolve this URL to a safe download source"
        )

    def read_source() -> bytes:
        factory = source.client_factory
        client = (
            NativeDownloadClient(allow_private=False)
            if factory is None or factory is NativeDownloadClient
            else factory()
        )
        if not isinstance(client, NativeDownloadClient):
            raise ProviderError("Provider returned an unsupported download transport")
        with (
            client,
            client.stream("GET", source.url, headers=dict(source.headers)) as response,
        ):
            if not 200 <= response.status_code < 300:
                raise ProviderError(f"File transfer failed with HTTP {response.status_code}")
            chunks: list[bytes] = []
            total = 0
            for chunk in response.iter_bytes():
                if not chunk:
                    continue
                total += len(chunk)
                if total > max_bytes:
                    raise ProviderError("Downloaded file exceeds size limit")
                chunks.append(chunk)
            return b"".join(chunks)

    try:
        return await asyncio.to_thread(read_source)
    except ProviderError:
        raise
    except Exception:
        raise ProviderError("File transfer failed") from None
