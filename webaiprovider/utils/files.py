"""Resolved file requests; credentials are transient and never archive metadata."""

import asyncio
import ipaddress
import socket
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from urllib.parse import urlsplit


def validate_network_address(address_str: str, allow_private: bool = False) -> str:
    try:
        addresses = [ipaddress.ip_address(address_str)]
    except ValueError:
        try:
            records = socket.getaddrinfo(address_str, None, type=socket.SOCK_STREAM)
        except socket.gaierror:
            raise ValueError("Download host could not be resolved") from None
        addresses = list(
            dict.fromkeys(ipaddress.ip_address(record[4][0].split("%", 1)[0]) for record in records)
        )

    if not addresses:
        raise ValueError("Download host has no usable network address")
    for ip in addresses:
        if ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified:
            raise ValueError("Prohibited destination network address")
        if not allow_private and not ip.is_global:
            raise ValueError("Prohibited destination network address")
    return str(addresses[0])


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
    def stream(self, method, url, *, headers, resolved_address=None, allow_private=None):
        from curl_cffi.requests.exceptions import RequestException

        permit_private = self.allow_private if allow_private is None else allow_private
        target = urlsplit(url)
        if not target.hostname:
            raise ValueError("Download URL has no valid hostname")
        dns_address = validate_network_address(target.hostname, allow_private=permit_private)
        pinned_address = (
            validate_network_address(resolved_address, allow_private=permit_private)
            if resolved_address
            else dns_address
        )
        try:
            ipaddress.ip_address(target.hostname.split("%", 1)[0])
            is_ip_literal = True
        except ValueError:
            is_ip_literal = False
        if not is_ip_literal:
            from curl_cffi import CurlOpt

            port = target.port or (443 if target.scheme == "https" else 80)
            address = f"[{pinned_address}]" if ":" in pinned_address else pinned_address
            self.client.curl_options[CurlOpt.RESOLVE] = [f"{target.hostname}:{port}:{address}"]
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


async def download_source(source: DownloadSource | None) -> bytes:
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
            return b"".join(response.iter_bytes())

    try:
        return await asyncio.to_thread(read_source)
    except ProviderError:
        raise
    except Exception as exc:
        raise ProviderError("File transfer failed") from exc
