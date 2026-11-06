"""Resolved file requests; credentials are transient and never archive metadata."""

import ipaddress
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Callable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_SENSITIVE_PARAM_SUBSTRINGS = (
    "token",
    "key",
    "auth",
    "secret",
    "sig",
    "credential",
    "session",
)


def redact_sensitive_url(url: str) -> str:
    try:
        parts = urlsplit(url)
        if not parts.scheme:
            return url
        netloc = parts.netloc
        if "@" in netloc:
            _, _, host_port = netloc.rpartition("@")
            netloc = f"***:***@{host_port}"
        if parts.query:
            pairs = parse_qsl(parts.query, keep_blank_values=True)
            redacted_pairs = []
            for k, v in pairs:
                k_lower = k.lower()
                if any(s in k_lower for s in _SENSITIVE_PARAM_SUBSTRINGS):
                    redacted_pairs.append((k, "[REDACTED]"))
                else:
                    redacted_pairs.append((k, v))
            query = urlencode(redacted_pairs, safe="[]")
        else:
            query = parts.query
        return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))
    except Exception:
        return "[REDACTED_URL]"


def validate_network_address(address_str: str, allow_private: bool = True) -> None:
    try:
        ip = ipaddress.ip_address(address_str)
    except ValueError:
        return
    if ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified:
        raise ValueError(f"Prohibited destination network address: {address_str}")
    if not allow_private and (ip.is_private or ip.is_loopback):
        raise ValueError(f"Prohibited destination network address: {address_str}")


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
            raise ValueError("Oturum kaydı geçersiz")
        selected.append(f"{key}={value}")
    return "; ".join(selected)


class NativeDownloadClient:
    """Stream bytes with the same HTTP transport used by Gemini and Claude."""

    def __init__(
        self,
        *,
        timeout=None,
        follow_redirects=False,
        trust_env=False,
        allow_private=True,
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
        validate_network_address(target.hostname, allow_private=permit_private)
        if resolved_address:
            validate_network_address(resolved_address, allow_private=permit_private)
            from curl_cffi import CurlOpt

            port = target.port or (443 if target.scheme == "https" else 80)
            address = f"[{resolved_address}]" if ":" in resolved_address else resolved_address
            self.client.curl_options[CurlOpt.RESOLVE] = [f"{target.hostname}:{port}:{address}"]
        try:
            with self.client.stream(
                method, url, headers=headers, allow_redirects=False
            ) as response:
                yield _NativeDownloadResponse(response)
        except RequestException:
            # Transport errors may contain signed URLs or credentials.
            raise ValueError(
                "Dosya aktarımı kesildi; bağlantıyı kontrol edip yeniden deneyin"
            ) from None


class _NativeDownloadResponse:
    def __init__(self, response):
        self.status_code = response.status_code
        self.headers = response.headers
        self.response = response

    def iter_bytes(self, chunk_size: int = 65536):
        return self.response.iter_content(chunk_size=chunk_size)
