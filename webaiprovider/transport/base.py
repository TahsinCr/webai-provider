"""Public transport contracts and origin-scoped request policy."""

from __future__ import annotations

import asyncio
import math
import re
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import unquote, urlsplit

from ..errors import AccountPaused, ProviderError, RateLimited, RemoteNotFound
from .retry import parse_retry_after

_CTRL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f]")


class _ResponseHead(Protocol):
    @property
    def status_code(self) -> int: ...

    @property
    def headers(self) -> Mapping[str, str]: ...


class TransportResponse(_ResponseHead, Protocol):
    @property
    def content(self) -> bytes: ...

    @property
    def text(self) -> str: ...

    def json(self) -> Any: ...


class StreamResponse(_ResponseHead, Protocol):
    def iter_bytes(self) -> Iterator[bytes]: ...
    def close(self) -> None: ...


class AsyncStreamResponse(_ResponseHead, Protocol):
    def aiter_bytes(self) -> AsyncIterator[bytes]: ...


def _is_positive_finite(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


@dataclass(frozen=True)
class TransportConfig:
    timeout: float | tuple[float, float] = (10.0, 50.0)
    max_response_bytes: int = 16 * 1024 * 1024
    max_stream_bytes: int | None = None

    def __post_init__(self):
        if isinstance(self.timeout, tuple):
            valid_timeout = len(self.timeout) == 2 and all(
                _is_positive_finite(part) for part in self.timeout
            )
        else:
            valid_timeout = _is_positive_finite(self.timeout)
        if not valid_timeout or self.max_response_bytes <= 0:
            raise ValueError("Transport timeout and response limit must be positive")
        if self.max_stream_bytes is not None and self.max_stream_bytes <= 0:
            raise ValueError("Transport stream limit must be positive")


class _AsyncStreamResponse:
    def __init__(self, response, chunks, limit=None):
        self.status_code: int = response.status_code
        self.headers: Mapping[str, str] = response.headers
        self._chunks = chunks
        self._limit = limit

    async def aiter_bytes(self) -> AsyncIterator[bytes]:
        size = 0
        async for chunk in self._chunks:
            size += len(chunk)
            if self._limit is not None and size > self._limit:
                raise ProviderError("Transport stream exceeds the size limit")
            yield chunk


class _StreamResponse:
    def __init__(self, response, limit=None):
        self.status_code: int = response.status_code
        self.headers: Mapping[str, str] = response.headers
        self._response = response
        self._limit = limit

    def iter_bytes(self) -> Iterator[bytes]:
        size = 0
        for chunk in self._response.iter_bytes():
            size += len(chunk)
            if self._limit is not None and size > self._limit:
                raise ProviderError("Transport stream exceeds the size limit")
            yield chunk

    def close(self):
        self._response.close()


class _TransportPolicy:
    def __init__(
        self,
        origin: str,
        *,
        config: TransportConfig | None = None,
        headers: Mapping[str, str] | Callable[[str, str], Mapping[str, str]] | None = None,
    ):
        target = urlsplit(origin)
        if (
            target.scheme not in ("http", "https")
            or not target.hostname
            or target.username is not None
            or target.password is not None
            or target.path not in ("", "/")
            or target.query
            or target.fragment
            or "\\" in origin
            or _CTRL_CHAR_RE.search(origin) is not None
        ):
            raise ValueError("Transport origin must be an HTTP origin without credentials or path")
        self._origin = origin.rstrip("/")
        self._config = config or TransportConfig()
        self._headers = headers
        self._rate_limited_until = 0.0

    @property
    def origin(self) -> str:
        return self._origin

    @property
    def config(self) -> TransportConfig:
        return self._config

    def url(self, path: str) -> str:
        parsed = urlsplit(path)
        decoded = unquote(parsed.path)
        if (
            parsed.scheme
            or parsed.netloc
            or not path.startswith("/")
            or path.startswith("//")
            or ".." in decoded
            or "\\" in decoded
            or parsed.fragment
            or _CTRL_CHAR_RE.search(path) is not None
        ):
            raise ValueError("Transport request must remain on its origin")
        return self.origin + path

    def _options(self, options: dict[str, Any], method: str, path: str) -> dict[str, Any]:
        if options.pop("stream", False):
            raise ValueError("Use transport.stream() to own a streaming response")
        if options.pop("allow_redirects", False) or options.pop("follow_redirects", False):
            raise ValueError("Authenticated transport redirects are disabled")
        defaults = {}
        if options.pop("inherit_headers", True) and self._headers is not None:
            defaults = self._headers(method, path) if callable(self._headers) else self._headers
        if defaults or options.get("headers") is not None:
            options["headers"] = {
                str(key).lower(): value
                for headers in (defaults, options.get("headers") or {})
                for key, value in headers.items()
            }
        host = (options.get("headers") or {}).get("host")
        if host is not None and host.casefold() != urlsplit(self.origin).netloc.casefold():
            raise ValueError("Transport Host header must match its origin")
        options.setdefault("timeout", self.config.timeout)
        remaining = self._rate_limited_until - time.monotonic()
        if remaining > 0:
            raise RateLimited(remaining)
        return options

    @staticmethod
    def check(response):
        code = response.status_code
        if code == 401 or code in (301, 302, 303, 307, 308):
            raise AccountPaused("Provider session expired or redirected; verify the account")
        if code == 403:
            raise ProviderError("Provider denied access (HTTP 403); the request was not retried")
        if code == 404:
            raise RemoteNotFound("Remote record was not found")
        if code == 429:
            raise RateLimited(
                parse_retry_after(response.headers.get("retry-after", ""), now=time.time())
            )
        if not 200 <= code < 300:
            raise ProviderError(f"Provider request failed (HTTP {code})")

    def record_rate_limit(self, retry_after: float | None) -> None:
        """Record a positive finite rate-limit deadline on this transport."""
        if (
            isinstance(retry_after, (int, float))
            and not isinstance(retry_after, bool)
            and math.isfinite(retry_after)
            and retry_after > 0
        ):
            deadline = time.monotonic() + float(retry_after)
            if deadline > self._rate_limited_until:
                self._rate_limited_until = deadline

    def _check_response(self, response, check_status):
        if check_status:
            try:
                self.check(response)
            except RateLimited as error:
                self.record_rate_limit(error.retry_after)
                raise

    def _check_size(self, response):
        if len(response.content) > self.config.max_response_bytes:
            raise ProviderError("Provider response exceeds the size limit")


class AsyncTransport(_TransportPolicy, ABC):
    """Extension contract used by async providers and custom transports."""

    @abstractmethod
    async def send(self, method: str, path: str, **options) -> TransportResponse: ...

    @abstractmethod
    def stream(
        self, method: str, path: str, **options
    ) -> AbstractAsyncContextManager[AsyncStreamResponse]: ...

    async def request(self, method: str, path: str, **options):
        response = await self.send(method, path, **options)
        if response.status_code == 204 or not response.content:
            return None
        try:
            if len(response.content) >= 256 * 1024:
                return await asyncio.to_thread(response.json)
            return response.json()
        except ValueError:
            raise ProviderError("Provider response format has changed") from None

    @abstractmethod
    async def close(self): ...


class Transport(_TransportPolicy, ABC):
    """Blocking transport contract with the same request semantics."""

    @abstractmethod
    def send(self, method: str, path: str, **options) -> TransportResponse: ...

    @abstractmethod
    def stream(
        self, method: str, path: str, **options
    ) -> AbstractContextManager[StreamResponse]: ...

    def request(self, method: str, path: str, **options):
        response = self.send(method, path, **options)
        if response.status_code == 204 or not response.content:
            return None
        try:
            return response.json()
        except ValueError:
            raise ProviderError("Provider response format has changed") from None

    @abstractmethod
    def close(self): ...
