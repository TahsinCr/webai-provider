"""Small synchronous Chrome DevTools client for local provider sessions."""

from __future__ import annotations

import contextlib
import ipaddress
import json
import time
import urllib.error
import urllib.request
from collections import deque
from urllib.parse import urlsplit

from curl_cffi import CurlECode, CurlError, CurlOpt, CurlWsFlag, Session
from curl_cffi.requests.websockets import WebSocketTimeout

from .filters import ALLOWED_PROVIDER_ORIGINS

_MAX_METADATA_BYTES = 2 * 1024 * 1024
_MAX_MESSAGE_BYTES = 2 * 1024 * 1024
_MAX_EVENT_QUEUE_BYTES = 8 * 1024 * 1024
_MAX_EVENT_QUEUE_ITEMS = 1024
_DEFAULT_TIMEOUT = 5.0


class DevToolsError(RuntimeError):
    """A local browser debugging endpoint or command failed."""


def _is_loopback(host: str | None) -> bool:
    if not host:
        return False
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _authority(parts) -> str:
    host = parts.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    return f"{host}:{parts.port}" if parts.port is not None else host


def _local_http_base(value: str) -> str:
    try:
        parts = urlsplit(value)
        port = parts.port
    except (AttributeError, TypeError, ValueError) as error:
        raise DevToolsError("Invalid local browser debugging address") from error
    if (
        parts.scheme != "http"
        or not _is_loopback(parts.hostname)
        or parts.username
        or parts.password
        or parts.path not in ("", "/")
        or parts.query
        or parts.fragment
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise DevToolsError("A local browser debugging address is required")
    return f"http://{_authority(parts)}"


def _provider_origin(value: str) -> str:
    try:
        parts = urlsplit(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise DevToolsError("Invalid provider address") from error
    origin = _page_origin(value)
    if origin is None:
        raise DevToolsError("Unsupported provider address")
    if (
        parts.path not in ("", "/")
        or parts.query
        or parts.fragment
        or parts.username
        or parts.password
    ):
        raise DevToolsError("Unsupported provider address")
    return origin


def _page_origin(value: str) -> str | None:
    try:
        parts = urlsplit(value)
        port = parts.port
    except (AttributeError, TypeError, ValueError):
        return None
    origin = f"{parts.scheme}://{parts.hostname}" if parts.hostname else ""
    if port is not None:
        origin += f":{port}"
    if (
        parts.scheme != "https"
        or origin not in ALLOWED_PROVIDER_ORIGINS
        or parts.username
        or parts.password
    ):
        return None
    return origin


def _debugger_websocket(value: str) -> tuple[str, str]:
    try:
        parts = urlsplit(value)
        port = parts.port
    except (AttributeError, TypeError, ValueError) as error:
        raise DevToolsError("Invalid browser debugging socket") from error
    if (
        parts.scheme != "ws"
        or not _is_loopback(parts.hostname)
        or parts.username
        or parts.password
        or not parts.path.startswith("/devtools/")
        or parts.query
        or parts.fragment
        or port is None
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise DevToolsError("A local browser debugging socket is required")
    origin = f"http://{_authority(parts)}"
    return value, origin


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_CDP_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirectHandler())


def _get_json(base: str, path: str, timeout: float = _DEFAULT_TIMEOUT):
    try:
        with _CDP_OPENER.open(f"{base}{path}", timeout=timeout) as response:
            status = getattr(response, "status", 200)
            if status < 200 or status >= 300:
                raise DevToolsError(f"Connected browser debugging endpoint returned HTTP {status}")
            body = bytearray()
            while True:
                chunk = response.read(min(65536, _MAX_METADATA_BYTES - len(body) + 1))
                if not chunk:
                    break
                if len(body) + len(chunk) > _MAX_METADATA_BYTES:
                    raise DevToolsError("Browser debugging metadata exceeds the size limit")
                body.extend(chunk)
    except urllib.error.HTTPError as error:
        try:
            code = error.code
        finally:
            error.close()
        raise DevToolsError(f"Connected browser debugging endpoint returned HTTP {code}") from error
    except (urllib.error.URLError, OSError) as error:
        raise DevToolsError("Connected browser debugging endpoint is unavailable") from error
    try:
        return json.loads(body)
    except (json.JSONDecodeError, ValueError) as error:
        raise DevToolsError("Browser returned invalid debugging metadata") from error


def is_devtools_available(cdp_url: str, timeout: float = 1.0) -> bool:
    """Check for a responsive local DevTools endpoint and a valid socket URL."""
    try:
        base = _local_http_base(cdp_url)
        version = _get_json(base, "/json/version", timeout)
        socket_url = version.get("webSocketDebuggerUrl") if isinstance(version, dict) else None
        if not isinstance(socket_url, str):
            return False
        _debugger_websocket(socket_url)
    except DevToolsError:
        return False
    return True


def _find_page(pages, origin: str) -> dict | None:
    if not isinstance(pages, list):
        raise DevToolsError("Browser returned an invalid page list")
    for page in pages:
        if not isinstance(page, dict) or page.get("type") != "page":
            continue
        page_origin = _page_origin(page.get("url", ""))
        socket_url = page.get("webSocketDebuggerUrl")
        if page_origin == origin and isinstance(socket_url, str):
            _debugger_websocket(socket_url)
            return page
    return None


def _page_for_origin(base: str, origin: str) -> dict:
    page = _find_page(_get_json(base, "/json/list"), origin)
    if page is not None:
        return page
    raise DevToolsError("Open the provider in the connected browser first")


class DevToolsConnection:
    """One bounded, single-owner CDP WebSocket connection."""

    def __init__(self, socket_url: str, timeout: float = _DEFAULT_TIMEOUT):
        socket_url, origin = _debugger_websocket(socket_url)
        self._timeout = timeout
        self._next_id = 0
        self._events: deque[tuple[dict, int]] = deque()
        self._event_bytes = 0
        self._session = Session(trust_env=False, curl_options={CurlOpt.PROXY: b""})
        try:
            self._socket = self._session.ws_connect(
                socket_url,
                timeout=timeout,
                headers={"Origin": origin},
                allow_redirects=False,
                max_message_size=_MAX_MESSAGE_BYTES,
            )
        except (CurlError, OSError, ValueError) as error:
            self.close()
            raise DevToolsError("Could not connect to the local browser debugger") from error

    def _receive(self, timeout: float) -> tuple[dict, int]:
        try:
            message, flags = self._socket.recv(timeout=max(timeout, 0.001))
        except WebSocketTimeout as error:
            raise DevToolsError("Browser debugging request timed out") from error
        except (CurlError, OSError) as error:
            if isinstance(error, CurlError) and error.code == CurlECode.TOO_LARGE:
                raise DevToolsError("Browser debugging message exceeds the size limit") from error
            raise DevToolsError("Browser debugging connection was interrupted") from error
        if flags & CurlWsFlag.CLOSE:
            raise DevToolsError("Browser debugging connection was interrupted")
        try:
            value = json.loads(message)
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise DevToolsError("Browser returned an invalid debugging message") from error
        if not isinstance(value, dict):
            raise DevToolsError("Browser returned an invalid debugging message")
        return value, len(message)

    def _queue_event(self, message: dict, size: int) -> None:
        if (
            len(self._events) >= _MAX_EVENT_QUEUE_ITEMS
            or self._event_bytes + size > _MAX_EVENT_QUEUE_BYTES
        ):
            raise DevToolsError("Browser debugging event buffer exceeded its size limit")
        self._events.append((message, size))
        self._event_bytes += size

    def _dequeue_event(self) -> dict:
        message, size = self._events.popleft()
        self._event_bytes -= size
        return message

    def call(self, method: str, params: dict | None = None, timeout: float | None = None) -> dict:
        self._next_id += 1
        command_id = self._next_id
        duration = self._timeout if timeout is None else timeout
        deadline = time.monotonic() + duration
        try:
            self._socket.send_str(
                json.dumps(
                    {"id": command_id, "method": method, "params": params or {}},
                    separators=(",", ":"),
                ),
                timeout=duration,
            )
        except (CurlError, OSError, ValueError) as error:
            raise DevToolsError("Could not send a browser debugging command") from error
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DevToolsError(f"Browser debugging command timed out: {method}")
            message, size = self._receive(remaining)
            if message.get("id") == command_id:
                if "error" in message:
                    raise DevToolsError(f"Browser debugging command failed: {method}")
                result = message.get("result", {})
                if not isinstance(result, dict):
                    raise DevToolsError("Browser returned an invalid command result")
                return result
            self._queue_event(message, size)

    def next_event(self, timeout: float) -> dict:
        deadline = time.monotonic() + timeout
        while True:
            if self._events:
                message = self._dequeue_event()
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise DevToolsError("Browser debugging request timed out")
                message, _ = self._receive(remaining)
            if isinstance(message.get("method"), str):
                return message

    def close(self) -> None:
        with contextlib.suppress(CurlError, OSError):
            self._session.close()

    def __enter__(self) -> DevToolsConnection:
        return self

    def __exit__(self, *_exc) -> None:
        self.close()


def read_provider_cookies(cdp_url: str, provider_url: str) -> list[dict]:
    """Read cookies scoped to an already-open provider tab; never persist them here."""
    base = _local_http_base(cdp_url)
    origin = _provider_origin(provider_url)
    page = _page_for_origin(base, origin)
    with DevToolsConnection(page["webSocketDebuggerUrl"]) as connection:
        result = connection.call("Network.getCookies", {"urls": [origin]})
    cookies = result.get("cookies")
    if not isinstance(cookies, list) or any(not isinstance(cookie, dict) for cookie in cookies):
        raise DevToolsError("Browser returned an invalid cookie list")
    return cookies
