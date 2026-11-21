"""Blocking HTTP transport."""

from collections import Counter
from contextlib import (
    ExitStack,
    contextmanager,
    suppress,
)
from io import BytesIO
from threading import Condition, RLock, get_ident

from ..errors import ProviderError
from .base import (
    Transport,
    _BufferedResponse,
    _StreamResponse,
)
from .network import (
    _IDEMPOTENT_METHODS,
    _is_curl_sync_session,
    _is_curl_timeout,
    _OriginDNS,
    _prepare_curl_session_sync,
)


class HTTPTransport(Transport):
    """Blocking HTTP transport backed by ``curl_cffi.requests.Session`` by default."""

    def __init__(
        self, origin, *, client_factory=None, client_getter=None, config=None, headers=None
    ):
        super().__init__(origin, config=config, headers=headers)
        self.factory = client_factory
        self._client_getter = client_getter
        self.client = None
        self._client_lock = RLock()
        self._idle = Condition(self._client_lock)
        self._callers: Counter[int] = Counter()
        self._closing = False
        self._dns = _OriginDNS(self.origin)

    def _connect(self):
        if self._client_getter is not None:
            return self._client_getter()
        with self._client_lock:
            if self.client is None:
                factory = self.factory
                if factory is None:
                    from curl_cffi.requests import Session as CurlSession

                    self.client = CurlSession(timeout=self.config.timeout, trust_env=False)
                else:
                    self.client = factory(timeout=self.config.timeout)
            return self.client

    @contextmanager
    def _activity(self):
        caller = get_ident()
        with self._idle:
            if not self._callers[caller]:
                self._idle.wait_for(lambda: not self._closing)
            self._callers[caller] += 1
        try:
            yield
        finally:
            with self._idle:
                self._callers[caller] -= 1
                if not self._callers[caller]:
                    del self._callers[caller]
                if not self._callers and self._closing:
                    self._idle.notify_all()

    def _reset_owned_client(self, failed_client):
        with self._client_lock:
            if (
                self._client_getter is not None
                or self.client is not failed_client
                or self._callers.total() > 1
            ):
                return
            client, self.client = self.client, None
        with suppress(Exception):
            client.close()

    def _buffer_response(self, response) -> _BufferedResponse:
        self._check_declared_size(response)
        iterator = getattr(response, "iter_content", None) or getattr(response, "iter_bytes", None)
        if not callable(iterator):
            raise ProviderError("Provider response cannot be streamed")
        first = b""
        buffer = None
        limit = self.config.max_response_bytes
        total = 0
        for chunk in iterator():
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

    def _send_curl(self, method, url, options, *, check_status):
        from curl_cffi import CurlError
        from curl_cffi.curl import CURL_WRITEFUNC_ERROR

        limit = self.config.max_response_bytes
        for attempt in (0, 1):
            client = self._connect()
            buffer = BytesIO()
            overflow = False

            def collect(chunk: bytes) -> int:
                nonlocal overflow
                if buffer.tell() + len(chunk) > limit:
                    overflow = True
                    return CURL_WRITEFUNC_ERROR
                return buffer.write(chunk)

            _prepare_curl_session_sync(
                client, self._dns.host, self._dns.port, options, dns_state=self._dns
            )
            try:
                response = client.request(
                    method, url, allow_redirects=False, content_callback=collect, **options
                )
            except CurlError as error:
                response = getattr(error, "response", None)
                received_headers = bool(getattr(response, "status_code", 0))
                if received_headers:
                    self._check_response(response, check_status)
                if overflow:
                    raise ProviderError("Provider response exceeds the size limit") from None
                if (
                    attempt == 0
                    and method.upper() in _IDEMPOTENT_METHODS
                    and not received_headers
                    and not buffer.tell()
                    and _is_curl_timeout(error)
                ):
                    self._dns.invalidate(client)
                    self._reset_owned_client(client)
                    continue
                raise
            self._check_response(response, check_status)
            self._check_declared_size(response)
            return _BufferedResponse(response, buffer.getvalue())
        raise ProviderError("Provider request could not be completed")

    def send(self, method, path, *, check_status=True, **options):
        with self._activity():
            return self._send(method, path, check_status=check_status, **options)

    def _send(self, method, path, *, check_status, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        if self.factory is None and self._client_getter is None:
            if options.pop("content_callback", None) is not None:
                raise ValueError("Use transport.stream() to process response chunks")
            return self._send_curl(method, url, options, check_status=check_status)
        can_retry = method.upper() in _IDEMPOTENT_METHODS
        for attempt in (0, 1):
            client = self._connect()
            stream = getattr(client, "stream", None)
            with ExitStack() as stack:
                try:
                    _prepare_curl_session_sync(
                        client, self._dns.host, self._dns.port, options, dns_state=self._dns
                    )
                    if not callable(stream):
                        response = client.request(method, url, allow_redirects=False, **options)
                        self._check_response(response, check_status)
                        self._check_size(response)
                        return response
                    response = stack.enter_context(
                        stream(method, url, allow_redirects=False, **options)
                    )
                    self._check_response(response, check_status)
                except BaseException as exc:
                    stack.close()
                    if attempt == 0 and can_retry and _is_curl_timeout(exc):
                        self._dns.invalidate(client)
                        self._reset_owned_client(client)
                        continue
                    raise
                return self._buffer_response(response)
        raise ProviderError("Provider request could not be completed")

    @contextmanager
    def stream(self, method, path, *, check_status=True, _message=False, **options):
        if _message:
            options["_message"] = True
        with (
            self._activity(),
            self._stream(method, path, check_status=check_status, **options) as stream,
        ):
            yield stream

    @contextmanager
    def _stream(self, method, path, *, check_status, _message=False, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        can_retry = method.upper() in _IDEMPOTENT_METHODS
        native = self.factory is None and self._client_getter is None

        with ExitStack() as stack:
            response = None
            for attempt in (0, 1):
                client = self._connect()
                try:
                    _prepare_curl_session_sync(
                        client, self._dns.host, self._dns.port, options, dns_state=self._dns
                    )
                    defer_head = _message and _is_curl_sync_session(client)
                    if native or defer_head:
                        from .curl_stream import open_blocking_curl_stream

                        if options.pop("content_callback", None) is not None:
                            raise ValueError("Consume data with the returned stream iterator")
                        stream = open_blocking_curl_stream(
                            client, method, url, options, defer_head=defer_head
                        )
                    else:
                        stream = client.stream(method, url, allow_redirects=False, **options)
                    response = stack.enter_context(stream)
                    if not defer_head:
                        self._check_response(response, check_status)
                    break
                except BaseException as exc:
                    stack.close()
                    if (
                        attempt == 0
                        and can_retry
                        and not getattr(getattr(exc, "response", None), "status_code", 0)
                        and _is_curl_timeout(exc)
                    ):
                        self._dns.invalidate(client)
                        self._reset_owned_client(client)
                        continue
                    raise
            yield _StreamResponse(
                response,
                self.config.max_stream_bytes,
                check_head=(lambda head: self._check_response(head, check_status))
                if defer_head
                else None,
            )

    def close(self):
        with self._idle:
            if self._callers[get_ident()]:
                raise ProviderError("Cannot close the transport from an active HTTP call")
            self._idle.wait_for(lambda: not self._closing)
            self._closing = True
            try:
                self._idle.wait_for(lambda: not self._callers)
                self._close_client()
            finally:
                self._closing = False
                self._idle.notify_all()

    def _close_client(self):
        client, self.client = self.client, None
        if client is not None:
            client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _message_stream(transport, method, path, **options):
    if isinstance(transport, HTTPTransport):
        return transport.stream(method, path, _message=True, **options)
    return transport.stream(method, path, **options)


def _wait_for_stream_head(response):
    wait = getattr(response, "_wait_for_head", None)
    if wait is not None:
        yield from wait()
