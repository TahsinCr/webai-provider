"""Blocking HTTP transport."""

from collections import Counter
from contextlib import (
    ExitStack,
    contextmanager,
    suppress,
)
from io import BytesIO
from threading import Condition, RLock, get_ident

from ..core.timing import current_send_timing
from ..errors import WebAIProviderError
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
    _prepare_curl_session,
    create_curl_share,
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
        self._multi_pool = None
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

                    self.client = CurlSession(
                        timeout=self.config.timeout,
                        trust_env=False,
                        curl_share=create_curl_share(),
                    )
                else:
                    self.client = factory(timeout=self.config.timeout)
            return self.client

    def _ensure_multi_pool(self):
        pool = self._multi_pool
        if pool is not None:
            return pool
        with self._client_lock:
            if self._multi_pool is None:
                from .curl_stream import _MultiPool

                self._multi_pool = _MultiPool()
            return self._multi_pool

    def _invalidate_dns(self, client) -> None:
        self._dns.invalidate(client)
        pool = self._multi_pool
        if pool is not None:
            pool.clear()

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
            raise WebAIProviderError("Provider response cannot be streamed")
        first = b""
        buffer = None
        limit = self.config.max_response_bytes
        total = 0
        for chunk in iterator():
            total += len(chunk)
            if total > limit:
                raise WebAIProviderError("Provider response exceeds the size limit")
            if chunk:
                if not first:
                    first = bytes(chunk)
                    continue
                if buffer is None:
                    buffer = BytesIO()
                    buffer.write(first)
                buffer.write(bytes(chunk))
        return _BufferedResponse(response, first if buffer is None else buffer.getvalue())

    def _send_curl(self, method, url, options, *, check_status, client=None):
        from curl_cffi import CurlError
        from curl_cffi.curl import CURL_WRITEFUNC_ERROR

        limit = self.config.max_response_bytes
        for attempt in (0, 1):
            current = client if attempt == 0 and client is not None else self._connect()
            buffer = BytesIO()
            overflow = False

            def collect(chunk: bytes) -> int:
                nonlocal overflow
                if buffer.tell() + len(chunk) > limit:
                    overflow = True
                    return CURL_WRITEFUNC_ERROR
                return buffer.write(chunk)

            _prepare_curl_session(
                current, self._dns.host, self._dns.port, options, dns_state=self._dns
            )
            try:
                response = current.request(
                    method, url, allow_redirects=False, content_callback=collect, **options
                )
            except CurlError as error:
                response = getattr(error, "response", None)
                received_headers = bool(getattr(response, "status_code", 0))
                if received_headers:
                    self._check_response(response, check_status)
                if overflow:
                    raise WebAIProviderError("Provider response exceeds the size limit") from None
                if not received_headers and not buffer.tell() and _is_curl_timeout(error):
                    self._invalidate_dns(current)
                    if attempt == 0 and method.upper() in _IDEMPOTENT_METHODS:
                        self._reset_owned_client(current)
                        continue
                raise
            self._check_response(response, check_status)
            self._check_declared_size(response)
            body = buffer.getvalue()
            if not body and getattr(response, "content", b""):
                self._check_size(response)
                return response
            return _BufferedResponse(response, body)
        raise WebAIProviderError("Provider request could not be completed")

    def send(self, method, path, *, check_status=True, **options):
        with self._activity():
            return self._send(method, path, check_status=check_status, **options)

    def _send(self, method, path, *, check_status, **options):
        url = self.url(path)
        options = self._options(options, method, path)
        client = self._connect()
        if _is_curl_sync_session(client):
            if options.pop("content_callback", None) is not None:
                raise ValueError("Use transport.stream() to process response chunks")
            return self._send_curl(method, url, options, check_status=check_status, client=client)
        can_retry = method.upper() in _IDEMPOTENT_METHODS
        for attempt in (0, 1):
            if attempt > 0:
                client = self._connect()
            stream = getattr(client, "stream", None)
            with ExitStack() as stack:
                try:
                    _prepare_curl_session(
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
                    if not getattr(
                        getattr(exc, "response", None), "status_code", 0
                    ) and _is_curl_timeout(exc):
                        self._invalidate_dns(client)
                        if attempt == 0 and can_retry:
                            self._reset_owned_client(client)
                            continue
                    raise
                return self._buffer_response(response)
        raise WebAIProviderError("Provider request could not be completed")

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
        timing = current_send_timing()

        with ExitStack() as stack:
            response = None
            defer_head = False
            for attempt in (0, 1):
                client = self._connect()
                try:
                    _prepare_curl_session(
                        client, self._dns.host, self._dns.port, options, dns_state=self._dns
                    )
                    is_curl = _is_curl_sync_session(client)
                    defer_head = _message and is_curl
                    total_timeout = options.pop("total_timeout", None)
                    if is_curl:
                        from .curl_stream import open_blocking_curl_stream

                        if options.pop("content_callback", None) is not None:
                            raise ValueError("Consume data with the returned stream iterator")
                        stream = open_blocking_curl_stream(
                            client,
                            method,
                            url,
                            options,
                            defer_head=defer_head,
                            multi_pool=self._ensure_multi_pool(),
                            timing=timing,
                            total_timeout=total_timeout,
                        )
                    else:
                        stream = client.stream(method, url, allow_redirects=False, **options)
                    response = stack.enter_context(stream)
                    if not defer_head:
                        self._check_response(response, check_status)
                    break
                except BaseException as exc:
                    stack.close()
                    if not getattr(
                        getattr(exc, "response", None), "status_code", 0
                    ) and _is_curl_timeout(exc):
                        self._invalidate_dns(client)
                        if attempt == 0 and can_retry:
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
                raise WebAIProviderError("Cannot close the transport from an active HTTP call")
            self._idle.wait_for(lambda: not self._closing)
            self._closing = True
            try:
                self._idle.wait_for(lambda: not self._callers)
                self._close_client()
            finally:
                self._closing = False
                self._idle.notify_all()

    def _close_client(self):
        pool, self._multi_pool = self._multi_pool, None
        if pool is not None:
            pool.close()
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
