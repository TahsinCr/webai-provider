"""Pull a bounded curl response without a Python transfer worker."""

from contextlib import closing, contextmanager
from math import ceil

from curl_cffi import CurlError, CurlOpt
from curl_cffi._wrapper import ffi, lib
from curl_cffi.curl import CURL_WRITEFUNC_PAUSE
from curl_cffi.requests.exceptions import code2error
from curl_cffi.requests.utils import set_curl_options


def _check_multi(code: int) -> None:
    if code:
        raise CurlError(ffi.string(lib.curl_multi_strerror(code)).decode())


class _CurlStream:
    def __init__(self, curl, multi):
        self._curl = curl
        self._multi = multi
        self._running = ffi.new("int *")
        self._chunk = None
        self._paused = False
        self._closed = False
        self._done = False
        self._error = 0
        self.response = None

    @property
    def status_code(self):
        return self.response.status_code

    @property
    def headers(self):
        return self.response.headers

    def receive(self, chunk: bytes) -> int:
        if self._chunk is not None:
            self._paused = True
            return CURL_WRITEFUNC_PAUSE
        self._chunk = chunk
        return len(chunk)

    def advance(self) -> None:
        while self._chunk is None and not self._done and not self._closed:
            if self._paused:
                self._paused = False
                self._curl.pause(0)
                self._raise_callback_error()
                if self._chunk is not None:
                    return
            _check_multi(lib.curl_multi_perform(self._multi, self._running))
            self._raise_callback_error()
            if not self._running[0]:
                self._done = True
                message = lib.curl_multi_info_read(self._multi, self._running)
                if message != ffi.NULL:
                    self._error = message.data.result
            elif self._chunk is None:
                _check_multi(lib.curl_multi_poll(self._multi, ffi.NULL, 0, 1000, ffi.NULL))

    def _raise_callback_error(self) -> None:
        error = self._curl._get_callback_exception()
        if error is not None:
            raise error

    def check_error(self) -> None:
        if self._error:
            message = ffi.string(lib.curl_easy_strerror(self._error)).decode()
            raise code2error(self._error, message)(message, self._error, self.response)

    def iter_bytes(self):
        while not self._closed:
            self.advance()
            if self._chunk is None:
                self.check_error()
                return
            chunk, self._chunk = self._chunk, None
            yield chunk

    def close(self) -> None:
        self._closed = True
        self._chunk = None


@contextmanager
def open_blocking_curl_stream(session, method, url, options):
    """Use the same curl_cffi request options and response parser as blocking send."""
    session._check_session_closed()
    options = dict(options)
    encoding = options.pop("default_encoding", "utf-8")
    discard_cookies = options.pop("discard_cookies", False)
    timeout = options.pop("timeout")
    multi = lib.curl_multi_init()
    if multi == ffi.NULL:
        raise CurlError("Could not initialize a curl stream")
    try:
        with closing(session.curl.duphandle()) as curl:
            stream = _CurlStream(curl, multi)
            request, buffer, headers, *_ = set_curl_options(
                curl,
                method=method,
                url=url,
                params_list=[session.params, options.pop("params", None)],
                headers_list=[session.headers, options.pop("headers", None)],
                cookies_list=[session.cookies, options.pop("cookies", None)],
                proxies_list=[session.proxies, options.pop("proxies", None)],
                verify_list=[session.verify, options.pop("verify", None)],
                trust_env=session.trust_env,
                impersonate=options.pop("impersonate", None) or session.impersonate,
                allow_redirects=False,
                accept_encoding=options.pop("accept_encoding", "gzip, deflate, br"),
                curl_options=session.curl_options,
                content_callback=stream.receive,
                timeout=None,
                **options,
            )
            if timeout is not None:
                if isinstance(timeout, tuple):
                    connect, read = timeout
                    idle = connect + read
                else:
                    connect = idle = timeout
                curl.setopt(CurlOpt.CONNECTTIMEOUT_MS, int(connect * 1000))
                curl.setopt(CurlOpt.LOW_SPEED_LIMIT, 1)
                curl.setopt(CurlOpt.LOW_SPEED_TIME, ceil(idle))
            curl._ensure_cacert()
            _check_multi(lib.curl_multi_add_handle(multi, curl._curl))
            try:
                stream.advance()
                head = session._parse_response(curl, buffer, headers, encoding, discard_cookies)
                head.request = request
                stream.response = head
                if not head.status_code:
                    stream.check_error()
                yield stream
            finally:
                stream.close()
                lib.curl_multi_remove_handle(multi, curl._curl)
    finally:
        lib.curl_multi_cleanup(multi)
