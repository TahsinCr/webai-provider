"""Blocking HTTP session for saved accounts."""

from contextlib import contextmanager

from ..accounts.credentials import read_session, save_session
from ..accounts.filters import ProviderCookieFilter
from .cookie import _SessionCookies
from .http import HTTPTransport


class HttpSession(_SessionCookies, HTTPTransport):
    """Blocking HTTP session with the same saved credential policy as AsyncHttpSession."""

    def __init__(
        self, origin, credential_file, required_cookie, *, client_factory=None, config=None
    ):
        ProviderCookieFilter.validate_origin(origin)
        super().__init__(origin, client_factory=client_factory, config=config)
        self._init_session(credential_file, required_cookie)

    def _connect(self):
        from curl_cffi.requests import Session

        with self._client_lock:
            if self.client is None:
                cookies = self._load_initial_cookies(read_session(self.path))
                self.client = self._build_client(cookies, Session)
            return self.client

    def get_saved_cookies(self) -> dict[str, str]:
        with self._client_lock:
            self._connect()
            return dict(self._saved_cookies or {})

    def persist(self, *, force: bool = True):
        with self._client_lock:
            if (update := self._cookie_update(force=force)) is None:
                return
            cookies, now = update
            try:
                save_session(self.path, cookies)
            except OSError:
                self._warn_persist()
            else:
                self._record_persisted(cookies, now)

    def _reset_owned_client(self, failed_client):
        with self._client_lock:
            if self.client is failed_client:
                self.persist()
                super()._reset_owned_client(failed_client)

    def send(self, method, path, **options):
        try:
            response = super().send(method, path, **options)
            self.authenticated = True
            return response
        finally:
            self.persist(force=False)

    @contextmanager
    def stream(self, method, path, **options):
        try:
            with super().stream(method, path, **options) as response:
                yield response
        finally:
            self.persist(force=False)

    def close(self):
        with self._client_lock:
            try:
                self.persist()
            finally:
                super().close()
