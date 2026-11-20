import asyncio
import time
from collections.abc import Mapping
from contextlib import contextmanager
from threading import RLock

from ..accounts.cookie_policy import is_cookie_chunk
from ..accounts.credentials import (
    read_session,
    read_session_async,
    save_session,
    save_session_async,
)
from ..accounts.filters import COOKIE_POLICIES_BY_ORIGIN, ProviderCookieFilter
from .http import AsyncHTTPTransport, HTTPTransport, curl_cookie_jar

_SECONDARY_PERSIST_INTERVAL = 5.0


def _chunked_cookie_values(cookies: Mapping[str, str], name: str) -> dict[str, str]:
    """Return integer-indexed chunk entries for a split session cookie."""
    return {key: value for key, value in cookies.items() if is_cookie_chunk(key, name)}


def _critical_cookies_changed(
    origin: str,
    saved_cookies: Mapping[str, str],
    cookies: Mapping[str, str],
    required_cookie: str | None,
) -> bool:
    """Return whether any auth, chunked session, or bot-challenge cookie changed."""
    policy = COOKIE_POLICIES_BY_ORIGIN[origin]
    for name in policy.authentication_names(required_cookie):
        if cookies.get(name) != saved_cookies.get(name):
            return True
        if policy.allow_chunks and (
            _chunked_cookie_values(cookies, name) != _chunked_cookie_values(saved_cookies, name)
        ):
            return True
    return any(cookies.get(name) != saved_cookies.get(name) for name in policy.critical_names)


class _SessionCookies:
    def _init_session(self, credential_file, required_cookie):
        self.path = credential_file
        self.required_cookie = required_cookie
        self.authenticated = False
        self._saved_cookies = None
        self._last_persisted_at: float | None = None
        self._dirty = False
        self.persistence_warning = None

    def _build_client(self, cookies, default_factory):
        factory = self.factory
        kwargs = dict(
            cookies=cookies,
            impersonate="chrome",
            timeout=self.config.timeout,
            headers={
                "origin": self.origin,
                "referer": self.origin + "/",
                "accept": "application/json",
            },
        )
        if factory is None:
            kwargs["cookies"] = curl_cookie_jar(cookies)
            kwargs["trust_env"] = False
            factory = default_factory
        return factory(**kwargs)

    def _load_initial_cookies(self, values):
        cookies = ProviderCookieFilter.filter_cookies(
            self.origin, values.get("cookies", values), self.required_cookie
        )
        self._record_persisted(cookies, time.monotonic())
        return cookies

    def _cookie_update(self, *, force):
        if self.client is None or self._saved_cookies is None:
            return None
        jar = getattr(self.client.cookies, "jar", None)
        if jar is None:
            cookies = {k: self.client.cookies.get(k) or v for k, v in self._saved_cookies.items()}
        else:
            cookies = ProviderCookieFilter.filter_from_jar(self.origin, jar, self._saved_cookies)
        if cookies == self._saved_cookies:
            self._dirty = False
            return None
        now = time.monotonic()
        if (
            not force
            and self._last_persisted_at is not None
            and now - self._last_persisted_at < _SECONDARY_PERSIST_INTERVAL
            and not _critical_cookies_changed(
                self.origin, self._saved_cookies, cookies, self.required_cookie
            )
        ):
            self._dirty = True
            return None
        return cookies, now

    def _record_persisted(self, cookies, now):
        self._saved_cookies = dict(cookies)
        self._last_persisted_at = now
        self._dirty = False

    def _warn_persist(self):
        self.persistence_warning = (
            "The session was refreshed, but the local record could not be updated."
        )


class AsyncHttpSession(_SessionCookies, AsyncHTTPTransport):
    def __init__(
        self, origin, credential_file, required_cookie, *, client_factory=None, config=None
    ):
        ProviderCookieFilter.validate_origin(origin)
        super().__init__(
            origin,
            client_factory=client_factory,
            after_response=self._persist_after_response,
            config=config,
        )
        self._init_session(credential_file, required_cookie)
        self._connect_lock = asyncio.Lock()
        self._save_lock = asyncio.Lock()

    def _create_client(self, cookies):
        from curl_cffi.requests import AsyncSession

        return self._build_client(cookies, AsyncSession)

    def _connect(self):
        if self.client is None:
            cookies = self._load_initial_cookies(read_session(self.path))
            self.client = self._create_client(cookies)
        return self.client

    async def _ensure_connected(self):
        if self.client is not None:
            return self.client
        async with self._connect_lock:
            if self.client is None:
                values = await read_session_async(self.path)
                if self.client is None:
                    self.client = self._create_client(self._load_initial_cookies(values))
        return self.client

    async def get_saved_cookies(self) -> dict[str, str]:
        await self._ensure_connected()
        return dict(self._saved_cookies or {})

    async def _acquire_client_async(self):
        await self._ensure_connected()
        return self._acquire_client()

    async def _reset_owned_client(self, failed_client):
        if (
            self._client_getter is None
            and self.client is failed_client
            and (self._dirty or self.client is not None)
        ):
            await self.persist(force=True)
        await super()._reset_owned_client(failed_client)

    async def send(self, method, path, **options):
        response = await super().send(method, path, **options)
        self.authenticated = True
        return response

    async def _persist_after_response(self):
        await self.persist(force=False)

    async def persist(self, *, force: bool = True):
        async with self._save_lock:
            if (update := self._cookie_update(force=force)) is None:
                return
            cookies, now = update
            try:
                await save_session_async(self.path, cookies)
            except OSError:
                self._warn_persist()
            else:
                self._record_persisted(cookies, now)

    async def close(self):
        saved_after = self._after_response
        self._after_response = None
        try:
            if self._dirty or self.client is not None:
                await self.persist(force=True)
            await super().close()
        finally:
            self._after_response = saved_after


class HttpSession(_SessionCookies, HTTPTransport):
    """Blocking HTTP session with the same saved credential policy as AsyncHttpSession."""

    def __init__(
        self, origin, credential_file, required_cookie, *, client_factory=None, config=None
    ):
        ProviderCookieFilter.validate_origin(origin)
        super().__init__(origin, client_factory=client_factory, config=config)
        self._init_session(credential_file, required_cookie)
        self._session_lock = RLock()

    def _connect(self):
        from curl_cffi.requests import Session

        with self._session_lock:
            if self.client is None:
                cookies = self._load_initial_cookies(read_session(self.path))
                self.client = self._build_client(cookies, Session)
            return self.client

    def get_saved_cookies(self) -> dict[str, str]:
        with self._session_lock:
            self._connect()
            return dict(self._saved_cookies or {})

    def persist(self, *, force: bool = True):
        with self._session_lock:
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
        with self._session_lock:
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
        with self._session_lock:
            try:
                self.persist()
            finally:
                super().close()
