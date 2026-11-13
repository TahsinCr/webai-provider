import asyncio
import time
from collections.abc import Mapping

from ..accounts.credentials import read_session, save_session_async
from ..accounts.filters import REQUIRED_PROVIDER_COOKIES, ProviderCookieFilter
from .http import AsyncHTTPTransport

_SECONDARY_PERSIST_INTERVAL = 5.0
_CRITICAL_COOKIE_NAMES = ("sso", "sso-rw", "__cf_bm", "cf_clearance")


def _chunked_cookie_values(cookies: Mapping[str, str], prefix: str) -> dict[str, str]:
    """Return integer-indexed chunk entries for a split session cookie."""
    return {
        key: value
        for key, value in cookies.items()
        if key.startswith(prefix) and (suffix := key[len(prefix) :]).isascii() and suffix.isdigit()
    }


def _critical_cookies_changed(
    origin: str,
    saved_cookies: Mapping[str, str],
    cookies: Mapping[str, str],
    required_cookie: str | None,
) -> bool:
    """Return whether any auth, chunked session, or bot-challenge cookie changed."""
    required = required_cookie or REQUIRED_PROVIDER_COOKIES.get(origin)
    if required is not None:
        if cookies.get(required) != saved_cookies.get(required):
            return True
        prefix = f"{required}."
        if _chunked_cookie_values(cookies, prefix) != _chunked_cookie_values(saved_cookies, prefix):
            return True
    return any(cookies.get(name) != saved_cookies.get(name) for name in _CRITICAL_COOKIE_NAMES)


class HttpSession(AsyncHTTPTransport):
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
        self.path = credential_file
        self.required_cookie = required_cookie
        self.factory = client_factory
        self.client = None
        self.authenticated = False
        self._saved_cookies = None
        self._save_lock = asyncio.Lock()
        self._last_persisted_at: float | None = None
        self._dirty = False
        self.persistence_warning = None

    def _connect(self):
        if self.client is None:
            values = read_session(self.path)
            raw_cookies = values.get("cookies", values)
            cookies = ProviderCookieFilter.filter_cookies(
                self.origin, raw_cookies, self.required_cookie
            )
            self._saved_cookies = dict(cookies)
            self._last_persisted_at = time.monotonic()
            self._dirty = False
            factory = self.factory
            if factory is None:
                from curl_cffi.requests import AsyncSession

                factory = AsyncSession
            self.client = factory(
                cookies=cookies,
                impersonate="chrome",
                timeout=self.config.timeout,
                headers={
                    "origin": self.origin,
                    "referer": self.origin + "/",
                    "accept": "application/json",
                },
            )
        return self.client

    async def send(self, method, path, **options):
        response = await super().send(method, path, **options)
        self.authenticated = True
        return response

    async def _persist_after_response(self):
        await self.persist(force=False)

    async def persist(self, *, force: bool = True):
        if self.client is None or self._saved_cookies is None:
            return
        async with self._save_lock:
            jar = getattr(self.client.cookies, "jar", None)
            if jar is None:
                cookies = {
                    k: self.client.cookies.get(k) or v for k, v in self._saved_cookies.items()
                }
            else:
                cookies = ProviderCookieFilter.filter_from_jar(
                    self.origin, jar, self._saved_cookies
                )
            if cookies == self._saved_cookies:
                self._dirty = False
                return
            required_changed = _critical_cookies_changed(
                self.origin, self._saved_cookies, cookies, self.required_cookie
            )
            now = time.monotonic()
            if (
                not force
                and not required_changed
                and self._last_persisted_at is not None
                and now - self._last_persisted_at < _SECONDARY_PERSIST_INTERVAL
            ):
                self._dirty = True
                return
            try:
                await save_session_async(self.path, cookies)
                self._saved_cookies = cookies
                self._last_persisted_at = now
                self._dirty = False
            except OSError:
                self.persistence_warning = (
                    "The session was refreshed, but the local record could not be updated."
                )

    async def close(self):
        saved_after = self._after_response
        self._after_response = None
        try:
            if self._dirty or self.client is not None:
                await self.persist(force=True)
            await super().close()
        finally:
            self._after_response = saved_after
