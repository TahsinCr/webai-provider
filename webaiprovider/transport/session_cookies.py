"""Credential and cookie persistence policy shared by HTTP sessions."""

import time
from collections.abc import Mapping

from ..accounts.cookie_policy import is_cookie_chunk
from ..accounts.filters import COOKIE_POLICIES_BY_ORIGIN, ProviderCookieFilter
from .http import curl_cookie_jar

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
