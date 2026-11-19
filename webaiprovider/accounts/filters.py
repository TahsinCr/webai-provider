from typing import Any
from urllib.parse import urlsplit

from ..errors import AccountPaused
from ..providers.chatgpt.auth import COOKIE_POLICY as CHATGPT_COOKIES
from ..providers.claude.auth import COOKIE_POLICY as CLAUDE_COOKIES
from ..providers.gemini.auth import COOKIE_POLICY as GEMINI_COOKIES
from ..providers.grok.auth import COOKIE_POLICY as GROK_COOKIES

PROVIDER_COOKIE_POLICIES = {
    "chatgpt": CHATGPT_COOKIES,
    "claude": CLAUDE_COOKIES,
    "gemini": GEMINI_COOKIES,
    "grok": GROK_COOKIES,
}
COOKIE_POLICIES_BY_ORIGIN = {rule.origin: rule for rule in PROVIDER_COOKIE_POLICIES.values()}
REQUIRED_PROVIDER_COOKIES = {
    origin: rule.required for origin, rule in COOKIE_POLICIES_BY_ORIGIN.items()
}
ALLOWED_PROVIDER_ORIGINS = tuple(COOKIE_POLICIES_BY_ORIGIN)


class ProviderCookieFilter:
    @staticmethod
    def validate_origin(origin: str) -> str:
        if origin not in ALLOWED_PROVIDER_ORIGINS:
            raise ValueError(f"unsupported session origin: {origin}")
        return origin

    @staticmethod
    def domain_matches(host: str, cookie_domain: str | None) -> bool:
        if not cookie_domain:
            return True
        clean_domain = cookie_domain.lstrip(".")
        return host == clean_domain or host.endswith("." + clean_domain)

    @classmethod
    def filter_cookies(
        cls,
        origin: str,
        cookies: dict[str, str],
        required_cookie: str | None = None,
    ) -> dict[str, str]:
        cls.validate_origin(origin)
        if not isinstance(cookies, dict) or not cookies:
            raise AccountPaused("Reverify your session once through the Accounts section")
        for key, val in cookies.items():
            if not isinstance(key, str) or not isinstance(val, str):
                raise AccountPaused("Session record is invalid")
            if any(char in key + val for char in "\n\r\0"):
                raise AccountPaused("Session record is invalid")
        if not COOKIE_POLICIES_BY_ORIGIN[origin].accepted(cookies, required_cookie):
            raise AccountPaused("Reverify your session once through the Accounts section")
        return dict(cookies)

    @classmethod
    def filter_from_jar(
        cls,
        origin: str,
        jar: Any,
        saved_cookies: dict[str, str],
    ) -> dict[str, str]:
        host = urlsplit(origin).hostname or ""
        cookies = dict(saved_cookies)
        for cookie in sorted(jar, key=lambda c: len(c.domain or "")):
            if cookie.name in cookies and cls.domain_matches(host, cookie.domain):
                cookies[cookie.name] = cookie.value
        return cookies


def filter_provider_cookies(
    origin: str, cookies: dict[str, str], required: str | None = None
) -> dict[str, str]:
    return ProviderCookieFilter.filter_cookies(origin, cookies, required)
