from typing import Any
from urllib.parse import urlsplit

from ..errors import AccountPaused

REQUIRED_PROVIDER_COOKIES = {
    "https://chatgpt.com": "__Secure-next-auth.session-token",
    "https://claude.ai": "sessionKey",
    "https://gemini.google.com": "__Secure-1PSID",
    "https://grok.com": "sso",
}

ALLOWED_PROVIDER_ORIGINS = (
    "https://chatgpt.com",
    "https://claude.ai",
    "https://gemini.google.com",
    "https://grok.com",
)


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
            raise AccountPaused("Oturumunuzu Hesaplar bölümünden bir kez yeniden doğrulayın")
        req = required_cookie or REQUIRED_PROVIDER_COOKIES.get(origin)
        if req is not None and not cookies.get(req):
            raise AccountPaused("Oturumunuzu Hesaplar bölümünden bir kez yeniden doğrulayın")
        for key, val in cookies.items():
            if not isinstance(key, str) or not isinstance(val, str):
                raise AccountPaused("Oturum kaydı geçersiz")
            if "\n" in key + val or "\r" in key + val:
                raise AccountPaused("Oturum kaydı geçersiz")
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
