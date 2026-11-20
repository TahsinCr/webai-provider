from pathlib import Path
from urllib.parse import urlsplit

from .cookie_policy import PROVIDER_COOKIE_POLICIES
from .cookies import import_cookies
from .credentials import CredentialStore, save_session
from .filters import ProviderCookieFilter
from .profiles import BrowserProfile, default_profile

_ORIGIN_TO_PROVIDER = {rule.origin: name for name, rule in PROVIDER_COOKIE_POLICIES.items()}


class CookieReader:
    @staticmethod
    def default_profile(browser: str | None = None) -> BrowserProfile | None:
        return default_profile(browser)

    @staticmethod
    def provider_for_origin(origin_or_provider: str) -> str:
        if origin_or_provider in _ORIGIN_TO_PROVIDER:
            return _ORIGIN_TO_PROVIDER[origin_or_provider]
        parsed = urlsplit(origin_or_provider)
        if parsed.scheme and parsed.netloc:
            origin = f"{parsed.scheme}://{parsed.netloc}"
            if origin in _ORIGIN_TO_PROVIDER:
                return _ORIGIN_TO_PROVIDER[origin]
            raise ValueError(f"unsupported session origin: {origin_or_provider}")
        provider = origin_or_provider.strip().lower()
        if provider not in PROVIDER_COOKIE_POLICIES:
            raise ValueError(f"unsupported provider: {origin_or_provider}")
        return provider

    @classmethod
    def read_provider_cookies(
        cls,
        origin_or_provider: str,
        profile: BrowserProfile | None = None,
    ) -> dict[str, str]:
        provider = cls.provider_for_origin(origin_or_provider)
        _family, values = import_cookies(provider, profile=profile)
        return values


def import_browser_session(
    origin: str,
    target: CredentialStore | Path | str | None = None,
    profile: BrowserProfile | None = None,
    required_cookie: str | None = None,
) -> dict[str, str]:
    ProviderCookieFilter.validate_origin(origin)
    cookies = CookieReader.read_provider_cookies(origin, profile=profile)
    validated = ProviderCookieFilter.filter_cookies(
        origin, cookies, required_cookie=required_cookie
    )
    if target is not None:
        save_session(target, validated)
    return validated
