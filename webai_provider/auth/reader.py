from typing import Any
from urllib.parse import urlsplit

from ..profiles import BrowserProfile, default_profile, import_cookies

_ORIGIN_TO_PROVIDER = {
    "https://chatgpt.com": "chatgpt",
    "https://claude.ai": "claude",
    "https://gemini.google.com": "gemini",
    "https://grok.com": "grok",
}


class CookieReader:
    @staticmethod
    def default_profile(family: str | None = None) -> BrowserProfile | None:
        return default_profile(family)

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
        if provider not in _ORIGIN_TO_PROVIDER.values():
            raise ValueError(f"unsupported provider: {origin_or_provider}")
        return provider

    @classmethod
    def read_provider_cookies(
        cls,
        origin_or_provider: str,
        profile: Any = None,
    ) -> dict[str, str]:
        provider = cls.provider_for_origin(origin_or_provider)
        _family, values = import_cookies(provider)
        return values
