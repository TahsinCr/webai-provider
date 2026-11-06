from pathlib import Path
from urllib.parse import urlsplit

from ..profiles import BrowserProfile, default_profile, import_cookies


class CookieReader:
    @staticmethod
    def default_profile(family: str | None = None) -> BrowserProfile | None:
        return default_profile(family)

    @staticmethod
    def read_profile_cookies(
        profile: BrowserProfile | str | Path | None = None,
        domains: tuple[str, ...] | None = None,
    ) -> dict[str, str]:
        return import_cookies(profile=profile, domains=domains)

    @staticmethod
    def read_provider_cookies(
        origin: str,
        profile: BrowserProfile | str | Path | None = None,
    ) -> dict[str, str]:
        host = urlsplit(origin).hostname or origin
        domains = (host,)
        return CookieReader.read_profile_cookies(profile=profile, domains=domains)
