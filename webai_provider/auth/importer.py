from pathlib import Path
from typing import Any

from ..accounts.credentials import CredentialStore, save_session
from .filters import REQUIRED_PROVIDER_COOKIES, ProviderCookieFilter
from .reader import CookieReader


def import_browser_session(
    origin: str,
    target: CredentialStore | Path | str | None = None,
    profile: Any = None,
    required_cookie: str | None = None,
) -> dict[str, str]:
    ProviderCookieFilter.validate_origin(origin)
    req = required_cookie or REQUIRED_PROVIDER_COOKIES.get(origin)
    cookies = CookieReader.read_provider_cookies(origin, profile=profile)
    validated = ProviderCookieFilter.filter_cookies(origin, cookies, required_cookie=req)
    if target is not None:
        save_session(target, validated)
    return validated
