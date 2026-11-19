from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from ..errors import (
    ProfileError,
    ProfileInvalidResultError,
    ProfileSessionNotFoundError,
    ProfileTimeoutError,
)
from .filters import PROVIDER_COOKIE_POLICIES, ProviderCookieFilter
from .profiles import (
    _PROFILE_ERROR_MAP,
    BrowserProfile,
    _failure_code,
    _firefox_rows,
    _firefox_session_cookies,
    snapshot_sqlite_file,
)


def select_cookies(jar, provider):
    policy = PROVIDER_COOKIE_POLICIES[provider]
    host, values = policy.host, {}

    def value_of(cookie, key, default=None):
        return (
            cookie.get(key, default) if isinstance(cookie, dict) else getattr(cookie, key, default)
        )

    now = time.time()
    for cookie in sorted(jar, key=lambda item: len(value_of(item, "domain", "") or "")):
        domain = (value_of(cookie, "domain", "") or "").lstrip(".").lower()
        if not domain or not ProviderCookieFilter.domain_matches(host, domain):
            continue
        name, value = value_of(cookie, "name"), value_of(cookie, "value")
        if not isinstance(name, str) or not name or not isinstance(value, str):
            continue
        expires = value_of(cookie, "expires")
        expired = (
            cookie.is_expired()
            if hasattr(cookie, "is_expired")
            else isinstance(expires, (int, float)) and expires > 0 and expires <= now
        )
        if expired or value_of(cookie, "path", "/") != "/":
            continue
        if not policy.matches_name(name):
            continue
        if name in policy.secure_names and not value_of(cookie, "secure", False):
            continue
        if any(c in name + value for c in "\r\n\0"):
            continue
        values[name] = value
    if not policy.accepted(values):
        raise ProfileSessionNotFoundError()
    return values


def import_cookies(
    provider: str,
    browser: str | None = None,
    *,
    profile: BrowserProfile | None = None,
):
    if provider not in PROVIDER_COOKIE_POLICIES:
        raise ValueError("unsupported provider")
    from . import profiles

    profile = profile or profiles.default_profile(browser)
    try:
        result = subprocess.run(
            [sys.executable, "-m", "webaiprovider.accounts.cookies"],
            input=json.dumps(
                {
                    "provider": provider,
                    "family": profile.family,
                    "cookie_backend": profile.cookie_backend,
                    "cookies": str(profile.cookies),
                    "key_file": str(profile.key_file) if profile.key_file else None,
                }
            ),
            capture_output=True,
            text=True,
            timeout=25,
            check=False,
            env=dict(
                os.environ,
                PYTHONPATH=str(Path(__file__).resolve().parent.parent.parent)
                + os.pathsep
                + os.environ.get("PYTHONPATH", ""),
            ),
        )
    except subprocess.TimeoutExpired:
        raise ProfileTimeoutError() from None
    if result.returncode == 2:
        raise ProfileSessionNotFoundError()
    if result.returncode:
        error_cls = _PROFILE_ERROR_MAP.get(result.returncode, ProfileError)
        raise error_cls()
    try:
        if len(result.stdout) > 1_048_576:
            raise ValueError("invalid result")
        values = json.loads(result.stdout)
        if (
            not isinstance(values, dict)
            or not values
            or any(not isinstance(k, str) or not isinstance(v, str) for k, v in values.items())
        ):
            raise ValueError("invalid result")
    except ValueError:
        raise ProfileInvalidResultError() from None
    return profile.family, values


def _main():
    try:
        import sqlite3

        import browser_cookie3

        data = json.load(sys.stdin)
        family = data["family"]
        if family == "firefox":
            from http.cookiejar import CookieJar

            jar = CookieJar()
            for row in _firefox_rows(data["cookies"], data["provider"]):
                jar.set_cookie(browser_cookie3.create_cookie(*row))
            for cookie in _firefox_session_cookies(Path(data["cookies"]).parent, data["provider"]):
                jar.set_cookie(
                    browser_cookie3.create_cookie(
                        cookie.get("host", ""),
                        cookie.get("path", "/"),
                        cookie.get("secure", False),
                        cookie.get("expiry"),
                        cookie.get("name", ""),
                        cookie.get("value", ""),
                        cookie.get("httponly", False),
                    )
                )
        else:
            policy = PROVIDER_COOKIE_POLICIES[data["provider"]]
            cookie_domain = policy.browser_domain or policy.host
            loader = getattr(browser_cookie3, data["cookie_backend"])
            try:
                jar = loader(
                    cookie_file=data["cookies"],
                    key_file=data["key_file"],
                    domain_name=cookie_domain,
                )
            except sqlite3.OperationalError:
                snapshot_file, temp_holder = snapshot_sqlite_file(Path(data["cookies"]))
                try:
                    jar = loader(
                        cookie_file=str(snapshot_file),
                        key_file=data["key_file"],
                        domain_name=cookie_domain,
                    )
                finally:
                    temp_holder.cleanup()
        values = select_cookies(jar, data["provider"])
        sys.stdout.write(json.dumps(values))
    except ProfileError:
        return 2
    except Exception as error:
        return _failure_code(error)
    return 0


if __name__ == "__main__":
    sys.exit(_main())
