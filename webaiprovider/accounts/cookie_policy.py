from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit


@dataclass(frozen=True, slots=True)
class CookiePolicy:
    origin: str
    required: str
    names: frozenset[str]
    browser_domain: str | None = None
    allow_chunks: bool = False
    alternatives: tuple[str, ...] = ()
    secure_names: frozenset[str] = frozenset()
    critical_names: tuple[str, ...] = ()

    @property
    def host(self) -> str:
        return urlsplit(self.origin).netloc

    def authentication_names(self, required: str | None = None) -> tuple[str, ...]:
        if required and required != self.required:
            return (required,)
        return (self.required, *self.alternatives)

    def matches_name(self, name: str) -> bool:
        return name in self.names or (
            self.allow_chunks
            and any(is_cookie_chunk(name, base) for base in self.authentication_names())
        )

    def accepted(self, cookies: dict[str, str], required: str | None = None) -> bool:
        for name in self.authentication_names(required):
            if cookies.get(name):
                return True
            if self.allow_chunks:
                count = sum(is_cookie_chunk(key, name) for key in cookies)
                if count and all(cookies.get(f"{name}.{i}") for i in range(count)):
                    return True
        return False


def is_cookie_chunk(key: str, name: str) -> bool:
    prefix = name + "."
    if not key.startswith(prefix):
        return False
    suffix = key[len(prefix) :]
    return bool(suffix) and suffix.isascii() and suffix.isdecimal()


PROVIDER_COOKIE_POLICIES = {
    "chatgpt": CookiePolicy(
        origin="https://chatgpt.com",
        required="__Secure-next-auth.session-token",
        names=frozenset(
            {
                "__Host-next-auth.csrf-token",
                "__Secure-next-auth.session-token",
                "__Secure-authjs.session-token",
                "__cf_bm",
                "cf_clearance",
                "oai-did",
            }
        ),
        allow_chunks=True,
        alternatives=("__Secure-authjs.session-token",),
        critical_names=("__cf_bm", "cf_clearance"),
    ),
    "claude": CookiePolicy(
        origin="https://claude.ai",
        required="sessionKey",
        names=frozenset(
            {"__cf_bm", "anthropic-device-id", "cf_clearance", "lastActiveOrg", "sessionKey"}
        ),
        allow_chunks=True,
        critical_names=("__cf_bm", "cf_clearance"),
    ),
    "gemini": CookiePolicy(
        origin="https://gemini.google.com",
        required="__Secure-1PSID",
        names=frozenset(
            {
                "APISID",
                "HSID",
                "SAPISID",
                "SID",
                "SIDCC",
                "SSID",
                "__Secure-1PAPISID",
                "__Secure-1PSID",
                "__Secure-1PSIDCC",
                "__Secure-1PSIDTS",
                "__Secure-3PAPISID",
                "__Secure-3PSID",
                "__Secure-3PSIDCC",
                "__Secure-3PSIDTS",
            }
        ),
        browser_domain="google.com",
    ),
    "grok": CookiePolicy(
        origin="https://grok.com",
        required="sso",
        names=frozenset({"sso", "sso-rw"}),
        secure_names=frozenset({"sso", "sso-rw"}),
        alternatives=("sso-rw",),
        critical_names=("sso-rw", "__cf_bm", "cf_clearance"),
    ),
}
COOKIE_POLICIES_BY_ORIGIN = {rule.origin: rule for rule in PROVIDER_COOKIE_POLICIES.values()}
REQUIRED_PROVIDER_COOKIES = {
    origin: rule.required for origin, rule in COOKIE_POLICIES_BY_ORIGIN.items()
}
ALLOWED_PROVIDER_ORIGINS = tuple(COOKIE_POLICIES_BY_ORIGIN)
