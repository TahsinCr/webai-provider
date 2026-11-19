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
