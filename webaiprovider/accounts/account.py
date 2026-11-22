from __future__ import annotations

import hashlib
import json
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..base import Capability, ProviderType
from .credentials import CredentialStore, FileCredentialStore, MemoryCredentialStore


def credential_fingerprint(data: Any) -> str | None:
    """Return a non-reversible SHA-256 digest of a credential mapping."""
    if not isinstance(data, Mapping):
        return None
    try:
        encoded = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError):
        return None
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Account:
    id: str
    provider: str
    credentials: CredentialStore | Path | str | dict[str, Any]
    capabilities: tuple[Capability, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)
    name: str | None = None
    is_active: bool = True
    _memory_store: MemoryCredentialStore | None = field(
        default=None, init=False, repr=False, compare=False
    )
    _session_lock: threading.Lock = field(init=False, repr=False, compare=False)
    _verified_session: tuple[str, float, Any] | None = field(
        default=None, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "provider", ProviderType.parse(self.provider).value)
        object.__setattr__(self, "_session_lock", threading.Lock())
        if isinstance(self.credentials, dict):
            object.__setattr__(self, "_memory_store", MemoryCredentialStore(self.credentials))

    def _get_verified_session(self, fingerprint: str | None, *, ttl: float) -> Any | None:
        if not fingerprint:
            return None
        with self._session_lock:
            entry = self._verified_session
            if entry is None:
                return None
            stored_fp, verified_at, payload = entry
            if stored_fp != fingerprint or time.monotonic() - verified_at >= ttl:
                object.__setattr__(self, "_verified_session", None)
                return None
            return payload

    def _set_verified_session(self, fingerprint: str | None, payload: Any) -> None:
        if not fingerprint:
            return
        with self._session_lock:
            object.__setattr__(self, "_verified_session", (fingerprint, time.monotonic(), payload))

    def _clear_verified_session(self) -> None:
        with self._session_lock:
            object.__setattr__(self, "_verified_session", None)

    def store(self) -> CredentialStore:
        if isinstance(self.credentials, CredentialStore):
            return self.credentials
        if self._memory_store is not None:
            return self._memory_store
        return FileCredentialStore(self.credentials)

    @classmethod
    def from_cookies(
        cls,
        provider: ProviderType | str,
        cookies: dict[str, Any] | Path | str,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> Account:
        import uuid

        account_id = id or str(uuid.uuid4())
        provider_type = ProviderType.parse(provider)
        return cls(
            id=account_id,
            provider=provider_type.value,
            credentials=cookies,
            name=name or f"{provider_type.value.capitalize()} Account",
        )

    @classmethod
    def from_browser(
        cls,
        provider: ProviderType | str,
        *,
        browser: str | None = None,
        id: str | None = None,
        name: str | None = None,
    ) -> Account:
        from .importer import import_cookies

        provider_type = ProviderType.parse(provider)
        _, cookies = import_cookies(provider_type.value, browser=browser)
        return cls.from_cookies(provider, cookies, id=id, name=name)
