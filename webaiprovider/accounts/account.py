from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..base import Capability
from ..provider import ProviderType
from .credentials import CredentialStore, FileCredentialStore, MemoryCredentialStore


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

    def __post_init__(self) -> None:
        object.__setattr__(self, "provider", ProviderType.parse(self.provider).value)
        if isinstance(self.credentials, dict):
            object.__setattr__(self, "_memory_store", MemoryCredentialStore(self.credentials))

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
