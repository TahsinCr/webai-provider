from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..models import Capability
from ..provider_type import ProviderType
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

    def __post_init__(self) -> None:
        object.__setattr__(self, "provider", ProviderType.parse(self.provider).value)

    def store(self) -> CredentialStore:
        if isinstance(self.credentials, CredentialStore):
            return self.credentials
        if isinstance(self.credentials, dict):
            return MemoryCredentialStore(self.credentials)
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
        return cls(
            id=account_id,
            provider=ProviderType.parse(provider).value,
            credentials=cookies,
            name=name or f"{provider.capitalize()} Account",
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

    def to_dict(self) -> dict[str, Any]:
        data = {
            "id": self.id,
            "provider": self.provider,
            "capabilities": [c.value if hasattr(c, "value") else str(c) for c in self.capabilities],
            "metadata": dict(self.metadata),
            "name": self.name,
            "is_active": self.is_active,
        }
        if isinstance(self.credentials, (str, Path)):
            data["credentials"] = str(self.credentials)
        return data
