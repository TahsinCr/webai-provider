from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..models import Capability
from .credentials import CredentialStore, FileCredentialStore, MemoryCredentialStore

if TYPE_CHECKING:
    from ..session import ProviderSession


@dataclass(frozen=True)
class Account:
    id: str
    provider: str
    credentials: CredentialStore | Path | str | dict[str, Any]
    capabilities: tuple[Capability, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)
    name: str | None = None
    is_active: bool = True

    def store(self) -> CredentialStore:
        if isinstance(self.credentials, CredentialStore):
            return self.credentials
        if isinstance(self.credentials, dict):
            return MemoryCredentialStore(self.credentials)
        return FileCredentialStore(self.credentials)

    @classmethod
    def from_cookies(
        cls,
        provider: str,
        cookies: dict[str, Any] | Path | str,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> Account:
        import uuid

        account_id = id or str(uuid.uuid4())
        return cls(
            id=account_id,
            provider=provider.lower(),
            credentials=cookies,
            name=name or f"{provider.capitalize()} Account",
        )

    @classmethod
    def from_browser(
        cls,
        provider: str,
        *,
        browser: str | None = None,
        id: str | None = None,
        name: str | None = None,
    ) -> Account:
        from .importer import import_cookies

        _, cookies = import_cookies(provider.lower(), browser=browser)
        return cls.from_cookies(provider, cookies, id=id, name=name)

    def connect(self) -> ProviderSession:
        from ..bridge import get_bridge
        from ..session import ProviderSession

        bridge = get_bridge(self.provider, self)
        return ProviderSession(self, bridge=bridge)

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
