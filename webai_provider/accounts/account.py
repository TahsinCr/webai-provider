from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..models import Capability
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

    def store(self) -> CredentialStore:
        if isinstance(self.credentials, CredentialStore):
            return self.credentials
        if isinstance(self.credentials, dict):
            return MemoryCredentialStore(self.credentials)
        return FileCredentialStore(self.credentials)

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
