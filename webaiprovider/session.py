from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .catalogs.dynamic import ModelsCollection
from .collections.chats import ChatsCollection
from .collections.projects import ProjectsCollection
from .contracts import ModelRef

if TYPE_CHECKING:
    from .accounts.account import Account
    from .bridge import ProviderBridge


class ProviderSession:
    def __init__(
        self,
        account: Account,
        *,
        bridge: ProviderBridge | None = None,
    ) -> None:
        self.account = account
        self._bridge = bridge
        self.chats = ChatsCollection(self)
        self.projects = ProjectsCollection(self)
        self.models = ModelsCollection(self._load_models)

    @property
    def provider(self) -> str:
        return self.account.provider

    async def _load_models(self, refresh: bool = False) -> list[ModelRef]:
        if self._bridge is not None:
            return await self._bridge.list_models(refresh=refresh)
        return []

    async def close(self) -> None:
        if self._bridge is not None:
            await self._bridge.close_session(self)

    async def __aenter__(self) -> ProviderSession:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    def __repr__(self) -> str:
        return f"<ProviderSession provider={self.provider!r} account={self.account.id!r}>"
