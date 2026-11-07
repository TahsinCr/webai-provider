from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .catalogs.dynamic import ModelsCollection
from .catalogs.policies import Models
from .collections.chats import ChatsCollection
from .collections.projects import ProjectsCollection
from .contracts import EffortRef, ModelRef, ModelSelection
from .errors import ProviderError
from .provider_type import ProviderType
from .resources.response import Response

if TYPE_CHECKING:
    from .accounts.account import Account
    from .bridge import ProviderBridge


class Provider:
    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
    ) -> None:
        account_type = ProviderType.parse(account.provider)
        self.type = ProviderType.parse(type) if type is not None else account_type
        if self.type is not account_type:
            raise ValueError("Provider type must match the account provider")
        self.account = account
        self._bridge: ProviderBridge | None = None
        self._opened = False
        self.chats = ChatsCollection(self)
        self.projects = ProjectsCollection(self)
        self.models = ModelsCollection(self._load_models)

    @property
    def provider(self) -> str:
        return self.account.provider

    async def _load_models(self, refresh: bool = False) -> list[ModelRef]:
        bridge = self._require_bridge()
        return await bridge.list_models(refresh=refresh)

    async def open(self) -> Provider:
        if self._opened:
            return self
        from .bridge import ProviderBridge

        bridge = ProviderBridge(self.account)
        try:
            await bridge.open()
        except BaseException:
            try:
                await bridge.close()
            except Exception:
                pass
            raise
        self._bridge = bridge
        self._opened = True
        return self

    def _require_bridge(self) -> ProviderBridge:
        if self._bridge is None or not self._opened:
            raise ProviderError("Open the provider before using its resources")
        return self._bridge

    async def close(self) -> None:
        if self._bridge is not None:
            bridge, self._bridge = self._bridge, None
            self._opened = False
            await bridge.close()

    async def ask(
        self,
        prompt: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Any = None,
    ) -> Response:
        was_open = self._opened
        if not was_open:
            await self.open()
        try:
            chat = await self.chats.create()
            return await chat.messages.create(
                prompt,
                model=model,
                attachments=attachments,
            ).start()
        finally:
            if not was_open:
                await self.close()

    async def __aenter__(self) -> Provider:
        return await self.open()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    def __repr__(self) -> str:
        return f"<Provider type={self.type.value!r} account={self.account.id!r}>"
