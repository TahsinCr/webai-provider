from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import BaseProvider
from .catalogs import Models, ModelsCollection
from .collections.chats import ChatsCollection
from .collections.projects import ProjectsCollection
from .contracts import EffortRef, ModelRef, ModelSelection
from .core.scope import ProviderResourceScope
from .core.telemetry import ProviderTelemetry
from .errors import ProviderError
from .provider_type import ProviderType
from .resources.chat import Chat
from .resources.project import Project
from .resources.response import Response

if TYPE_CHECKING:
    from .accounts.account import Account
    from .bridge import ProviderBridge


class AsyncProvider(BaseProvider):
    _chat_type = Chat
    _project_type = Project
    _projects_collection_type = ProjectsCollection

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
    ) -> None:
        super().__init__(account, type=type)
        self.telemetry = ProviderTelemetry()
        self._scope: ProviderResourceScope | None = None
        self._bridge: ProviderBridge | None = None
        self._opened = False
        self.chats = ChatsCollection(self)
        self.projects = self._projects_collection_type(self)
        self.models = ModelsCollection(self._load_models)

    async def _load_models(self, refresh: bool = False) -> list[ModelRef]:
        bridge = self._require_bridge()
        return await bridge.list_models(refresh=refresh)

    async def open(self) -> AsyncProvider:
        if self._opened:
            return self
        from .bridge import ProviderBridge

        scope = ProviderResourceScope()
        bridge = ProviderBridge(self.account)
        try:
            await bridge.open()
        except BaseException:
            try:
                await bridge.close()
            except Exception:
                pass
            await scope.aclose()
            raise
        scope.defer(bridge.close)
        self._scope = scope
        self._bridge = bridge
        self._opened = True
        return self

    def _require_bridge(self) -> ProviderBridge:
        if self._bridge is None or not self._opened:
            raise ProviderError("Open the provider before using its resources")
        return self._bridge

    async def close(self) -> None:
        scope, self._scope = self._scope, None
        bridge, self._bridge = self._bridge, None
        self._opened = False
        if scope is not None:
            await scope.aclose()
        elif bridge is not None:
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

    async def __aenter__(self) -> AsyncProvider:
        return await self.open()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    def __repr__(self) -> str:
        return f"<Provider type={self.type.value!r} account={self.account.id!r}>"
