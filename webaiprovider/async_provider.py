from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import BaseProvider
from .catalogs import Models, ModelsCollection, _model_refs_from_catalog
from .collections.chats import ChatsCollection
from .collections.projects import ProjectsCollection
from .contracts import EffortRef, ModelRef, ModelSelection
from .core.scope import ProviderResourceScope
from .core.telemetry import ProviderTelemetry
from .errors import ProviderError
from .provider import ProviderType
from .providers import create_adapter
from .providers.base import ProviderAdapter
from .resources.chat import Chat
from .resources.project import Project
from .resources.response import Response

if TYPE_CHECKING:
    from .accounts.account import Account


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
        self._adapter: ProviderAdapter | None = None
        self._opened = False
        self.chats = ChatsCollection(self)
        self.projects = self._projects_collection_type(self)
        self.models = ModelsCollection(self._load_models)

    async def _load_models(self, refresh: bool = False) -> list[ModelRef]:
        catalog = await self._require_adapter().models(force=refresh)
        return _model_refs_from_catalog(self.type.value, catalog)

    async def open(self) -> AsyncProvider:
        if self._opened:
            return self
        scope = ProviderResourceScope()
        adapter = create_adapter(self.account, self.type)
        try:
            await adapter.prepare()
        except BaseException:
            try:
                await adapter.close()
            except Exception:
                pass
            await scope.aclose()
            raise
        scope.defer(adapter.close)
        self._scope = scope
        self._adapter = adapter
        self._opened = True
        return self

    def _require_adapter(self) -> ProviderAdapter:
        if self._adapter is None or not self._opened:
            raise ProviderError("Open the provider before using its resources")
        return self._adapter

    async def close(self) -> None:
        scope, self._scope = self._scope, None
        adapter, self._adapter = self._adapter, None
        self._opened = False
        if scope is not None:
            await scope.aclose()
        elif adapter is not None:
            await adapter.close()

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
