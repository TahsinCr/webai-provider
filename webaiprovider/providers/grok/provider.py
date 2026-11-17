from __future__ import annotations

from typing import Any

from ...accounts.account import Account
from ...async_provider import AsyncProvider
from ...base import BaseProviderAdapter, Capability
from ...provider import Provider, ProviderType
from ...resources.chat.chat import Chat, MovableChat
from ...transport import AsyncTransport
from .adapter import GrokAdapter
from .resources import AsyncGrokChat


class AsyncGrokProvider(AsyncProvider):
    provider_type = ProviderType.GROK

    _chat_type = AsyncGrokChat

    capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.TEMPORARY,
            Capability.AUTH,
            Capability.ATTACHMENTS,
            Capability.HISTORY,
            Capability.RENAME,
            Capability.DELETE,
            Capability.MODELS,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.PROJECT_DELETE,
            Capability.PROJECT_MEMBERSHIP,
            Capability.RESOURCES,
            Capability.MESSAGE_RETRY,
            Capability.MESSAGE_EDIT,
        }
    )

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        client_factory: Any = None,
        transport: AsyncTransport | None = None,
        adapter: BaseProviderAdapter | None = None,
    ) -> None:
        self._client_factory = client_factory
        super().__init__(account, type=type, transport=transport, adapter=adapter)

    def _create_adapter(self) -> GrokAdapter:
        return GrokAdapter(
            account=self.account,
            capabilities=self.capabilities,
            transport=self._transport,
            client_factory=self._client_factory,
        )


class GrokChat(MovableChat, Chat):
    pass


class GrokProvider(Provider):
    provider_type = ProviderType.GROK
    async_provider_type = AsyncGrokProvider
    chat_wrapper_type = GrokChat
