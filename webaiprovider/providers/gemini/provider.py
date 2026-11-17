from __future__ import annotations

from typing import Any

from ...accounts.account import Account
from ...async_provider import AsyncProvider
from ...base import BaseProviderAdapter, Capability
from ...provider import Provider, ProviderType
from ...resources.chat.chat import BranchableChat, Chat
from ...transport import AsyncTransport
from .adapter import GeminiAdapter
from .notebooks import Notebooks
from .resources import AsyncGeminiChat


class AsyncGeminiProvider(AsyncProvider):
    provider_type = ProviderType.GEMINI
    stream_idle_timeout = 120
    stream_total_timeout = 600

    _chat_type = AsyncGeminiChat

    capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.HISTORY,
            Capability.DELETE,
            Capability.AUTH,
            Capability.MODELS,
            Capability.EFFORT,
            Capability.ATTACHMENTS,
            Capability.TEMPORARY,
            Capability.MESSAGE_EDIT,
            Capability.MESSAGE_RETRY,
            Capability.CHAT_BRANCH,
            Capability.RENAME,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.PROJECT_DELETE,
            Capability.RESOURCES,
        }
    )

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
        client_factory: Any = None,
        notebooks: Notebooks | None = None,
        transport: AsyncTransport | None = None,
        adapter: BaseProviderAdapter | None = None,
    ) -> None:
        self._client_factory = client_factory
        self._initial_notebooks = notebooks
        super().__init__(account, type=type, transport=transport, adapter=adapter)

    def _create_adapter(self) -> GeminiAdapter:
        return GeminiAdapter(
            account=self.account,
            capabilities=self.capabilities,
            transport=self._transport,
            client_factory=self._client_factory,
            notebooks=self._initial_notebooks,
        )

    @property
    def notebooks(self) -> Notebooks | None:
        return getattr(self.adapter, "notebooks", None)


class GeminiChat(BranchableChat, Chat):
    pass


class GeminiProvider(Provider):
    provider_type = ProviderType.GEMINI
    async_provider_type = AsyncGeminiProvider
    chat_wrapper_type = GeminiChat
