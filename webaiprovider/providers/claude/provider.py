from __future__ import annotations

from typing import cast

from ...accounts.status import SessionStatus
from ...async_provider import AsyncProvider
from ...base import Capability
from ...provider import Provider, ProviderType
from ...resources.chat.chat import Chat, MovableChat
from .adapter import ClaudeAdapter
from .resources import AsyncClaudeChat


class AsyncClaudeProvider(AsyncProvider):
    provider_type = ProviderType.CLAUDE

    _chat_type = AsyncClaudeChat

    capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.TEMPORARY,
            Capability.AUTH,
            Capability.HISTORY,
            Capability.MODELS,
            Capability.EFFORT,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.RESOURCES,
            Capability.PROJECT_DELETE,
            Capability.PROJECT_MEMBERSHIP,
            Capability.DELETE,
            Capability.RENAME,
            Capability.ATTACHMENTS,
            Capability.MESSAGE_EDIT,
            Capability.MESSAGE_RETRY,
        }
    )

    def _create_adapter(self) -> ClaudeAdapter:
        return ClaudeAdapter(
            account=self.account,
            capabilities=self.capabilities,
            transport=self._transport,
        )

    async def forget_session(self) -> SessionStatus:
        await self.close()
        return await cast(ClaudeAdapter, self.adapter).purge_credentials()


class ClaudeChat(MovableChat, Chat):
    pass


class ClaudeProvider(Provider):
    provider_type = ProviderType.CLAUDE
    async_provider_type = AsyncClaudeProvider
    chat_wrapper_type = ClaudeChat
