from __future__ import annotations

from ...async_provider import AsyncProvider
from ...provider import Provider, SyncChat
from ...provider_type import ProviderType
from ...resources.chat import MovableChat
from .resources import AsyncClaudeChat


class AsyncClaudeProvider(AsyncProvider):
    provider_type = ProviderType.CLAUDE
    _chat_type = AsyncClaudeChat


class ClaudeChat(MovableChat, SyncChat):
    pass


class ClaudeProvider(Provider):
    provider_type = ProviderType.CLAUDE
    async_provider_type = AsyncClaudeProvider
    chat_wrapper_type = ClaudeChat
