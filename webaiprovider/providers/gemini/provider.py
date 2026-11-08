from __future__ import annotations

from ...async_provider import AsyncProvider
from ...provider import Provider, SyncChat
from ...provider_type import ProviderType
from ...resources.chat import BranchableChat
from .resources import AsyncGeminiChat


class AsyncGeminiProvider(AsyncProvider):
    provider_type = ProviderType.GEMINI
    _chat_type = AsyncGeminiChat


class GeminiChat(BranchableChat, SyncChat):
    pass


class GeminiProvider(Provider):
    provider_type = ProviderType.GEMINI
    async_provider_type = AsyncGeminiProvider
    chat_wrapper_type = GeminiChat
