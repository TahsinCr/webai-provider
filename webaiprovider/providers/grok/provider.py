from __future__ import annotations

from ...async_provider import AsyncProvider
from ...provider import Provider, SyncChat
from ...provider_type import ProviderType
from ...resources.chat import MovableChat
from .resources import AsyncGrokChat


class AsyncGrokProvider(AsyncProvider):
    provider_type = ProviderType.GROK
    _chat_type = AsyncGrokChat


class GrokChat(MovableChat, SyncChat):
    pass


class GrokProvider(Provider):
    provider_type = ProviderType.GROK
    async_provider_type = AsyncGrokProvider
    chat_wrapper_type = GrokChat
