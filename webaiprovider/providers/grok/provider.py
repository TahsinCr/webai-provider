from __future__ import annotations

from typing import TYPE_CHECKING

from ...async_provider import AsyncProvider
from ...provider import Provider, ProviderType, SyncChat
from ...resources.chat import MovableChat
from .resources import AsyncGrokChat

if TYPE_CHECKING:
    from ...accounts.account import Account
    from ..base import ProviderAdapter


def create_adapter(account: Account) -> ProviderAdapter:
    from .adapter import GrokAdapter

    return GrokAdapter(account.store(), account_id=account.id)


class AsyncGrokProvider(AsyncProvider):
    provider_type = ProviderType.GROK
    _chat_type = AsyncGrokChat


class GrokChat(MovableChat, SyncChat):
    pass


class GrokProvider(Provider):
    provider_type = ProviderType.GROK
    async_provider_type = AsyncGrokProvider
    chat_wrapper_type = GrokChat
