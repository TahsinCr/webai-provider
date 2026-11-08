from __future__ import annotations

from typing import TYPE_CHECKING

from ...async_provider import AsyncProvider
from ...provider import Provider, ProviderType, SyncChat
from ...resources.chat import MovableChat
from .resources import AsyncClaudeChat

if TYPE_CHECKING:
    from ...accounts.account import Account
    from ..base import ProviderAdapter


def create_adapter(account: Account) -> ProviderAdapter:
    from .adapter import ClaudeAdapter

    return ClaudeAdapter(account.store(), account_id=account.id)


class AsyncClaudeProvider(AsyncProvider):
    provider_type = ProviderType.CLAUDE
    _chat_type = AsyncClaudeChat


class ClaudeChat(MovableChat, SyncChat):
    pass


class ClaudeProvider(Provider):
    provider_type = ProviderType.CLAUDE
    async_provider_type = AsyncClaudeProvider
    chat_wrapper_type = ClaudeChat
