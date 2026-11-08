from __future__ import annotations

from typing import TYPE_CHECKING

from ...async_provider import AsyncProvider
from ...provider import Provider, ProviderType, SyncChat
from ...resources.chat import BranchableChat
from .resources import AsyncGeminiChat

if TYPE_CHECKING:
    from ...accounts.account import Account
    from ..base import ProviderAdapter


def create_adapter(account: Account) -> ProviderAdapter:
    from .adapter import GeminiAdapter

    return GeminiAdapter(account.store(), account_id=account.id)


class AsyncGeminiProvider(AsyncProvider):
    provider_type = ProviderType.GEMINI
    _chat_type = AsyncGeminiChat


class GeminiChat(BranchableChat, SyncChat):
    pass


class GeminiProvider(Provider):
    provider_type = ProviderType.GEMINI
    async_provider_type = AsyncGeminiProvider
    chat_wrapper_type = GeminiChat
