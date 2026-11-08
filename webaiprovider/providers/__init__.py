"""Bundled provider backends."""
from __future__ import annotations

from typing import TYPE_CHECKING

from ..errors import UnsupportedOperationError
from ..provider_type import ProviderType

if TYPE_CHECKING:
    from ..accounts.account import Account
    from .base import ProviderAdapter


def create_adapter(account: Account, provider_type: ProviderType) -> ProviderAdapter:
    if provider_type is ProviderType.CHATGPT:
        from .chatgpt.provider import create_adapter as factory
    elif provider_type is ProviderType.GEMINI:
        from .gemini.provider import create_adapter as factory
    elif provider_type is ProviderType.CLAUDE:
        from .claude.provider import create_adapter as factory
    elif provider_type is ProviderType.GROK:
        from .grok.provider import create_adapter as factory
    else:
        raise UnsupportedOperationError(f"Unsupported provider: {provider_type}")
    return factory(account)
