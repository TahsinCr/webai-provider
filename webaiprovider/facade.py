from __future__ import annotations

from .accounts.account import Account
from .provider_type import ProviderType
from .providers.chatgpt.collections import ChatGPTProjectsCollection
from .session import Provider


class ChatGPTProvider(Provider):
    def __init__(self, account: Account) -> None:
        super().__init__(account, type=ProviderType.CHATGPT)
        self.projects = ChatGPTProjectsCollection(self)


class ClaudeProvider(Provider):
    def __init__(self, account: Account) -> None:
        super().__init__(account, type=ProviderType.CLAUDE)


class GeminiProvider(Provider):
    def __init__(self, account: Account) -> None:
        super().__init__(account, type=ProviderType.GEMINI)


class GrokProvider(Provider):
    def __init__(self, account: Account) -> None:
        super().__init__(account, type=ProviderType.GROK)
