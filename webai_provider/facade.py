from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from .accounts.account import Account
from .catalogs.policies import Models
from .contracts import EffortRef, ModelRef, ModelSelection
from .resources.response import Response

if TYPE_CHECKING:
    from .session import ProviderSession


class BaseProvider:
    provider_id: str = ""

    def __init__(
        self,
        cookies: dict[str, Any] | Path | str | None = None,
        *,
        account: Account | None = None,
        name: str | None = None,
    ) -> None:
        if account is not None:
            self.account = account
        elif cookies is not None:
            self.account = Account.from_cookies(self.provider_id, cookies, name=name)
        else:
            self.account = Account.from_browser(self.provider_id, name=name)

    def connect(self) -> ProviderSession:
        return self.account.connect()

    async def ask(
        self,
        prompt: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Any = None,
    ) -> Response:
        session = self.connect()
        try:
            chat = await session.chats.create()
            op = chat.messages.create(
                prompt,
                model=model,
                attachments=attachments,
            )
            return await op.start()
        finally:
            await session.close()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} account={self.account.id!r}>"


class ChatGPTProvider(BaseProvider):
    provider_id = "chatgpt"


class ClaudeProvider(BaseProvider):
    provider_id = "claude"


class GeminiProvider(BaseProvider):
    provider_id = "gemini"


class GrokProvider(BaseProvider):
    provider_id = "grok"


BaseAsyncProvider = BaseProvider
AsyncChatGPTProvider = ChatGPTProvider
AsyncClaudeProvider = ClaudeProvider
AsyncGeminiProvider = GeminiProvider
AsyncGrokProvider = GrokProvider
