"""Consumer-side type contracts checked with Pyright."""

from typing_extensions import assert_type

from webaiprovider import (
    Account,
    AsyncChat,
    AsyncChatGPTChat,
    AsyncChatGPTProvider,
    AsyncProvider,
    Chat,
    ChatGPTChat,
    ChatGPTProvider,
    Provider,
)
from webaiprovider.providers.chatgpt import AsyncChatGPTProject, ChatGPTProject
from webaiprovider.providers.claude import (
    AsyncClaudeChat,
    AsyncClaudeProvider,
    ClaudeChat,
    ClaudeProvider,
)
from webaiprovider.providers.gemini import (
    AsyncGeminiChat,
    AsyncGeminiProvider,
    GeminiChat,
    GeminiProvider,
)
from webaiprovider.providers.grok import AsyncGrokChat, AsyncGrokProvider, GrokChat, GrokProvider


def check(account: Account) -> None:
    p = ChatGPTProvider(account)
    assert_type(p.chats.get("test"), ChatGPTChat)
    assert_type(p.chats.list(), list[ChatGPTChat])
    assert_type(p.projects.create("name", memory_share=False), ChatGPTProject)
    assert_type(Provider(account).chats.get("test"), Chat)
    with ChatGPTProvider(account) as opened:
        assert_type(opened.chats.get("test"), ChatGPTChat)
    assert_type(ClaudeProvider(account).chats.get("test"), ClaudeChat)
    assert_type(GeminiProvider(account).chats.get("test"), GeminiChat)
    assert_type(GrokProvider(account).chats.get("test"), GrokChat)


async def check_async(account: Account) -> None:
    p = AsyncChatGPTProvider(account)
    assert_type(await p.chats.get("test"), AsyncChatGPTChat)
    assert_type(await p.chats.list(), list[AsyncChatGPTChat])
    assert_type(await p.projects.create("name", memory_share=False), AsyncChatGPTProject)
    assert_type(await AsyncProvider(account).chats.get("test"), AsyncChat)
    async with AsyncChatGPTProvider(account) as opened:
        assert_type(await opened.chats.get("test"), AsyncChatGPTChat)
    assert_type(await AsyncClaudeProvider(account).chats.get("test"), AsyncClaudeChat)
    assert_type(await AsyncGeminiProvider(account).chats.get("test"), AsyncGeminiChat)
    assert_type(await AsyncGrokProvider(account).chats.get("test"), AsyncGrokChat)


def use_provider(provider: Provider) -> Chat:
    return provider.chats.get("test")


async def use_async_provider(provider: AsyncProvider) -> AsyncChat:
    return await provider.chats.get("test")


async def check_shared_contract() -> None:
    account = Account.from_cookies("chatgpt", {}, id="account-1")
    assert_type(account.id, str)
    assert_type(use_provider(ChatGPTProvider(account)), Chat)
    assert_type(await use_async_provider(AsyncChatGPTProvider(account)), AsyncChat)
