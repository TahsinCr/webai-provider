"""Consumer-side type contracts checked with Pyright."""

from typing_extensions import assert_type

from webaiprovider import (
    Artifact,
    AsyncChat,
    AsyncChatGPTChat,
    AsyncChatGPTWebAIProvider,
    AsyncWebAIProvider,
    Chat,
    ChatGPTChat,
    ChatGPTWebAIProvider,
    DeepResearchTool,
    ImageGenerationTool,
    ResearchState,
    Response,
    WebAIAccount,
    WebAIProvider,
    WebSearchTool,
)
from webaiprovider.providers.chatgpt import AsyncChatGPTProject, ChatGPTProject
from webaiprovider.providers.claude import (
    AsyncClaudeChat,
    AsyncClaudeWebAIProvider,
    ClaudeChat,
    ClaudeWebAIProvider,
)
from webaiprovider.providers.gemini import (
    AsyncGeminiChat,
    AsyncGeminiWebAIProvider,
    DeepResearchSources,
    GeminiChat,
    GeminiDeepResearchTool,
    GeminiWebAIProvider,
)
from webaiprovider.providers.grok import (
    AsyncGrokChat,
    AsyncGrokWebAIProvider,
    GrokChat,
    GrokWebAIProvider,
)


def check(account: WebAIAccount) -> None:
    p = ChatGPTWebAIProvider(account)
    assert_type(p.chats.get("test"), ChatGPTChat)
    assert_type(p.chats.get("test").move_to(None), ChatGPTChat)
    assert_type(p.chats.list(), list[ChatGPTChat])
    assert_type(p.projects.create("name", memory_share=False), ChatGPTProject)
    assert_type(WebAIProvider(account).chats.get("test"), Chat)
    with ChatGPTWebAIProvider(account) as opened:
        assert_type(opened.chats.get("test"), ChatGPTChat)
    assert_type(ClaudeWebAIProvider(account).chats.get("test"), ClaudeChat)
    assert_type(GeminiWebAIProvider(account).chats.get("test"), GeminiChat)
    assert_type(GrokWebAIProvider(account).chats.get("test"), GrokChat)


async def check_async(account: WebAIAccount) -> None:
    p = AsyncChatGPTWebAIProvider(account)
    chat = await p.chats.get("test")
    assert_type(chat, AsyncChatGPTChat)
    assert_type(await chat.move_to(None), AsyncChatGPTChat)
    assert_type(await p.chats.list(), list[AsyncChatGPTChat])
    assert_type(await p.projects.create("name", memory_share=False), AsyncChatGPTProject)
    assert_type(await AsyncWebAIProvider(account).chats.get("test"), AsyncChat)
    async with AsyncChatGPTWebAIProvider(account) as opened:
        assert_type(await opened.chats.get("test"), AsyncChatGPTChat)
    assert_type(await AsyncClaudeWebAIProvider(account).chats.get("test"), AsyncClaudeChat)
    assert_type(await AsyncGeminiWebAIProvider(account).chats.get("test"), AsyncGeminiChat)
    assert_type(await AsyncGrokWebAIProvider(account).chats.get("test"), AsyncGrokChat)


def use_provider(provider: WebAIProvider) -> Chat:
    return provider.chats.get("test")


async def use_async_provider(provider: AsyncWebAIProvider) -> AsyncChat:
    return await provider.chats.get("test")


async def check_shared_contract() -> None:
    account = WebAIAccount.from_cookies("chatgpt", {}, id="account-1")
    assert_type(account.id, str)
    assert_type(use_provider(ChatGPTWebAIProvider(account)), Chat)
    assert_type(await use_async_provider(AsyncChatGPTWebAIProvider(account)), AsyncChat)
    assert_type(
        ChatGPTWebAIProvider(account).chats.create().ask("hi", tool=WebSearchTool()),
        Response,
    )
    assert_type(
        GeminiDeepResearchTool(sources=DeepResearchSources.WEB_AND_CHAT, auto_start=False),
        GeminiDeepResearchTool,
    )
    _dr = DeepResearchTool(auto_start=True)
    _img = ImageGenerationTool()
    _state: ResearchState | None = Response("ok").research
    _art: list[Artifact] = list(Response("ok").artifacts)
    del _dr, _img, _state, _art
