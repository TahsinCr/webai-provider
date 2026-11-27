from __future__ import annotations

import unittest

from webaiprovider import (
    AsyncClaudeWebAIProvider,
    AsyncGrokWebAIProvider,
    AsyncWebAIProvider,
    Capability,
    ClaudeWebAIProvider,
    DeepResearchTool,
    FeatureUnavailable,
    GrokWebAIProvider,
    ImageGenerationTool,
    MemoryCredentialStore,
    UnsupportedOperationError,
    WebAIAccount,
    WebAIProvider,
    WebSearchTool,
)
from webaiprovider.providers.gemini import GeminiDeepResearchTool
from webaiprovider.providers.grok.image import apply_grok_tool_payload


def _account(provider: str) -> WebAIAccount:
    return WebAIAccount(
        id=f"test-{provider}",
        provider=provider,
        credentials=MemoryCredentialStore({"session": "ok"}),
    )


class GrokAndClaudeToolsTests(unittest.TestCase):
    def test_grok_supports_web_search_and_image_but_rejects_deep_research(self) -> None:
        grok = GrokWebAIProvider(_account("grok"))
        self.assertIn(Capability.WEB_SEARCH, grok.capabilities)
        self.assertIn(Capability.IMAGE_GENERATION, grok.capabilities)
        self.assertNotIn(Capability.DEEP_RESEARCH, grok.capabilities)

        grok._validate_tool(WebSearchTool())
        grok._validate_tool(ImageGenerationTool())
        with self.assertRaises(FeatureUnavailable):
            grok.ask("research", tool=DeepResearchTool())
        with self.assertRaises(UnsupportedOperationError):
            grok._validate_tool(GeminiDeepResearchTool())

        generic_grok = WebAIProvider(_account("grok"))
        with self.assertRaises(FeatureUnavailable):
            generic_grok.ask("research", tool=DeepResearchTool())

        body: dict[str, object] = {"disableSearch": True}
        apply_grok_tool_payload(body, WebSearchTool())
        self.assertFalse(body["disableSearch"])
        apply_grok_tool_payload(body, ImageGenerationTool())
        self.assertTrue(body["enableImageGeneration"])

    def test_claude_rejects_unsupported_tools_before_network(self) -> None:
        claude = ClaudeWebAIProvider(_account("claude"))
        generic_claude = WebAIProvider(_account("claude"))
        for target in (claude, generic_claude):
            for tool in (WebSearchTool(), ImageGenerationTool(), DeepResearchTool()):
                with self.assertRaises(FeatureUnavailable):
                    target.ask("hello", tool=tool)
            with self.assertRaises(UnsupportedOperationError):
                target._validate_tool(GeminiDeepResearchTool())


class AsyncGrokAndClaudeToolsTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_grok_and_claude_validation(self) -> None:
        grok = AsyncGrokWebAIProvider(_account("grok"))
        grok._validate_tool(WebSearchTool())
        grok._validate_tool(ImageGenerationTool())
        with self.assertRaises(FeatureUnavailable):
            await grok.ask("research", tool=DeepResearchTool())

        claude = AsyncClaudeWebAIProvider(_account("claude"))
        generic_claude = AsyncWebAIProvider(_account("claude"))
        for target in (claude, generic_claude):
            with self.assertRaises(FeatureUnavailable):
                await target.ask("draw", tool=ImageGenerationTool())
            with self.assertRaises(UnsupportedOperationError):
                target._validate_tool(GeminiDeepResearchTool())


if __name__ == "__main__":
    unittest.main()
