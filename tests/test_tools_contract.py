from __future__ import annotations

import unittest
from dataclasses import dataclass
from typing import ClassVar
from unittest.mock import patch

import webaiprovider
from webaiprovider import (
    AsyncGeminiWebAIProvider,
    AsyncWebAIProvider,
    Capability,
    ClaudeWebAIProvider,
    DeepResearchTool,
    FeatureUnavailable,
    GeminiWebAIProvider,
    ImageGenerationTool,
    MemoryCredentialStore,
    Tool,
    UnsupportedOperationError,
    WebAIAccount,
    WebAIProvider,
    WebSearchTool,
)
from webaiprovider.core.types import OperationContext
from webaiprovider.resources.message.request import (
    async_build_message_request_pipeline,
    async_run_pipeline,
    build_message_request_pipeline,
    run_pipeline,
)


@dataclass(frozen=True, slots=True)
class _DummyGeminiOnlyTool(Tool):
    capability: ClassVar[Capability | None] = Capability.DEEP_RESEARCH
    provider_type: ClassVar[str | None] = "gemini"


def _dummy_account(provider: str) -> WebAIAccount:
    return WebAIAccount(
        id=f"test-{provider}",
        provider=provider,
        credentials=MemoryCredentialStore({"session": "ok"}),
    )


class ToolsContractTests(unittest.TestCase):
    def test_root_exports_and_capabilities(self) -> None:
        self.assertIs(webaiprovider.Tool, Tool)
        self.assertIs(webaiprovider.WebSearchTool, WebSearchTool)
        self.assertIs(webaiprovider.ImageGenerationTool, ImageGenerationTool)
        self.assertIs(webaiprovider.DeepResearchTool, DeepResearchTool)
        self.assertEqual(Capability.WEB_SEARCH.value, "web_search")
        self.assertEqual(Capability.IMAGE_GENERATION.value, "image_generation")
        self.assertEqual(Capability.DEEP_RESEARCH.value, "deep_research")

    def test_tool_defaults_and_validation(self) -> None:
        self.assertFalse(DeepResearchTool().auto_start)
        self.assertTrue(DeepResearchTool(auto_start=True).auto_start)
        with self.assertRaises(TypeError):
            DeepResearchTool(auto_start="yes")  # type: ignore[arg-type]

    def test_generic_provider_rejects_provider_specific_tool(self) -> None:
        account = _dummy_account("gemini")
        generic = WebAIProvider(account)
        concrete = GeminiWebAIProvider(account)
        self.assertTrue(generic._constructed_via_generic)
        self.assertFalse(concrete._constructed_via_generic)

        tool = _DummyGeminiOnlyTool()
        with self.assertRaises(UnsupportedOperationError):
            generic._validate_tool(tool)

        with patch.object(
            concrete,
            "_capabilities",
            concrete.capabilities | frozenset({Capability.DEEP_RESEARCH}),
        ):
            concrete._validate_tool(tool)

        chatgpt_concrete = WebAIProvider(_dummy_account("chatgpt"))
        chatgpt_concrete._constructed_via_generic = False
        with self.assertRaises(UnsupportedOperationError):
            chatgpt_concrete._validate_tool(tool)

    def test_unsupported_capability_fails_before_network(self) -> None:
        account = _dummy_account("claude")
        provider = ClaudeWebAIProvider(account)
        with self.assertRaises(FeatureUnavailable):
            provider.ask("draw a cat", tool=ImageGenerationTool())
        chat = provider.chats.create()
        with self.assertRaises(FeatureUnavailable):
            chat.responses.create("research", tool=DeepResearchTool())

    def test_invalid_tool_and_timeout_rejected(self) -> None:
        provider = WebAIProvider(_dummy_account("chatgpt"))
        with self.assertRaises(TypeError):
            provider._validate_tool("search")
        for bad_timeout in (0, -1, float("nan"), float("inf"), True):
            with self.assertRaises(ValueError):
                provider._validate_tool(WebSearchTool(), tool_timeout=bad_timeout)

    def test_pipeline_forwards_tool_and_timeout_to_provider_request(self) -> None:
        provider = WebAIProvider(_dummy_account("chatgpt"))
        with patch.object(
            provider,
            "_capabilities",
            provider.capabilities | frozenset({Capability.WEB_SEARCH}),
        ):
            chat = provider.chats.create()
            op = chat.responses.create("latest news", tool=WebSearchTool(), tool_timeout=45.0)
            ctx = OperationContext(
                name="message.send",
                operation_id="op-1",
                provider_id="chatgpt",
                account_id=provider.account_id,
                parameters={"chat": chat, "message": op.message},
            )
            pipeline = build_message_request_pipeline(
                provider_id="chatgpt",
                account_id=provider.account_id,
                model_resolver=lambda c: c,
            )
            result = run_pipeline(pipeline, ctx)
            self.assertIsNotNone(result.request)
            self.assertEqual(result.request.tool, WebSearchTool())
            self.assertEqual(result.request.tool_timeout, 45.0)


class AsyncToolsContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_generic_vs_concrete_and_pipeline(self) -> None:
        account = _dummy_account("gemini")
        generic = AsyncWebAIProvider(account)
        concrete = AsyncGeminiWebAIProvider(account)
        self.assertTrue(generic._constructed_via_generic)
        self.assertFalse(concrete._constructed_via_generic)

        tool = _DummyGeminiOnlyTool()
        with self.assertRaises(UnsupportedOperationError):
            generic._validate_tool(tool)

        with patch.object(
            concrete,
            "_capabilities",
            concrete.capabilities | frozenset({Capability.DEEP_RESEARCH}),
        ):
            concrete._validate_tool(tool)
            chat = await concrete.chats.create()
            op = chat.responses.create("deep dive", tool=tool, tool_timeout=600.0)
            ctx = OperationContext(
                name="message.send",
                operation_id="op-2",
                provider_id="gemini",
                account_id=concrete.account_id,
                parameters={"chat": chat, "message": op.message},
            )

            async def _noop_resolver(c: object) -> object:
                return c

            pipeline = async_build_message_request_pipeline(
                provider_id="gemini",
                account_id=concrete.account_id,
                model_resolver=_noop_resolver,  # type: ignore[arg-type]
            )
            result = await async_run_pipeline(pipeline, ctx)
            self.assertIsNotNone(result.request)
            self.assertEqual(result.request.tool, tool)
            self.assertEqual(result.request.tool_timeout, 600.0)


if __name__ == "__main__":
    unittest.main()
