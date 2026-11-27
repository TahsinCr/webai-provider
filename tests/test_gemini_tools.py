from __future__ import annotations

import base64
import unittest
from types import SimpleNamespace

from webaiprovider import (
    AsyncGeminiWebAIProvider,
    AsyncWebAIProvider,
    DeepResearchTool,
    GeminiWebAIProvider,
    ImageGenerationTool,
    MemoryCredentialStore,
    UnsupportedOperationError,
    WebAIAccount,
    WebAIProvider,
    WebSearchTool,
)
from webaiprovider.providers.gemini import DeepResearchSources, GeminiDeepResearchTool
from webaiprovider.providers.gemini.deep_research import (
    build_start_research_fields,
    gemini_tool_fields,
    parse_gemini_research_hnvqhb,
    parse_gemini_research_plan,
)
from webaiprovider.providers.gemini.image import (
    GEMINI_IMAGE_TOOL_FIELDS,
    extract_proto_image_files,
    strip_image_placeholders,
)
from webaiprovider.providers.gemini.proto import ProtoView, encode_message
from webaiprovider.providers.gemini.webchannel import build_process_session_turn_data
from webaiprovider.resources.message.protocol import ProviderRequest


def _gemini_account() -> WebAIAccount:
    return WebAIAccount(
        id="gemini-test",
        provider="gemini",
        credentials=MemoryCredentialStore({"__Secure-1PSID": "sid"}),
    )


class GeminiToolsTests(unittest.TestCase):
    def test_generic_provider_rejects_gemini_deep_research_tool(self) -> None:
        account = _gemini_account()
        generic = WebAIProvider(account)
        concrete = GeminiWebAIProvider(account)

        with self.assertRaises(UnsupportedOperationError):
            generic._validate_tool(GeminiDeepResearchTool())
        concrete._validate_tool(GeminiDeepResearchTool(sources=DeepResearchSources.CHAT))
        concrete._validate_tool(DeepResearchTool())
        concrete._validate_tool(ImageGenerationTool())
        concrete._validate_tool(WebSearchTool())

    def test_tool_protobuf_field_mappings(self) -> None:
        self.assertEqual(gemini_tool_fields(WebSearchTool()), {50: 13})
        self.assertEqual(gemini_tool_fields(ImageGenerationTool()), GEMINI_IMAGE_TOOL_FIELDS)
        self.assertEqual(
            gemini_tool_fields(GeminiDeepResearchTool(sources=DeepResearchSources.WEB)),
            {50: 1, 55: [1], 56: [1]},
        )
        self.assertEqual(
            gemini_tool_fields(GeminiDeepResearchTool(sources=DeepResearchSources.CHAT)),
            {50: 1, 55: [12]},
        )
        self.assertEqual(
            gemini_tool_fields(GeminiDeepResearchTool(sources=DeepResearchSources.WEB_AND_CHAT)),
            {50: 1, 55: [1, 7], 56: [1, 12]},
        )
        # Generic DeepResearchTool defaults to WEB_AND_CHAT on Gemini.
        self.assertEqual(
            gemini_tool_fields(DeepResearchTool()),
            {50: 1, 55: [1, 7], 56: [1, 12]},
        )

    def test_image_generation_turn_and_placeholder_stripping(self) -> None:
        model = SimpleNamespace(
            id="gemini-neon",
            efforts=(SimpleNamespace(id="standard"),),
            default_effort="standard",
            metadata={"model_id": "neon", "number": 3, "effort_values": {"standard": 1}},
        )
        session = SimpleNamespace(models=(model,), _cookies={})
        _, form = build_process_session_turn_data(
            session,
            ProviderRequest("draw a cat", tool=ImageGenerationTool()),
            [],
            [],
            tool_fields=gemini_tool_fields(ImageGenerationTool()),
        )
        req0 = ProtoView(base64.b64decode(form["req0___data__"])).at(1, 8, 5, 1)
        assert req0 is not None
        self.assertEqual(req0.get_bytes(18), bytes.fromhex("0a020802"))
        self.assertEqual(req0.get_int(50), 0)

        self.assertEqual(
            strip_image_placeholders("http://googleusercontent.com/image_generation_content/0"),
            "",
        )
        pkt = encode_message(
            {2: {7: {1: {4: {2: "https://lh3.googleusercontent.com/gg/image123"}}}}}
        )
        files = extract_proto_image_files(ProtoView(pkt))
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]["media_type"], "image/jpeg")

    def test_deep_research_plan_start_turn_and_hnvqhb_parsing(self) -> None:
        plan_state = parse_gemini_research_plan(
            "# AI Research\n1. Gather papers\n2. Compare benchmarks",
            conversation_id="c_dr",
            turn_token="tok_dr",
        )
        assert plan_state.plan is not None
        self.assertEqual(plan_state.plan.title, "AI Research")
        self.assertEqual(plan_state.plan.steps, ("Gather papers", "Compare benchmarks"))

        fields = build_start_research_fields("c_dr", "tok_dr")
        self.assertEqual(fields[1], "Araştırmayı başlatın")
        self.assertEqual(fields[80], 3)
        self.assertEqual(fields[98], bytes.fromhex("08001001"))

        turns = [
            [
                ["agency-placeholder-task-id", "Searching official docs"],
                {"url": "https://example.com/a", "title": "Doc A", "cited": True},
                [
                    "im_1",
                    None,
                    "AI Report",
                    "agency-placeholder-task-id",
                    "# AI Report\n\nDetails.",
                ],
            ]
        ]
        parsed = parse_gemini_research_hnvqhb(turns)
        assert parsed is not None
        self.assertEqual(parsed["title"], "AI Report")
        self.assertIn("# AI Report", parsed["markdown"])
        self.assertEqual(parsed["thoughts"], "Searching official docs")
        self.assertEqual(parsed["sources"][0]["url"], "https://example.com/a")


class AsyncGeminiToolsTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_generic_rejects_gemini_tool(self) -> None:
        account = _gemini_account()
        generic = AsyncWebAIProvider(account)
        concrete = AsyncGeminiWebAIProvider(account)
        with self.assertRaises(UnsupportedOperationError):
            generic._validate_tool(GeminiDeepResearchTool())
        concrete._validate_tool(GeminiDeepResearchTool(sources=DeepResearchSources.WEB))


if __name__ == "__main__":
    unittest.main()
