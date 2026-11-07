from __future__ import annotations

import unittest

from webai_provider.catalogs import (
    ChatGPTModels,
    ClaudeModels,
    Efforts,
    GeminiModels,
    GrokModels,
    Models,
    SelectionResolver,
    StaticModelCatalog,
)
from webai_provider.contracts import EffortRef, ModelRef
from webai_provider.errors import AmbiguousMatchError, RemoteNotFound


class CatalogsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.models_5 = [
            ModelRef("chatgpt", "gpt-3.5", "GPT-3.5"),
            ModelRef("chatgpt", "gpt-4o-mini", "GPT-4o Mini"),
            ModelRef("chatgpt", "gpt-4o", "GPT-4o"),
            ModelRef("chatgpt", "o1-mini", "o1 Mini"),
            ModelRef("chatgpt", "o1", "o1"),
        ]
        self.efforts_3 = [
            EffortRef("chatgpt", "low", "Low", value=1),
            EffortRef("chatgpt", "medium", "Medium", value=4),
            EffortRef("chatgpt", "high", "High", value=8),
        ]

    def test_mathematical_medium_and_bounds_selection(self) -> None:
        low = SelectionResolver.resolve_model(Models.LOW, self.models_5)
        self.assertEqual(low.id, "gpt-3.5")

        high = SelectionResolver.resolve_model(Models.HIGH, self.models_5)
        self.assertEqual(high.id, "o1")

        med = SelectionResolver.resolve_model(Models.MEDIUM, self.models_5)
        self.assertEqual(med.id, "gpt-4o")

    def test_even_count_medium_formula(self) -> None:
        models_4 = self.models_5[:4]
        med = SelectionResolver.resolve_model(Models.MEDIUM, models_4)
        self.assertEqual(med.id, "gpt-4o")

    def test_auto_policy_with_default_or_fallback(self) -> None:
        auto_default = SelectionResolver.resolve_model(
            Models.AUTO, self.models_5, default_model_id="o1-mini"
        )
        self.assertEqual(auto_default.id, "o1-mini")

        auto_fallback = SelectionResolver.resolve_model(Models.AUTO, self.models_5)
        self.assertEqual(auto_fallback.id, "gpt-4o")

    def test_name_and_partial_match(self) -> None:
        resolved = SelectionResolver.resolve_model("4o Mini", self.models_5)
        self.assertEqual(resolved.id, "gpt-4o-mini")

    def test_ambiguous_match_error(self) -> None:
        with self.assertRaises(AmbiguousMatchError):
            SelectionResolver.resolve_model("gpt", self.models_5)

    def test_remote_not_found(self) -> None:
        with self.assertRaises(RemoteNotFound):
            SelectionResolver.resolve_model("nonexistent_model", self.models_5)

    def test_effort_resolution(self) -> None:
        low = SelectionResolver.resolve_effort(Efforts.LOW, self.efforts_3)
        self.assertIsNotNone(low)
        self.assertEqual(low.id, "low")

        med = SelectionResolver.resolve_effort(Efforts.MEDIUM, self.efforts_3)
        self.assertIsNotNone(med)
        self.assertEqual(med.id, "medium")

        high = SelectionResolver.resolve_effort(Efforts.HIGH, self.efforts_3)
        self.assertIsNotNone(high)
        self.assertEqual(high.id, "high")

    def test_static_models_and_updated_catalogs(self) -> None:
        self.assertEqual(ChatGPTModels.GPT_4O.id, "gpt-4o")
        self.assertEqual(ChatGPTModels.O3_MINI.id, "o3-mini")
        self.assertEqual(ChatGPTModels.GPT_4_5.id, "gpt-4.5-preview")

        self.assertEqual(ClaudeModels.CLAUDE_3_7_SONNET.id, "claude-3-7-sonnet")
        self.assertEqual(ClaudeModels.CLAUDE_3_5_SONNET.provider, "claude")

        self.assertEqual(GeminiModels.GEMINI_2_5_PRO.id, "gemini-2.5-pro")
        self.assertEqual(GeminiModels.GEMINI_2_0_FLASH_THINKING.id, "gemini-2.0-flash-thinking")

        self.assertEqual(GrokModels.GROK_3.id, "grok-3")
        self.assertEqual(GrokModels.GROK_3_THINK.id, "grok-3-think")

    def test_static_catalog_lookup_methods(self) -> None:
        models = ChatGPTModels.list()
        self.assertGreater(len(models), 5)
        self.assertTrue(all(isinstance(m, ModelRef) for m in models))
        self.assertTrue(all(m.provider == "chatgpt" for m in models))

        gpt4o = ChatGPTModels.get("gpt-4o")
        self.assertEqual(gpt4o.id, "gpt-4o")
        self.assertEqual(ChatGPTModels["gpt-4o"].id, "gpt-4o")

        self.assertTrue(ChatGPTModels.contains("gpt-4o"))
        self.assertTrue(ChatGPTModels.contains(ChatGPTModels.GPT_4O))
        self.assertFalse(ChatGPTModels.contains("unknown-model"))

        found = ChatGPTModels.find("o3-mini")
        self.assertEqual(found.id, "o3-mini")

        search_results = ChatGPTModels.search("mini")
        self.assertGreaterEqual(len(search_results), 2)

        with self.assertRaises(RemoteNotFound):
            ChatGPTModels.get("unknown-model")

    def test_catalog_subclasses_static_catalog_base(self) -> None:
        for cat in (ChatGPTModels, ClaudeModels, GeminiModels, GrokModels):
            self.assertTrue(issubclass(cat, StaticModelCatalog))
            self.assertFalse(issubclass(cat, Models))
            self.assertGreater(len(cat.list()), 3)
