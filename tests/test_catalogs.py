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
            ModelRef("chatgpt", "model-1", "Model 1"),
            ModelRef("chatgpt", "model-2-mini", "Model 2 Mini"),
            ModelRef("chatgpt", "model-3", "Model 3"),
            ModelRef("chatgpt", "model-4-mini", "Model 4 Mini"),
            ModelRef("chatgpt", "model-5", "Model 5"),
        ]
        self.efforts_3 = [
            EffortRef("chatgpt", "low", "Low", value=1),
            EffortRef("chatgpt", "medium", "Medium", value=4),
            EffortRef("chatgpt", "high", "High", value=8),
        ]

    def test_mathematical_medium_and_bounds_selection(self) -> None:
        low = SelectionResolver.resolve_model(Models.LOW, self.models_5)
        self.assertEqual(low.id, "model-1")

        high = SelectionResolver.resolve_model(Models.HIGH, self.models_5)
        self.assertEqual(high.id, "model-5")

        med = SelectionResolver.resolve_model(Models.MEDIUM, self.models_5)
        self.assertEqual(med.id, "model-3")

    def test_even_count_medium_formula(self) -> None:
        models_4 = self.models_5[:4]
        med = SelectionResolver.resolve_model(Models.MEDIUM, models_4)
        self.assertEqual(med.id, "model-3")

    def test_auto_policy_with_default_or_fallback(self) -> None:
        auto_default = SelectionResolver.resolve_model(
            Models.AUTO, self.models_5, default_model_id="model-4-mini"
        )
        self.assertEqual(auto_default.id, "model-4-mini")

        auto_fallback = SelectionResolver.resolve_model(Models.AUTO, self.models_5)
        self.assertEqual(auto_fallback.id, "model-3")

    def test_name_and_partial_match(self) -> None:
        resolved = SelectionResolver.resolve_model("2 Mini", self.models_5)
        self.assertEqual(resolved.id, "model-2-mini")

    def test_ambiguous_match_error(self) -> None:
        with self.assertRaises(AmbiguousMatchError):
            SelectionResolver.resolve_model("mini", self.models_5)

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
        catalogs = (
            (
                ChatGPTModels,
                {
                    "gpt-5-6-instant": (),
                    "gpt-5-6-thinking": ("standard", "extended"),
                    "gpt-5-6-t-mini": (),
                    "gpt-6": (),
                    "gpt-6-instant": (),
                    "gpt-6-thinking": ("standard", "extended"),
                    "gpt-6-t-mini": (),
                },
            ),
            (
                ClaudeModels,
                {
                    "claude-sonnet-5-5": ("low", "medium", "high", "xhigh", "max"),
                    "claude-haiku-5-5": ("low", "medium", "high", "xhigh", "max"),
                    "claude-sonnet-5": (
                        "low:auto",
                        "low:off",
                        "medium:auto",
                        "medium:off",
                        "high:auto",
                        "high:off",
                        "xhigh:auto",
                        "xhigh:off",
                        "max:auto",
                        "max:off",
                    ),
                    "claude-sonnet-4-6": (
                        "low:auto",
                        "low:off",
                        "medium:auto",
                        "medium:off",
                        "high:auto",
                        "high:off",
                        "max:auto",
                        "max:off",
                    ),
                },
            ),
            (
                GeminiModels,
                {
                    "gemini-flash-lite": ("standard", "medium", "extended"),
                    "gemini-flash": ("standard", "medium", "extended"),
                    "gemini-pro": ("standard", "medium", "extended"),
                    "gemini-argon": ("standard", "medium", "extended"),
                },
            ),
            (GrokModels, {"auto": (), "fast": (), "expert": (), "heavy": ()}),
        )
        for catalog, expected in catalogs:
            self.assertEqual({model.id: model.capabilities for model in catalog.list()}, expected)
            self.assertTrue(all(model.provider == catalog.provider for model in catalog.list()))

        self.assertEqual(GrokModels.GROK_3.name, "Grok 3")
        self.assertEqual(GrokModels.GROK_3.id, "fast")
        self.assertEqual(GrokModels.GROK_3.metadata["modelName"], "grok-3")
        self.assertEqual(GrokModels.GROK_4_7_EXPERT.name, "Grok 4.7 Expert")
        self.assertEqual(GrokModels.GROK_4_7_HEAVY.name, "Grok 4.7 Heavy")

    def test_static_catalog_lookup_methods(self) -> None:
        models = ChatGPTModels.list()
        self.assertGreaterEqual(len(models), 7)
        self.assertTrue(all(isinstance(m, ModelRef) for m in models))
        self.assertTrue(all(m.provider == "chatgpt" for m in models))

        thinking = ChatGPTModels.get("gpt-6-thinking")
        self.assertEqual(thinking.id, "gpt-6-thinking")
        self.assertEqual(ChatGPTModels["gpt-6-thinking"].id, "gpt-6-thinking")

        self.assertTrue(ChatGPTModels.contains("gpt-6-thinking"))
        self.assertTrue(ChatGPTModels.contains(ChatGPTModels.GPT_6_THINKING))
        self.assertFalse(ChatGPTModels.contains("unknown-model"))

        found = ChatGPTModels.find("gpt-6-thinking")
        self.assertEqual(found.id, "gpt-6-thinking")

        search_results = ChatGPTModels.search("thinking")
        self.assertGreaterEqual(len(search_results), 4)

        with self.assertRaises(RemoteNotFound):
            ChatGPTModels.get("unknown-model")

    def test_catalog_subclasses_static_catalog_base(self) -> None:
        for cat in (ChatGPTModels, ClaudeModels, GeminiModels, GrokModels):
            self.assertTrue(issubclass(cat, StaticModelCatalog))
            self.assertFalse(issubclass(cat, Models))
            self.assertGreater(len(cat.list()), 3)
