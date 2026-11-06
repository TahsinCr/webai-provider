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
        self.assertEqual(low.id, "low")  # type: ignore[union-attr]

        med = SelectionResolver.resolve_effort(Efforts.MEDIUM, self.efforts_3)
        self.assertIsNotNone(med)
        self.assertEqual(med.id, "medium")  # type: ignore[union-attr]

        high = SelectionResolver.resolve_effort(Efforts.HIGH, self.efforts_3)
        self.assertIsNotNone(high)
        self.assertEqual(high.id, "high")  # type: ignore[union-attr]

    def test_static_models(self) -> None:
        self.assertEqual(ChatGPTModels.GPT_4O.id, "gpt-4o")
        self.assertEqual(ClaudeModels.CLAUDE_3_5_SONNET.provider, "claude")
        self.assertEqual(GeminiModels.GEMINI_ARGON.id, "gemini-argon")
        self.assertEqual(GrokModels.GROK_3.id, "grok-3")
