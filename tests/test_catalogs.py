from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

from webaiprovider.accounts import Account
from webaiprovider.bridge import ProviderBridge
from webaiprovider.catalogs import (
    ChatGPTModels,
    ClaudeModels,
    GeminiModels,
    GrokModels,
    Models,
    ModelsCollection,
    SelectionResolver,
    StaticModelCatalog,
)
from webaiprovider.contracts import EffortRef, EffortSet, ModelRef, ModelSelection
from webaiprovider.errors import AmbiguousMatchError, RemoteNotFound
from webaiprovider.session import ProviderSession


class CatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.models = [
            ModelRef("chatgpt", f"model-{index}", f"Model {index}", rank=index - 1)
            for index in range(1, 6)
        ]
        self.efforts = tuple(
            EffortRef("chatgpt", "model-3", level, level.title(), rank=index)
            for index, level in enumerate(("low", "medium", "high"))
        )
        effort_set = EffortSet.from_refs(self.efforts, name="TestModelEfforts", default_id="medium")
        self.model = ModelRef(
            "chatgpt",
            "model-3",
            "Model 3",
            efforts=effort_set,
        )

    def test_policy_resolution_uses_rank_and_explicit_default(self) -> None:
        self.assertEqual(SelectionResolver.resolve(Models.LOW, self.models).id, "model-1")
        self.assertEqual(SelectionResolver.resolve(Models.MEDIUM, self.models).id, "model-3")
        self.assertEqual(SelectionResolver.resolve(Models.HIGH, self.models).id, "model-5")
        self.assertEqual(
            SelectionResolver.resolve(Models.AUTO, self.models, default_id="model-4").id,
            "model-4",
        )
        with self.assertRaises(RemoteNotFound):
            SelectionResolver.resolve(Models.AUTO, self.models)

        efforts = self.model.efforts.list()
        self.assertEqual(SelectionResolver.resolve(Models.LOW, efforts).id, "low")
        self.assertEqual(SelectionResolver.resolve(Models.MEDIUM, efforts).id, "medium")
        self.assertEqual(SelectionResolver.resolve(Models.HIGH, efforts).id, "high")

    def test_find_search_and_ambiguity(self) -> None:
        self.assertEqual(SelectionResolver.resolve("Model 2", self.models).id, "model-2")
        with self.assertRaises(AmbiguousMatchError):
            SelectionResolver.resolve("Model", self.models)
        with self.assertRaises(RemoteNotFound):
            SelectionResolver.resolve("missing", self.models)
        self.assertEqual(self.model.efforts.find("Medium").id, "medium")
        self.assertEqual([item.id for item in self.model.efforts.search("i")], ["medium", "high"])

    def test_model_and_effort_form_one_typed_selection(self) -> None:
        medium = self.model.efforts.get("medium")
        selection = self.model.select(medium)
        self.assertIsInstance(selection, ModelSelection)
        self.assertIsNotNone(selection.effort)
        assert selection.effort is not None
        self.assertEqual((selection.model.id, selection.effort.id), ("model-3", "medium"))
        self.assertEqual(medium.model_id, selection.model.id)
        high = self.model.select(Models.HIGH)
        self.assertIsNotNone(high.effort)
        assert high.effort is not None
        self.assertEqual(high.effort.id, "high")
        self.assertIsNone(self.model.select(Models.AUTO).effort)

        with self.assertRaises(ValueError):
            ModelRef(
                "chatgpt",
                "model-3",
                efforts=EffortSet.from_refs((EffortRef("claude", "model-3", "high", "High"),)),
            )

    def test_static_catalogs_have_subscription_neutral_chat_models(self) -> None:
        chatgpt_ids = {model.id for model in ChatGPTModels.list()}
        self.assertTrue(
            {
                "gpt-5-6-instant",
                "gpt-5-6-thinking",
                "gpt-6",
                "gpt-6-instant",
                "gpt-6-thinking",
                "gpt-6-mini",
                "gpt-6-t-mini",
            }.issubset(chatgpt_ids)
        )
        self.assertNotIn("gpt-5-6-mini", chatgpt_ids)
        self.assertNotIn("gpt-5-6-t-mini", chatgpt_ids)
        self.assertNotIn("gpt-5-6", chatgpt_ids)
        self.assertNotIn("gpt-6-astra", chatgpt_ids)
        medium = ChatGPTModels.GPT_6_THINKING.efforts.MEDIUM
        self.assertIsInstance(medium, EffortRef)
        self.assertEqual(medium.id, "standard")
        self.assertEqual(ChatGPTModels.GPT_6_THINKING.select(medium).effort, medium)

        claude_ids = {model.id for model in ClaudeModels.list()}
        self.assertTrue(
            {
                "claude-opus-5-5",
                "claude-fable-5-1",
                "claude-sonnet-5-5",
                "claude-haiku-5-5",
                "claude-opus-5",
                "claude-opus-4-8",
                "claude-opus-4-7",
                "claude-opus-4-6",
                "claude-opus-4-5-20251101",
                "claude-sonnet-5",
                "claude-sonnet-4-6",
                "claude-sonnet-4-5-20250929",
                "claude-haiku-4-5-20251001",
            }.issubset(claude_ids)
        )
        self.assertEqual(ClaudeModels.FABLE_5_1.efforts.MEDIUM.id, "medium")
        self.assertEqual(ClaudeModels.SONNET_5.efforts.MEDIUM_AUTO.id, "medium:auto")

        for catalog in (ChatGPTModels, ClaudeModels, GeminiModels, GrokModels):
            self.assertTrue(issubclass(catalog, StaticModelCatalog))
            self.assertTrue(all(model.provider == catalog.provider for model in catalog.list()))
            self.assertIsInstance(catalog.search(catalog.provider), list)

        self.assertEqual(GrokModels.GROK_3.name, "Grok 3")
        self.assertEqual(GrokModels.GROK_3.id, "fast")
        self.assertEqual(GrokModels.GROK_3.metadata["modelName"], "grok-3")

    def test_static_catalog_lookup_methods(self) -> None:
        model = ChatGPTModels.get("gpt-6-thinking")
        self.assertIs(model, ChatGPTModels["gpt-6-thinking"])
        self.assertEqual(ChatGPTModels.find("GPT-6 Thinking"), model)
        self.assertTrue(ChatGPTModels.contains(model))
        self.assertTrue(ChatGPTModels.contains(model.efforts.MEDIUM))
        self.assertFalse(ChatGPTModels.contains("unknown-model"))
        with self.assertRaises(RemoteNotFound):
            ChatGPTModels.get("unknown-model")


class DynamicCatalogTests(unittest.IsolatedAsyncioTestCase):
    async def test_account_model_cache_is_shared_across_sessions_and_refreshable(self) -> None:
        account = Account("account-1", "chatgpt", {"session_token": "test"})
        model = SimpleNamespace(id="gpt-6-pro", title="GPT-6 Pro", efforts=(), metadata={})
        catalog = SimpleNamespace(chat_models=(model,), default_chat_model=None)
        provider = SimpleNamespace(models=AsyncMock(return_value=catalog))
        bridge = ProviderBridge(account)
        bridge._provider_instance = provider
        first = ProviderSession(account, bridge=bridge)
        second = ProviderSession(account, bridge=bridge)

        self.assertEqual((await first.models.list())[0].id, "gpt-6-pro")
        self.assertEqual((await second.models.list())[0].id, "gpt-6-pro")
        provider.models.assert_awaited_once_with(force=False)

        await second.models.refresh()
        self.assertEqual(provider.models.await_args_list, [call(force=False), call(force=True)])

    async def test_account_catalog_is_cached_and_returns_same_typed_refs(self) -> None:
        loads = 0
        model = ModelRef(
            "claude",
            "claude-sonnet-5-5",
            "Claude Sonnet 5.5",
            efforts=EffortSet.from_refs(
                (EffortRef("claude", "claude-sonnet-5-5", "medium", "Medium"),),
                name="ClaudeSonnet55Efforts",
            ),
        )

        async def load(_refresh: bool = False) -> list[ModelRef]:
            nonlocal loads
            loads += 1
            return [model]

        catalog = ModelsCollection(load)
        found = await catalog.find("Sonnet")
        self.assertEqual(found, model)
        self.assertEqual((await catalog.get(model.id)).efforts.get("medium").id, "medium")
        self.assertEqual([item.id for item in await catalog.search("5.5")], [model.id])
        self.assertEqual(loads, 1)

        selection = await catalog.resolve(ClaudeModels.SONNET_5_5.efforts.MEDIUM)
        self.assertEqual(selection.model.id, model.id)
        self.assertIsNotNone(selection.effort)
        assert selection.effort is not None
        self.assertEqual(selection.effort.id, "medium")

    async def test_failed_refresh_keeps_the_last_valid_catalog(self) -> None:
        loads = 0
        model = ModelRef("grok", "fast", "Grok 3")

        async def load(_refresh: bool = False) -> list[ModelRef]:
            nonlocal loads
            loads += 1
            if loads == 2:
                raise RuntimeError("temporary catalog failure")
            return [model]

        catalog = ModelsCollection(load)
        self.assertEqual((await catalog.list())[0], model)
        with self.assertRaises(RuntimeError):
            await catalog.refresh()
        self.assertEqual((await catalog.list())[0], model)
        self.assertEqual(loads, 2)


if __name__ == "__main__":
    unittest.main()
