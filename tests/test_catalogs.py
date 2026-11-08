from __future__ import annotations

import copy
import unittest
from unittest.mock import AsyncMock, call, patch

from webaiprovider.accounts import Account
from webaiprovider.async_provider import AsyncProvider
from webaiprovider.bridge import ProviderBridge
from webaiprovider.catalogs import (
    Models,
    ModelsCollection,
    SelectionResolver,
    StaticModelCatalog,
)
from webaiprovider.contracts import EffortRef, EffortSet, ModelRef, ModelSelection
from webaiprovider.errors import AmbiguousMatchError, ProviderError, RemoteNotFound
from webaiprovider.providers.base import ProviderAdapter
from webaiprovider.providers.chatgpt.catalogs import ChatGPTModels
from webaiprovider.providers.chatgpt.discovery import model_catalog
from webaiprovider.providers.claude.catalogs import ClaudeModels, selector_catalog
from webaiprovider.providers.gemini.catalogs import GeminiModels
from webaiprovider.providers.grok.catalogs import GrokModels


def _claude_selector_fixture() -> dict:
    return {
        "model_selector_config": [
            {
                "id": "chat",
                "models": [
                    {
                        "id": "paid",
                        "name": "Paid",
                        "disabled": True,
                        "disabled_reason": {"type": "upgrade_required"},
                    },
                    {
                        "id": "allowed",
                        "name": "Allowed",
                        "thinking": {
                            "type": "effort_and_mode",
                            "effort_options": [
                                {"id": "low", "name": "Low"},
                                {"id": "medium", "name": "Medium", "recommended": True},
                            ],
                            "mode_options": [{"id": "auto", "name": "Thinking"}],
                        },
                    },
                    {"id": "disabled-by-policy", "disabled_reason": {"type": "policy"}},
                    {"id": "old", "inactive": True},
                ],
            }
        ],
        "model_selector_state": [
            {
                "id": "chat",
                "model": "paid",
                "thinking_by_model": [
                    {"id": "allowed", "thinking": {"effort": "low", "mode": "auto"}}
                ],
            }
        ],
    }


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
    async def test_provider_model_cache_is_reused_and_refreshable(self) -> None:
        from webaiprovider.models import FeatureState, Model, ModelCatalog

        account = Account("account-1", "chatgpt", {"session_token": "test"})
        model = Model("gpt-6-pro", "GPT-6 Pro")
        catalog = ModelCatalog(FeatureState.AVAILABLE, (model,))
        adapter = AsyncMock(spec=ProviderAdapter)
        adapter.id = account.provider
        adapter.account_id = account.id
        adapter.models.return_value = catalog
        bridge = ProviderBridge(account)
        bridge._provider_instance = adapter
        bridge.open = AsyncMock()
        bridge.close = AsyncMock()
        provider = AsyncProvider(account)
        with patch("webaiprovider.bridge.ProviderBridge", return_value=bridge):
            await provider.open()
        self.addAsyncCleanup(provider.close)

        self.assertEqual((await provider.models.list())[0].id, "gpt-6-pro")
        self.assertEqual((await provider.models.list())[0].id, "gpt-6-pro")
        adapter.models.assert_awaited_once_with(force=False)

        await provider.models.refresh()
        self.assertEqual(adapter.models.await_args_list, [call(force=False), call(force=True)])

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


class EntitlementTests(unittest.TestCase):
    def test_unavailable_models_never_enter_the_selectable_catalog(self) -> None:
        catalog = selector_catalog(_claude_selector_fixture())
        self.assertEqual([x["id"] for x in catalog["models"]], ["allowed"])
        self.assertEqual(catalog["default_model"], "allowed")
        model = catalog["models"][0]
        self.assertEqual(
            model["selections"]["medium"], {"effort": "medium", "thinking_mode": "auto"}
        )
        self.assertEqual(next(x["id"] for x in model["efforts"] if x["selected"]), "low")

    def test_two_thinking_axes_remain_selectable_without_inventing_values(self) -> None:
        data = _claude_selector_fixture()
        model = data["model_selector_config"][0]["models"][1]
        model["thinking"]["mode_options"].append({"id": "off", "name": "Off"})
        before = copy.deepcopy(data)
        result = selector_catalog(data)["models"][0]
        self.assertEqual(
            set(result["selections"]), {"low:auto", "low:off", "medium:auto", "medium:off"}
        )
        self.assertEqual(result["selections"]["low:off"], {"effort": "low", "thinking_mode": "off"})
        self.assertEqual(data, before)

    def test_changed_or_empty_authoritative_catalog_does_not_fall_back_to_global_models(
        self,
    ) -> None:
        for data in [{}, {"model_selector_config": [{"id": "chat", "models": []}]}]:
            with self.assertRaises(ProviderError):
                selector_catalog(data)

    def test_gpt_disabled_or_account_inaccessible_models_are_filtered(self) -> None:
        catalog = model_catalog(
            {
                "models": [
                    {"slug": "live"},
                    {"slug": "locked", "is_available": False},
                    {"slug": "disabled", "disabled": True},
                ]
            }
        )
        self.assertEqual([m.id for m in catalog.models], ["live"])


class ChatModelLookupTests(unittest.TestCase):
    def test_lookup_keeps_chat_visibility_order_and_unknown_selection(self) -> None:
        from webaiprovider.models import FeatureState, Model, ModelCatalog

        hidden = Model("shared", "Tool", chat_selectable=False)
        first = Model("shared", "First chat")
        second = Model("shared", "Second chat")
        catalog = ModelCatalog(FeatureState.AVAILABLE, (hidden, first, second))
        self.assertIs(catalog.chat_model("shared"), first)
        self.assertIsNone(catalog.chat_model("missing"))
        self.assertIsNone(catalog.chat_model(None))
        self.assertIsNone(ModelCatalog(FeatureState.UNKNOWN).chat_model("shared"))


if __name__ == "__main__":
    unittest.main()
