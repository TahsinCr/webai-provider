"""Live checks for the authenticated account's available model catalog."""

from contextlib import suppress

from webaiprovider.base import Capability
from webaiprovider.errors import RemoteNotFound

from .support.case import LiveAsyncTestCase, LiveSyncTestCase


class ModelApiTests(LiveSyncTestCase):
    def test_available_models_are_listed(self) -> None:
        if Capability.MODELS not in self.context.session.capabilities:
            self.skipTest("Provider does not expose model catalogs")
        models = self.call_api("models.list", self.context.session.models.list)
        self.assertTrue(models, "Authenticated account returned an empty model catalog")
        catalog = self.context.session.models
        model = models[0]
        self.assertEqual(catalog.get(model.id), model)
        self.assertEqual(catalog.find(model.id), model)
        self.assertIn(model, catalog.search(model.id, exact=True))
        self.assertEqual(catalog.resolve(model).model, model)
        if efforts := model.efforts.list():
            self.assertEqual(catalog.resolve(efforts[0]).effort, efforts[0])


class AsyncModelApiTests(LiveAsyncTestCase):
    async def test_available_models_are_listed(self) -> None:
        if Capability.MODELS not in self.context.session.capabilities:
            self.skipTest("Provider does not expose model catalogs")
        models = await self.call_api("models.list", self.context.session.models.list)
        self.assertTrue(models, "Authenticated account returned an empty model catalog")
        catalog = self.context.session.models
        model = models[0]
        self.assertEqual(await catalog.get(model.id), model)
        self.assertEqual(await catalog.find(model.id), model)
        self.assertIn(model, await catalog.search(model.id, exact=True))
        self.assertEqual((await catalog.resolve(model)).model, model)
        if efforts := model.efforts.list():
            self.assertEqual((await catalog.resolve(efforts[0])).effort, efforts[0])

    async def test_every_model_responds_with_standard_effort(self) -> None:
        capabilities = self.context.session.capabilities
        if Capability.MODELS not in capabilities or Capability.CHAT not in capabilities:
            self.skipTest("Provider does not expose chat model execution")

        use_temporary = Capability.TEMPORARY in capabilities
        if not use_temporary and Capability.DELETE not in capabilities:
            self.skipTest("Provider cannot safely isolate or clean up model verification chats")

        models = await self.call_api("models.refresh_all", self.context.session.models.refresh)
        self.assertTrue(models)
        for model in models:
            efforts = model.efforts.list()
            by_id = {effort.id: effort for effort in efforts}
            default_id = getattr(model.efforts, "DEFAULT_EFFORT_ID", None)
            effort = (
                by_id.get("standard")
                or by_id.get("medium")
                or (by_id.get(default_id) if default_id else None)
                or (efforts[0] if efforts else None)
            )
            selection = model.select(effort) if effort is not None else model
            marker = f"{self.context.marker}-m-{model.id}"
            entry = None if use_temporary else self.context.new_remote("chat", marker)
            if entry is not None:
                entry["attempted"] = True
                self.context.save()
            chat = await self.context.session.chats.create(title=marker, temporary=use_temporary)
            try:
                if entry is not None:
                    self.context.record_chat_id(entry, chat)
                op = chat.messages.create("Reply with OK.", model=selection)
                response = await self.call_or_skip_rate_limit(
                    f"models.send[{model.id}]", op.start, entry=entry, chat=chat
                )
                if entry is not None:
                    self.context.record_chat_id(entry, chat)
                self.assertTrue(
                    getattr(response.message, "content", "").strip(),
                    f"Model {model.id} returned empty text",
                )
            finally:
                if entry is not None:
                    await self.context.cleanup_entry_async(entry)
                elif chat.remote_id:
                    with suppress(RemoteNotFound):
                        await self.call_api(
                            "models.delete_temporary",
                            chat.delete,
                            expected_errors=(RemoteNotFound,),
                        )
