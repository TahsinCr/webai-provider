"""Live checks for the authenticated account's available model catalog."""

from webaiprovider.base import Capability

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
