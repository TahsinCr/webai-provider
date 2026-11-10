"""Live checks for the authenticated account's available model catalog."""

from webaiprovider.base import Capability

from .support.case import LiveAsyncTestCase, LiveSyncTestCase


class ModelApiTests(LiveSyncTestCase):
    def test_available_models_are_listed(self) -> None:
        if Capability.MODELS not in self.context.session.capabilities:
            self.skipTest("Provider does not expose model catalogs")
        models = self.call_api("models.list", self.context.session.models.list)
        self.assertTrue(models, "Authenticated account returned an empty model catalog")


class AsyncModelApiTests(LiveAsyncTestCase):
    async def test_available_models_are_listed(self) -> None:
        if Capability.MODELS not in self.context.session.capabilities:
            self.skipTest("Provider does not expose model catalogs")
        models = await self.call_api("models.list", self.context.session.models.list)
        self.assertTrue(models, "Authenticated account returned an empty model catalog")
