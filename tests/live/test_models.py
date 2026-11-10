"""Model catalog scenarios for the live API test case."""

from webaiprovider.base import Capability


class ModelApiTestsMixin:
    def test_02_available_models_are_listed(self) -> None:
        if Capability.MODELS not in self.context.session.capabilities:
            self.skipTest("Provider does not expose model catalogs")
        models = self.call_api("models.list", self.context.session.models.list)
        self.assertTrue(models, "Authenticated account returned an empty model catalog")
