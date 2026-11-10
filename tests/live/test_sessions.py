"""Session API scenarios, composed into one live test case."""

from webaiprovider.base import Capability


class SessionApiTestsMixin:
    def test_01_session_opens_and_exposes_capabilities(self) -> None:
        self.assertTrue(self.context.session.capabilities)
        self.assertTrue(
            all(isinstance(item, Capability) for item in self.context.session.capabilities)
        )
