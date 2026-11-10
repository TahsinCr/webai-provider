"""Live checks for provider session setup and advertised capabilities."""

from webaiprovider.base import Capability

from .support.case import LiveAsyncTestCase, LiveSyncTestCase


class SessionApiTests(LiveSyncTestCase):
    def test_session_opens_and_exposes_capabilities(self) -> None:
        self.assertTrue(self.context.session.capabilities)
        self.assertTrue(
            all(isinstance(item, Capability) for item in self.context.session.capabilities)
        )


class AsyncSessionApiTests(LiveAsyncTestCase):
    async def test_session_opens_and_exposes_capabilities(self) -> None:
        self.assertTrue(self.context.session.capabilities)
        self.assertTrue(
            all(isinstance(item, Capability) for item in self.context.session.capabilities)
        )
