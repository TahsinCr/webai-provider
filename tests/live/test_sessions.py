"""Live checks for provider session setup and advertised capabilities."""

from eventflow import AsyncEventBus

from webaiprovider import (
    LifecycleState,
    OperationHooks,
)
from webaiprovider.accounts.status import SessionState, SessionStatus
from webaiprovider.base import Capability

from .support.case import LiveAsyncTestCase


class AsyncSessionApiTests(LiveAsyncTestCase):
    async def test_session_opens_and_exposes_capabilities(self) -> None:
        self.assertTrue(self.context.session.capabilities)
        self.assertTrue(
            all(isinstance(item, Capability) for item in self.context.session.capabilities)
        )
        self.assertEqual(self.context.session.lifecycle_state, LifecycleState.OPEN)
        self.assertIsInstance(self.context.session.events, AsyncEventBus)
        self.assertIsInstance(self.context.session.hooks, OperationHooks)

        if Capability.AUTH in self.context.session.capabilities:
            status = await self.call_api("session.status", self.context.session.session_status)
            self.assertIsInstance(status, SessionStatus)
            self.assertTrue(status.saved_snapshot)
            self.assertIn(status.state, (SessionState.AUTHENTICATED, SessionState.UNKNOWN))
