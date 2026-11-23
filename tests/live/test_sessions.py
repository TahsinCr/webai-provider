"""Live checks for provider session setup and advertised capabilities."""

from flowevent import AsyncEventBus, EventBus

from webaiprovider import (
    AsyncOperationHooks,
    LifecycleState,
    OperationHooks,
)
from webaiprovider.accounts.status import SessionState, SessionStatus
from webaiprovider.base import Capability

from .support.case import LiveAsyncTestCase, LiveSyncTestCase


def _assert_session_common(case, *, event_bus_type: type, hooks_type: type) -> None:
    session = case.context.session
    case.assertTrue(session.capabilities)
    case.assertTrue(all(isinstance(item, Capability) for item in session.capabilities))
    case.assertEqual(session.lifecycle_state, LifecycleState.OPEN)
    case.assertIsInstance(session.events, event_bus_type)
    case.assertIsInstance(session.hooks, hooks_type)


def _assert_session_status(case, status: SessionStatus) -> None:
    case.assertIsInstance(status, SessionStatus)
    case.assertTrue(status.saved_snapshot)
    case.assertIn(status.state, (SessionState.AUTHENTICATED, SessionState.UNKNOWN))


class AsyncSessionApiTests(LiveAsyncTestCase):
    async def test_session_opens_and_exposes_capabilities(self) -> None:
        _assert_session_common(self, event_bus_type=AsyncEventBus, hooks_type=AsyncOperationHooks)
        if Capability.AUTH in self.context.session.capabilities:
            status = await self.call_api("session.status", self.context.session.session_status)
            _assert_session_status(self, status)


class SyncSessionApiTests(LiveSyncTestCase):
    def test_session_opens_and_exposes_capabilities(self) -> None:
        _assert_session_common(self, event_bus_type=EventBus, hooks_type=OperationHooks)
        if Capability.AUTH in self.context.session.capabilities:
            status = self.call_api("session.status", self.context.session.session_status)
            _assert_session_status(self, status)
