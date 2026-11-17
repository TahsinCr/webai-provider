"""Live checks for provider session setup and advertised capabilities."""

from eventflow import AsyncEventBus, EventBus

from webaiprovider import (
    LifecycleState,
    OperationHooks,
    SyncOperationHooks,
)
from webaiprovider.base import Capability

from .support.case import LiveAsyncTestCase, LiveSyncTestCase


class SessionApiTests(LiveSyncTestCase):
    def test_session_opens_and_exposes_capabilities(self) -> None:
        self.assertTrue(self.context.session.capabilities)
        self.assertTrue(
            all(isinstance(item, Capability) for item in self.context.session.capabilities)
        )
        self.assertEqual(self.context.session.lifecycle_state, LifecycleState.OPEN)
        self.assertIsInstance(self.context.session.events, EventBus)
        self.assertIsInstance(self.context.session.hooks, SyncOperationHooks)


class AsyncSessionApiTests(LiveAsyncTestCase):
    async def test_session_opens_and_exposes_capabilities(self) -> None:
        self.assertTrue(self.context.session.capabilities)
        self.assertTrue(
            all(isinstance(item, Capability) for item in self.context.session.capabilities)
        )
        self.assertEqual(self.context.session.lifecycle_state, LifecycleState.OPEN)
        self.assertIsInstance(self.context.session.events, AsyncEventBus)
        self.assertIsInstance(self.context.session.hooks, OperationHooks)
