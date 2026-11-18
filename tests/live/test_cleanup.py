"""Explicit recovery checks for previously journaled temporary test objects."""

from .support.case import LiveAsyncRecoveryTestCase


class AsyncCleanupRecoveryTests(LiveAsyncRecoveryTestCase):
    async def test_pending_remote_objects_are_deleted_and_verified(self) -> None:
        await self.context.cleanup_pending_async()
