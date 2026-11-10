"""Live checks for chat metadata access without loading message histories."""

from webaiprovider.base import Capability

from .support.case import LiveAsyncTestCase, LiveSyncTestCase


class ChatApiTests(LiveSyncTestCase):
    def test_chat_metadata_can_be_listed_without_loading_histories(self) -> None:
        if Capability.CHAT not in self.context.session.capabilities:
            self.skipTest("Provider does not expose chats")
        chats = self.call_api("chats.list_metadata", self.context.session.chats.refresh)
        self.assertIsInstance(chats, list)


class AsyncChatApiTests(LiveAsyncTestCase):
    async def test_chat_metadata_can_be_listed_without_loading_histories(self) -> None:
        if Capability.CHAT not in self.context.session.capabilities:
            self.skipTest("Provider does not expose chats")
        chats = await self.call_api("chats.list_metadata", self.context.session.chats.refresh)
        self.assertIsInstance(chats, list)
