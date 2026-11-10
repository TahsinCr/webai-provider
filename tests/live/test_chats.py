"""Chat metadata scenarios for the live API test case."""

from webaiprovider.base import Capability


class ChatApiTestsMixin:
    def test_03_chat_metadata_can_be_listed_without_loading_histories(self) -> None:
        if Capability.CHAT not in self.context.session.capabilities:
            self.skipTest("Provider does not expose chats")
        chats = self.call_api("chats.list_metadata", self.context.session.chats.refresh)
        self.assertIsInstance(chats, list)
