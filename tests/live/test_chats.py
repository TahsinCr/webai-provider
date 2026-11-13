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
        self.assertTrue(all(chat.project is None for chat in chats))

        if Capability.PROJECTS in self.context.session.capabilities:
            all_chats = await self.call_api(
                "chats.list_include_projects",
                lambda: self.context.session.chats.refresh(include_projects=True),
            )
            self.assertIsInstance(all_chats, list)
            absent = await self.call_api(
                "chats.search_include_projects",
                lambda: self.context.session.chats.search(
                    "__webai_absent__", include_projects=True
                ),
            )
            self.assertEqual(absent, [])
        else:
            self.context.skip("chats.include_projects: unsupported")
