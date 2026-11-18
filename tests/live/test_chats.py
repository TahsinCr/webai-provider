"""Live checks for chat metadata access without loading message histories."""

from webaiprovider.base import Capability

from .support.case import LiveAsyncTestCase


class AsyncChatApiTests(LiveAsyncTestCase):
    async def test_chat_metadata_can_be_listed_without_loading_histories(self) -> None:
        if Capability.CHAT not in self.context.session.capabilities:
            self.skipTest("Provider does not expose chats")
        self.assertEqual(await self.context.session.chats.list(limit=0), [])
        chats = await self.call_api(
            "chats.list_metadata", lambda: self.context.session.chats.list(limit=2)
        )
        self.assertLessEqual(len(chats), 2)
        self.assertTrue(all(chat.project is None for chat in chats))

        if Capability.PROJECTS in self.context.session.capabilities:
            all_chats = await self.call_api(
                "chats.list_include_projects",
                lambda: self.context.session.chats.list(limit=2, include_projects=True),
            )
            self.assertLessEqual(len(all_chats), 2)
        else:
            self.context.skip("chats.include_projects: unsupported")
