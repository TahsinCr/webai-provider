"""Live checks for chat metadata access without loading message histories."""

from webaiprovider.base import Capability

from .support.case import LiveAsyncTestCase, LiveSyncTestCase


def _chat_metadata_steps(case):
    if Capability.CHAT not in case.context.session.capabilities:
        case.skipTest("Provider does not expose chats")
    empty = yield case.context.session.chats.list(limit=0)
    case.assertEqual(empty, [])
    chats = yield case.call_api(
        "chats.list_metadata", lambda: case.context.session.chats.list(limit=2)
    )
    case.assertLessEqual(len(chats), 2)
    case.assertTrue(all(chat.project is None for chat in chats))

    if Capability.PROJECTS in case.context.session.capabilities:
        all_chats = yield case.call_api(
            "chats.list_include_projects",
            lambda: case.context.session.chats.list(limit=2, include_projects=True),
        )
        case.assertLessEqual(len(all_chats), 2)
    else:
        case.context.skip("chats.include_projects: unsupported")


class AsyncChatApiTests(LiveAsyncTestCase):
    async def test_chat_metadata_can_be_listed_without_loading_histories(self) -> None:
        await self.run_steps(_chat_metadata_steps(self))


class SyncChatApiTests(LiveSyncTestCase):
    def test_chat_metadata_can_be_listed_without_loading_histories(self) -> None:
        self.run_steps(_chat_metadata_steps(self))
