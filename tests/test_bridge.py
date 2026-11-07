from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, MagicMock

from webaiprovider import Account, MessageOperation, OperationState, Provider
from webaiprovider.base import (
    ConversationBound,
    MessageMetadata,
    ReplaceText,
    ResponseComplete,
    TextDelta,
    ThoughtDelta,
)
from webaiprovider.bridge import ProviderBridge
from webaiprovider.contracts import (
    ArtifactDiscovered,
    OutputBegin,
    OutputReplace,
    SourceDiscovered,
    ThinkingBegin,
    ThinkingDelta,
    ThinkingEnd,
)
from webaiprovider.errors import UnsupportedOperationError
from webaiprovider.models import Content, Conversation, Origin, Page, Resource
from webaiprovider.models import Message as NativeMessage
from webaiprovider.models import Project as NativeProject


class ProviderBridgeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.account = Account.from_cookies("claude", {"sessionKey": "session"})
        self.provider = MagicMock()
        self.provider.id = self.account.provider
        self.provider.account_id = self.account.id
        self.bridge = ProviderBridge(self.account)
        self.bridge._provider_instance = self.provider
        self.session = Provider(self.account)
        self.session._bridge = self.bridge
        self.session._opened = True

    async def test_project_actions_map_native_ids_and_instructions(self) -> None:
        original = NativeProject(
            Origin(self.account.provider, self.account.id, "project-1"),
            "Research",
            "Be concise",
            (Resource("resource-1", "Source", "text/plain"),),
        )
        self.provider.create_project = AsyncMock(return_value=original)
        self.provider.get_project = AsyncMock(return_value=original)
        self.provider.update_project = AsyncMock(side_effect=lambda project: project)
        self.provider.add_project_resource = AsyncMock(
            return_value=NativeProject(
                original.origin,
                original.name,
                original.instructions,
                (*original.resources, Resource("resource-2", "Notes", "text/plain")),
            )
        )
        self.provider.remove_project_resource = AsyncMock(return_value=original)
        self.provider.list_projects = AsyncMock(return_value=Page((original,)))

        project = await self.session.projects.create("Research", instruction="Be concise")
        self.provider.create_project.assert_awaited_once_with("Research", instructions="Be concise")
        self.assertEqual(project.remote_id, "project-1")
        self.assertEqual(await project.instruction.get(), "Be concise")

        added = await project.resources.add("A note", resource_type="text", name="Notes")
        self.provider.add_project_resource.assert_awaited_once_with(
            "project-1", "text", name="Notes", text="A note"
        )
        self.assertEqual(added["id"], "resource-2")

        await self.session.projects.update(project, title="Updated", instruction="New instructions")
        self.provider.update_project.assert_awaited_once()
        submitted = self.provider.update_project.await_args.args[0]
        self.assertEqual(submitted.name, "Updated")
        self.assertEqual(submitted.instructions, "New instructions")
        self.assertEqual(project.title, "Updated")
        self.assertEqual(await project.instruction.get(), "New instructions")

        await project.resources.remove("resource-2")
        self.provider.remove_project_resource.assert_awaited_once_with("project-1", "resource-2")

        await self.session.projects.refresh()
        loaded = self.session.projects._items[0]
        self.assertEqual(loaded.remote_id, "project-1")
        self.assertEqual(await loaded.instruction.get(), "Be concise")
        self.assertEqual(loaded.resources._items[0]["id"], "resource-1")

    async def test_project_chat_pages_and_move_actions_use_provider_bridge(self) -> None:
        from webaiprovider.resources.chat import Chat
        from webaiprovider.resources.project import Project

        conversation = Conversation(
            Origin(self.account.provider, self.account.id, "chat-1"),
            "Project chat",
            project_id="project-1",
        )
        self.provider.list_project_chats = AsyncMock(return_value=Page((conversation,), "cursor-2"))
        self.provider.move_chat = AsyncMock()
        project = Project(title="Research", session=self.session, remote_id="project-1")

        page = await project.chats.refresh()
        self.assertEqual(page.items[0].remote_id, "chat-1")
        self.assertIs(page.items[0].session, self.session)
        self.assertEqual(project.chats.next_cursor, "cursor-2")

        chat = Chat(title="Loose chat", session=self.session, remote_id="chat-2")
        await self.session.chats.move(chat, project)
        self.provider.move_chat.assert_awaited_once_with("chat-2", "project-1")
        self.assertEqual(chat.project_id, "project-1")

    async def test_stream_maps_typed_events_and_remote_identity(self) -> None:
        request = None

        async def stream(value):
            nonlocal request
            request = value
            yield ConversationBound("conversation-1")
            yield ThoughtDelta("reasoning")
            yield TextDelta("answer")
            yield MessageMetadata(
                sources=({"id": "source-1", "url": "https://example.test"},),
                files=({"id": "file-1", "name": "result.txt", "url": "https://files.test"},),
            )
            yield ResponseComplete()

        self.provider.stream = stream
        chat = await self.session.chats.create()
        operation = chat.messages.create("prompt")

        events = [event async for event in self.bridge.execute_stream(operation)]

        self.assertIsInstance(operation, MessageOperation)
        self.assertEqual(request.provider_id, self.account.provider)
        self.assertEqual(chat.remote_id, "conversation-1")
        self.assertTrue(any(isinstance(event, SourceDiscovered) for event in events))
        self.assertTrue(any(isinstance(event, ArtifactDiscovered) for event in events))
        source = next(event.source for event in events if isinstance(event, SourceDiscovered))
        artifact = next(event.artifact for event in events if isinstance(event, ArtifactDiscovered))
        self.assertEqual(source.id, "source-1")
        self.assertEqual(artifact.id, "file-1")

    async def test_download_rejects_unresolved_user_url(self) -> None:
        self.provider.resolve_download = AsyncMock(return_value=None)

        with self.assertRaises(UnsupportedOperationError):
            await self.bridge.download("https://example.test/file")

        self.provider.resolve_download.assert_awaited_once_with(
            "https://example.test/file", conversation_id=None, message_id=None
        )

    async def test_stream_handles_analysis_channel_and_propagates_message_ids(self) -> None:
        async def stream(_value):
            yield ConversationBound("conversation-2")
            yield ReplaceText("Step 1", "analysis")
            yield ReplaceText("", "final")
            yield ReplaceText("Step 1 and Step 2", "analysis")
            yield ReplaceText("Final output", "final")
            yield MessageMetadata(
                message_id="msg-assistant-99",
                user_message_id="msg-user-99",
            )
            yield ResponseComplete()

        self.provider.stream = stream
        chat = await self.session.chats.create()
        operation = chat.messages.create("solve")
        self.assertEqual(operation.state, OperationState.PENDING)

        events = [event async for event in operation.stream()]
        response = await operation.wait()

        self.assertEqual(operation.state, OperationState.COMPLETED)
        self.assertEqual(operation.message.id, "msg-user-99")
        self.assertEqual(response.message.id, "msg-assistant-99")
        self.assertEqual(response.text, "Final output")
        self.assertIn(response.message, chat.messages.list())
        self.assertEqual(self.session.telemetry.snapshot()["successful"], 1)
        self.assertIsInstance(events[0], ThinkingBegin)
        self.assertEqual(
            [e.delta for e in events if isinstance(e, ThinkingDelta)],
            ["Step 1", " and Step 2"],
        )
        thinking_end = next(e for e in events if isinstance(e, ThinkingEnd))
        self.assertEqual(thinking_end.text, "Step 1 and Step 2")
        self.assertTrue(any(isinstance(e, OutputBegin) for e in events))
        self.assertTrue(any(isinstance(e, OutputReplace) for e in events))

    async def test_chat_messages_refresh_loads_remote_history(self) -> None:
        origin = Origin(self.account.provider, self.account.id, "conv-remote-1")
        remote_conv = Conversation(
            origin,
            "Loaded Title",
            messages=(
                NativeMessage("u-1", "user", (Content("text", "Hello"),)),
                NativeMessage("a-think", "assistant", (Content("text", "Thinking"),), "analysis"),
                NativeMessage("a-1", "assistant", (Content("text", "Hi there!"),)),
            ),
        )
        self.provider.get_chat = AsyncMock(return_value=remote_conv)
        chat = await self.session.chats.create(title="")
        chat.remote_id = "conv-remote-1"

        messages = await chat.messages.refresh()

        self.assertEqual(chat.title, "Loaded Title")
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0].id, "u-1")
        self.assertEqual(messages[0].role, "user")
        self.assertEqual(messages[0].content, "Hello")
        self.assertEqual(messages[1].id, "a-1")
        self.assertEqual(messages[1].role, "assistant")
        self.assertEqual(messages[1].content, "Hi there!")


if __name__ == "__main__":
    unittest.main()
