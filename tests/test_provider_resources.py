from __future__ import annotations

import unittest
from unittest.mock import AsyncMock

from tests.streaming_support import StreamTestProvider
from webaiprovider import Account, AsyncProvider, MessageOperation, OperationState
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
from webaiprovider.models import Content, Conversation, Origin, Page, ResourceData
from webaiprovider.models import MessageData as NativeMessage
from webaiprovider.models import ProjectData as NativeProject
from webaiprovider.streams import (
    ConversationBound,
    MessageMetadata,
    ResponseComplete,
    TextDelta,
    TextReplacement,
    ThoughtDelta,
)


class ProviderResourceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.account = Account.from_cookies("claude", {"sessionKey": "session"})
        self.provider = AsyncProvider(self.account)
        for name in (
            "prepare",
            "_close_provider",
            "create_project",
            "get_project",
            "update_project",
            "add_project_resource",
            "remove_project_resource",
            "list_projects",
            "delete_project",
            "list_project_chats",
            "move_chat",
            "get_chat",
            "list_chats",
            "rename_chat",
            "delete_chat",
            "resolve_download",
        ):
            setattr(self.provider, name, AsyncMock())
        await self.provider.open()
        self.addAsyncCleanup(self.provider.close)

    async def asyncTearDown(self) -> None:
        await self.provider.close()

    async def test_public_project_actions_use_shared_provider_api(self) -> None:
        original = NativeProject(
            Origin(self.account.provider, self.account.id, "project-1"),
            "Research",
            "Be concise",
            (ResourceData("resource-1", "Source", "text/plain"),),
        )
        self.provider.create_project.return_value = original
        self.provider.get_project.return_value = original
        self.provider.update_project.side_effect = lambda project: project
        self.provider.add_project_resource.return_value = NativeProject(
            original.origin,
            original.name,
            original.instructions,
            (*original.resources, ResourceData("resource-2", "Notes", "text/plain")),
        )
        self.provider.remove_project_resource.return_value = original
        self.provider.list_projects.return_value = Page((original,))

        project = await self.provider.projects.create("Research", instruction="Be concise")
        self.provider.create_project.assert_awaited_once_with("Research", instructions="Be concise")
        self.assertEqual(project.remote_id, "project-1")
        self.assertEqual(await project.instruction.get(), "Be concise")
        added = await project.resources.add("A note", resource_type="text", name="Notes")
        self.provider.add_project_resource.assert_awaited_once_with(
            "project-1", "text", name="Notes", text="A note"
        )
        self.assertEqual(added["id"], "resource-2")

        await self.provider.projects.update(
            project, title="Updated", instruction="New instructions"
        )
        self.provider.update_project.assert_awaited_once()
        submitted = self.provider.update_project.await_args.args[0]
        self.assertEqual(submitted.name, "Updated")
        self.assertEqual(submitted.instructions, "New instructions")
        self.assertEqual(project.title, "Updated")
        self.assertEqual(await project.instruction.get(), "New instructions")

        await project.resources.remove("resource-2")
        self.provider.remove_project_resource.assert_awaited_once_with("project-1", "resource-2")
        await self.provider.projects.refresh()
        loaded = self.provider.projects._items[0]
        self.assertEqual(loaded.remote_id, "project-1")
        self.assertEqual(await loaded.instruction.get(), "Be concise")
        self.assertEqual(loaded.resources._items[0]["id"], "resource-1")

    async def test_project_chat_pages_and_move_actions(self) -> None:
        from webaiprovider.providers.grok import AsyncGrokChat
        from webaiprovider.resources.project.project import AsyncProject

        account = Account.from_cookies("grok", {"sso": "session"})
        provider = AsyncProvider(account)
        provider.prepare = AsyncMock()
        provider._close_provider = AsyncMock()
        provider.list_project_chats = AsyncMock()
        provider.move_chat = AsyncMock()
        conversation = Conversation(
            Origin(account.provider, account.id, "chat-1"),
            "Project chat",
            project_id="project-1",
        )
        provider.list_project_chats.return_value = Page((conversation,), "cursor-2")
        await provider.open()
        self.addAsyncCleanup(provider.close)

        project = AsyncProject(title="Research", session=provider, remote_id="project-1")
        page = await project.chats.refresh()
        self.assertEqual(page.items[0].remote_id, "chat-1")
        self.assertIs(page.items[0].session, provider)
        self.assertEqual(project.chats.next_cursor, "cursor-2")

        chat = AsyncGrokChat(title="Loose chat", session=provider, remote_id="chat-2")
        await chat.move_to(project)
        provider.move_chat.assert_awaited_once_with("chat-2", "project-1")
        self.assertEqual(chat.project_id, "project-1")

    async def test_stream_maps_typed_events_and_remote_identity(self) -> None:
        stream_provider = StreamTestProvider(
            self.account.provider,
            self.account.id,
            (
                ConversationBound("conversation-1"),
                ThoughtDelta("reasoning"),
                TextDelta("answer"),
                MessageMetadata(
                    sources=({"id": "source-1", "url": "https://example.test"},),
                    files=({"id": "file-1", "name": "result.txt", "url": "https://files.test"},),
                ),
                ResponseComplete(),
            ),
        )
        self.provider._stream_request = stream_provider._stream_request
        chat = await self.provider.chats.create()
        operation = chat.messages.create("prompt")
        events = [event async for event in operation.stream()]

        self.assertIsInstance(operation, MessageOperation)
        self.assertEqual(stream_provider.requests[0].prompt, "prompt")
        self.assertEqual(chat.remote_id, "conversation-1")
        self.assertTrue(any(isinstance(event, SourceDiscovered) for event in events))
        self.assertTrue(any(isinstance(event, ArtifactDiscovered) for event in events))
        source = next(event.source for event in events if isinstance(event, SourceDiscovered))
        artifact = next(event.artifact for event in events if isinstance(event, ArtifactDiscovered))
        self.assertEqual(source.id, "source-1")
        self.assertEqual(artifact.id, "file-1")

    async def test_download_rejects_unresolved_user_url(self) -> None:
        self.provider.resolve_download.return_value = None
        chat = await self.provider.chats.create()
        chat.remote_id = "conversation-1"
        with self.assertRaises(UnsupportedOperationError):
            await chat.download("https://example.test/file")
        self.provider.resolve_download.assert_awaited_once_with(
            "https://example.test/file", conversation_id="conversation-1"
        )

    async def test_stream_handles_analysis_channel_and_propagates_message_ids(self) -> None:
        stream_provider = StreamTestProvider(
            self.account.provider,
            self.account.id,
            (
                ConversationBound("conversation-2"),
                TextReplacement("Step 1", "analysis"),
                TextReplacement("", "final"),
                TextReplacement("Step 1 and Step 2", "analysis"),
                TextReplacement("Final output", "final"),
                MessageMetadata(
                    message_id="msg-assistant-99",
                    user_message_id="msg-user-99",
                ),
                ResponseComplete(),
            ),
        )
        self.provider._stream_request = stream_provider._stream_request
        chat = await self.provider.chats.create()
        operation = chat.messages.create("solve")
        self.assertEqual(operation.state, OperationState.PENDING)
        events = [event async for event in operation.stream()]
        response = await operation.wait()

        self.assertEqual(operation.state, OperationState.COMPLETED)
        self.assertEqual(operation.message.id, "msg-user-99")
        self.assertEqual(response.message.id, "msg-assistant-99")
        self.assertEqual(response.text, "Final output")
        self.assertIn(response.message, chat.messages.list())
        self.assertEqual(self.provider.telemetry.snapshot()["successful"], 1)
        self.assertIsInstance(events[0], ThinkingBegin)
        self.assertEqual(
            [event.delta for event in events if isinstance(event, ThinkingDelta)],
            ["Step 1", " and Step 2"],
        )
        thinking_end = next(event for event in events if isinstance(event, ThinkingEnd))
        self.assertEqual(thinking_end.text, "Step 1 and Step 2")
        self.assertTrue(any(isinstance(event, OutputBegin) for event in events))
        self.assertTrue(any(isinstance(event, OutputReplace) for event in events))

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
        self.provider.get_chat.return_value = remote_conv
        chat = await self.provider.chats.create(title="")
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
