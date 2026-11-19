from __future__ import annotations

import unittest
from dataclasses import replace
from importlib import import_module
from unittest.mock import AsyncMock

from tests.streaming_support import StreamTestProvider
from webaiprovider import Account, AsyncMessageOperation, AsyncProvider, OperationState, Provider
from webaiprovider.collections import Page
from webaiprovider.core.operations import run_pipeline_unwrapped
from webaiprovider.errors import UnsupportedOperationError
from webaiprovider.resources.chat.types import Conversation
from webaiprovider.resources.message.attachment import Attachment
from webaiprovider.resources.message.operation import (
    ArtifactDiscovered,
    OutputBegin,
    OutputReplace,
    SourceDiscovered,
    ThinkingBegin,
    ThinkingDelta,
    ThinkingEnd,
)
from webaiprovider.resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    ResponseComplete,
    TextDelta,
    TextReplacement,
    ThoughtDelta,
)
from webaiprovider.resources.message.types import Content
from webaiprovider.resources.message.types import MessageData as NativeMessage
from webaiprovider.resources.project.types import ProjectData as NativeProject


class ProviderResourceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.account = Account.from_cookies("claude", {"sessionKey": "session"})
        self.provider = AsyncProvider(self.account)
        for name in ("_prepare_session", "_close_session"):
            setattr(self.provider, name, AsyncMock())
        for name in (
            "_create_project_data",
            "_get_project_data",
            "_patch_project",
            "_add_project_resource",
            "_remove_project_resource",
            "_list_projects_data",
            "_delete_project_data",
            "_list_project_chats_data",
        ):
            setattr(self.provider, name, AsyncMock())
        for name in (
            "_get_chat_data",
            "_list_chats_data",
            "_rename_chat_data",
            "_delete_chat_data",
            "_move_chat_data",
        ):
            setattr(self.provider, name, AsyncMock())
        self.provider._resolve_download = AsyncMock()
        await self.provider.open()
        self.addAsyncCleanup(self.provider.close)

    async def asyncTearDown(self) -> None:
        await self.provider.close()

    async def test_public_project_actions_use_shared_provider_api(self) -> None:
        original = NativeProject(
            "project-1",
            "Research",
            "Be concise",
            (Attachment("resource-1", "Source", "text/plain"),),
        )
        state = {"project": original}
        self.provider._create_project_data.return_value = original
        self.provider._get_project_data.side_effect = lambda _pid: state["project"]

        async def do_patch(project_id, **fields):
            del project_id
            state["project"] = replace(
                state["project"],
                **{key: value for key, value in fields.items() if value is not None},
            )
            return state["project"]

        self.provider._patch_project = AsyncMock(side_effect=do_patch)
        with_resource = NativeProject(
            original.id,
            original.name,
            original.instructions,
            (*original.resources, Attachment("resource-2", "Notes", "text/plain")),
        )

        async def do_add(*_args, **_kwargs):
            state["project"] = with_resource
            return state["project"]

        async def do_remove(*_args, **_kwargs):
            state["project"] = original
            return state["project"]

        self.provider._add_project_resource.side_effect = do_add
        self.provider._remove_project_resource.side_effect = do_remove
        self.provider._list_projects_data.return_value = Page((original,))

        project = await self.provider.projects.create("Research", instruction="Be concise")
        self.provider._create_project_data.assert_awaited_once_with(
            "Research", instructions="Be concise"
        )
        self.assertEqual(project.remote_id, "project-1")
        self.assertEqual(await project.instruction.get(), "Be concise")
        added = await project.attachments.create("A note", resource_type="text", name="Notes")
        self.provider._add_project_resource.assert_awaited_once_with(
            "project-1", "text", name="Notes", text="A note"
        )
        self.assertEqual(added.id, "resource-2")

        await self.provider.projects.update(
            project, title="Updated", instruction="New instructions"
        )
        self.provider._patch_project.assert_awaited_once_with(
            "project-1", name="Updated", instructions="New instructions"
        )
        self.assertEqual(project.title, "Updated")
        self.assertEqual(await project.instruction.get(), "New instructions")

        self.provider._get_project_data.reset_mock()
        await project.attachments.delete("resource-2")
        self.provider._remove_project_resource.assert_awaited_once_with("project-1", "resource-2")
        self.provider._get_project_data.assert_not_awaited()
        state["project"] = original
        loaded_projects = await self.provider.projects.list(limit=None)
        loaded = loaded_projects[0]
        self.assertEqual(loaded.remote_id, "project-1")
        self.assertEqual(await loaded.instruction.get(), "Be concise")
        self.assertEqual((await loaded.attachments.list())[0].id, "resource-1")

    async def test_project_chat_pages_and_move_actions(self) -> None:
        from webaiprovider.providers.grok import AsyncGrokChat
        from webaiprovider.resources.project.project import AsyncProject

        account = Account.from_cookies("grok", {"sso": "session"})
        provider = AsyncProvider(account)
        provider._prepare_session = AsyncMock()
        provider._close_session = AsyncMock()
        chat1 = Conversation(
            "chat-1",
            "Project chat 1",
            project=NativeProject("project-1", "Research"),
        )
        chat2 = Conversation(
            "chat-2b",
            "Project chat 2",
            project=NativeProject("project-1", "Research"),
        )
        provider._list_project_chats_data = AsyncMock(
            side_effect=[Page((chat1,), "cursor-2"), Page((chat2,), None)]
        )
        provider._move_chat_data = AsyncMock()
        await provider.open()
        self.addAsyncCleanup(provider.close)

        project = AsyncProject(title="Research", provider=provider, remote_id="project-1")
        items = await project.chats.list(limit=None)
        self.assertEqual([item.remote_id for item in items], ["chat-1", "chat-2b"])
        self.assertIs(items[0].provider, provider)
        self.assertIs(items[0].project, project)

        chat = AsyncGrokChat(title="Loose chat", provider=provider, remote_id="chat-2")
        self.assertIsNone(chat.project)
        await chat.move_to(project)
        provider._move_chat_data.assert_awaited_once_with("chat-2", "project-1")
        self.assertIsNotNone(chat.project)
        assert chat.project is not None
        self.assertEqual(chat.project.remote_id, "project-1")
        with self.assertRaises(UnsupportedOperationError):
            await chat.move_to(project)

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
        self.provider._stream_provider_events = stream_provider._stream_provider_events
        chat = await self.provider.chats.create()
        operation = chat.responses.create("prompt")
        events = [event async for event in operation.stream()]

        self.assertIsInstance(operation, AsyncMessageOperation)
        self.assertEqual(stream_provider.requests[0].prompt, "prompt")
        self.assertEqual(chat.remote_id, "conversation-1")
        self.assertTrue(any(isinstance(event, SourceDiscovered) for event in events))
        self.assertTrue(any(isinstance(event, ArtifactDiscovered) for event in events))
        source = next(event.source for event in events if isinstance(event, SourceDiscovered))
        artifact = next(event.artifact for event in events if isinstance(event, ArtifactDiscovered))
        self.assertEqual(source.id, "source-1")
        self.assertEqual(artifact.id, "file-1")

    async def test_download_rejects_unresolved_user_url(self) -> None:
        self.provider._resolve_download.return_value = None
        chat = await self.provider.chats.create()
        chat.remote_id = "conversation-1"
        with self.assertRaises(UnsupportedOperationError):
            await chat.download("https://example.test/file")
        self.provider._resolve_download.assert_awaited_once_with(
            "https://example.test/file", conversation_id="conversation-1", message_id=None
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
        self.provider._stream_provider_events = stream_provider._stream_provider_events
        chat = await self.provider.chats.create()
        operation = chat.responses.create("solve")
        self.assertEqual(operation.state, OperationState.PENDING)
        events = [event async for event in operation.stream()]
        response = await operation.wait()

        self.assertEqual(operation.state, OperationState.COMPLETED)
        self.assertEqual(operation.message.id, "msg-user-99")
        self.assertEqual(response.id, "msg-assistant-99")
        self.assertEqual(response.text, "Final output")
        self.assertIs(response.message, operation.message)
        self.assertEqual(response.message.response, response)
        self.provider._get_chat_data.return_value = Conversation(
            "conversation-2",
            "Solved",
            messages=(
                NativeMessage("msg-user-99", "user", (Content("text", "solve"),)),
                NativeMessage("msg-assistant-99", "assistant", (Content("text", "Final output"),)),
            ),
        )
        self.assertIn("msg-assistant-99", [r.id for r in await chat.responses.list()])
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
        remote_conv = Conversation(
            "conv-remote-1",
            "Loaded Title",
            messages=(
                NativeMessage("u-1", "user", (Content("text", "Hello"),)),
                NativeMessage("a-think", "assistant", (Content("text", "Thinking"),), "analysis"),
                NativeMessage("a-1", "assistant", (Content("text", "Hi there!"),)),
            ),
            metadata={"default_model_slug": "sonnet"},
        )
        self.provider._get_chat_data.return_value = remote_conv
        chat = await self.provider.chats.create(title="")
        chat.remote_id = "conv-remote-1"
        responses = await chat.responses.list(limit=None)
        self.assertEqual(chat.title, "Loaded Title")
        self.assertEqual(chat.metadata, {"default_model_slug": "sonnet"})
        self.assertEqual(len(responses), 1)
        self.assertEqual(responses[0].id, "a-1")
        self.assertEqual(responses[0].text, "Hi there!")
        self.assertEqual(responses[0].message.id, "u-1")
        self.assertEqual(responses[0].message.role, "user")
        self.assertEqual(responses[0].message.content, "Hello")
        self.assertIsNone(responses[0].timing)

    async def test_chats_include_projects_and_temporary_flag(self) -> None:
        account = Account.from_cookies("chatgpt", {"session_token": "tok"})
        provider = AsyncProvider(account)
        provider._prepare_session = AsyncMock()
        provider._close_session = AsyncMock()
        project_alpha = NativeProject("proj-1", "Alpha")
        loose_conv = Conversation(
            "chat-loose",
            "Loose",
            metadata={"temporary": False, "source": "list"},
        )
        shared_conv = Conversation("chat-shared", "Shared In Main")
        project_shared_conv = Conversation(
            "chat-shared",
            "Shared In Project",
            project=project_alpha,
        )
        project_only_conv = Conversation(
            "chat-proj",
            "Project Only",
            project=project_alpha,
        )
        from webaiprovider.base import Capability
        from webaiprovider.errors import FeatureUnavailable

        provider._get_chat_data = AsyncMock(side_effect=FeatureUnavailable(Capability.CHAT))
        provider._list_chats_data = AsyncMock(return_value=Page((loose_conv, shared_conv), None))
        provider._list_projects_data = AsyncMock(return_value=Page((project_alpha,), None))
        provider._list_project_chats_data = AsyncMock(
            return_value=Page((project_shared_conv, project_only_conv), None)
        )
        await provider.open()
        self.addAsyncCleanup(provider.close)

        main_page = await provider.chats.list(limit=None)
        self.assertEqual([c.remote_id for c in main_page], ["chat-loose", "chat-shared"])
        self.assertEqual(main_page[0].metadata, {"temporary": False, "source": "list"})
        self.assertTrue(all(c.project is None for c in main_page))
        provider._list_projects_data.assert_not_awaited()
        provider._list_project_chats_data.assert_not_awaited()
        self.assertEqual(await provider.chats.search("Project Only"), [])

        all_page = await provider.chats.list(limit=None, include_projects=True)
        self.assertEqual(
            [c.remote_id for c in all_page],
            ["chat-loose", "chat-shared", "chat-proj"],
        )
        provider._list_projects_data.assert_awaited()
        provider._list_project_chats_data.assert_awaited_with("proj-1", cursor=None)
        shared = await provider.chats.get("chat-shared", include_projects=True)
        self.assertEqual(shared.title, "Shared In Project")
        self.assertIsNotNone(shared.project)
        assert shared.project is not None
        self.assertEqual(shared.project.remote_id, "proj-1")
        self.assertEqual(shared.project.title, "Alpha")
        matches = await provider.chats.search("Project Only", include_projects=True)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].remote_id, "chat-proj")

        temp_chat = await provider.chats.create("Incognito", temporary=True)
        self.assertTrue(temp_chat.temporary)
        op = temp_chat.responses.create("secret")
        ctx = provider.runtime.new_context(
            "chat.send.prepare",
            resource_id=temp_chat.id,
            operation=op,
            prompt=op.message.content,
            model=op.message.model,
            attachments=tuple(op.message.attachments),
            parameters={"chat": temp_chat, "message": op.message},
        )
        prepared = await run_pipeline_unwrapped(provider._send_pipeline, ctx)
        self.assertTrue(prepared.request.temporary)


class ProviderResourceFamilyTests(unittest.TestCase):
    def test_resource_types_survive_history_generation_and_sync_wrapping(self) -> None:
        for provider_id, name in (
            ("chatgpt", "ChatGPT"),
            ("claude", "Claude"),
            ("gemini", "Gemini"),
            ("grok", "Grok"),
        ):
            with self.subTest(provider=provider_id):
                module = import_module(f"webaiprovider.providers.{provider_id}.resources")
                provider = Provider(Account.from_cookies(provider_id, {}))
                runtime_provider = provider._provider
                runtime_provider._prepare_session = AsyncMock()
                runtime_provider._close_session = AsyncMock()
                runtime_provider._get_chat_data = AsyncMock(
                    return_value=Conversation(
                        "chat-1",
                        "Fixture",
                        (
                            NativeMessage("user-1", "user", (Content("text", "Question"),)),
                            NativeMessage(
                                "answer-1",
                                "assistant",
                                (Content("text", "Answer"),),
                                metadata={
                                    "sources": [{"url": "https://example.test"}],
                                    "files": [{"id": "artifact-1"}],
                                },
                            ),
                        ),
                    )
                )
                runtime_provider._get_project_data = AsyncMock(
                    return_value=NativeProject(
                        "project-1", "Fixture", resources=(Attachment("attachment-1", "note.txt"),)
                    )
                )
                scripted = StreamTestProvider(
                    provider_id,
                    "default",
                    (
                        TextDelta("Next answer"),
                        MessageMetadata(message_id="answer-2"),
                        ResponseComplete(),
                    ),
                )
                runtime_provider._stream_provider_events = scripted._stream_provider_events
                with provider:
                    chat, responses = provider.chats.get_with_responses("chat-1", limit=1)
                    response = responses[0]
                    self.assertIs(type(chat), getattr(module, f"{name}Chat"))
                    self.assertIs(type(chat._resource), getattr(module, f"Async{name}Chat"))
                    self.assertIs(type(response), getattr(module, f"{name}Response"))
                    self.assertIs(type(response.message), getattr(module, f"{name}Message"))
                    self.assertIs(
                        type(response.message._resource), getattr(module, f"Async{name}Message")
                    )
                    self.assertIs(
                        type(next(iter(response.sources))), getattr(module, f"{name}Source")
                    )
                    self.assertIs(
                        type(next(iter(response.artifacts))), getattr(module, f"{name}Artifact")
                    )
                    project = provider.projects.get("project-1")
                    self.assertIs(type(project), getattr(module, f"{name}Project"))
                    self.assertIs(type(project._resource), getattr(module, f"Async{name}Project"))
                    self.assertIs(
                        type(project.attachments.list()[0]), getattr(module, f"{name}Attachment")
                    )
                    sent = chat.ask("Next question")
                    self.assertIs(type(sent), type(response))
                    self.assertIs(type(sent.message), type(response.message))
                    self.assertEqual(sent.remote_id, "answer-2")
