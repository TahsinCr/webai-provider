from __future__ import annotations

import asyncio
import threading
import unittest
from dataclasses import replace
from importlib import import_module
from unittest.mock import ANY, AsyncMock, Mock

from tests.streaming_support import StreamTestProvider
from webaiprovider import (
    AsyncMessageOperation,
    AsyncWebAIProvider,
    OperationEvent,
    OperationState,
    WebAIAccount,
    WebAIProvider,
)
from webaiprovider.collections import Page
from webaiprovider.core.concurrency import maybe_await
from webaiprovider.errors import UnsupportedOperationError, WebAIProviderError
from webaiprovider.resources.chat.types import Conversation
from webaiprovider.resources.message.attachment import Attachment
from webaiprovider.resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    PartClosed,
    ProviderRequest,
    ResponseComplete,
    ResponseVerifying,
    TextDelta,
    TextReplacement,
    ThoughtDelta,
)
from webaiprovider.resources.message.request import async_run_pipeline
from webaiprovider.resources.message.types import Content
from webaiprovider.resources.message.types import MessageData as NativeMessage
from webaiprovider.resources.project.types import ProjectData as NativeProject
from webaiprovider.resources.response.operation import (
    ArtifactDiscovered,
    OutputBegin,
    OutputReplace,
    SourceDiscovered,
    ThinkingBegin,
    ThinkingDelta,
    ThinkingEnd,
)
from webaiprovider.resources.response.parts import TextPart, ThinkingPart


class ChatPreparationTests(unittest.IsolatedAsyncioTestCase):
    async def test_prepare_reuses_context_in_the_next_send_for_each_mode(self) -> None:
        for provider_type, mock in ((WebAIProvider, Mock), (AsyncWebAIProvider, AsyncMock)):
            for name in ("claude", "gemini", "grok"):
                with self.subTest(mode=provider_type.__name__, provider=name):
                    provider = provider_type(WebAIAccount.from_cookies(name, {}))
                    provider._prepare_session = mock()
                    provider._close_session = mock()
                    await maybe_await(provider.open())
                    try:
                        await self.check_prepared_send(provider, mock, name)
                    finally:
                        await maybe_await(provider.close())

    async def check_prepared_send(self, provider, mock, name: str) -> None:
        if name == "claude":
            cid = "11111111-1111-4111-8111-111111111111"
            organization = "22222222-2222-4222-8222-222222222222"
            path = f"/api/organizations/{organization}/chat_conversations/{cid}"
            client = provider.client
            client.organization = organization
            client.http.request = read = mock(return_value={"current_leaf_message_uuid": "parent"})
        elif name == "gemini":
            cid = "c_prepare"
            turn = [[cid, "r_prepare"], None, [["Hi"]], [[["rc_prepare", ["Hello"]]]]]
            provider.rpc.call = read = mock(return_value=[[[turn]]])
            project_id = "notebooks/abcdef"
            project = provider.projects._new(remote_id=project_id)
            native = [None, [None] * 12 + [[1, "project context"]]]
            provider.notebooks.get = project_read = mock(
                return_value=NativeProject(project_id, "Notebook", metadata={"native": native})
            )
        else:
            cid = "conversation"
            rows = [
                {"responseId": "user", "sender": "human"},
                {"responseId": "parent", "parentResponseId": "user", "sender": "assistant"},
            ]
            provider._response_nodes = read = mock(
                return_value={"responseNodes": rows, "lastResponseId": "parent"}
            )

        chat = provider.chats._new(remote_id=cid, project=project if name == "gemini" else None)
        read.assert_not_called()
        await maybe_await(chat.prepare())
        await maybe_await(chat.prepare())

        request = ProviderRequest("Next message", conversation_id=cid)
        if name == "claude":
            parent, _, _ = await maybe_await(
                client._prepare_stream_conversation(cid, path, {"conversation_id": cid}, None)
            )
            self.assertEqual(parent, "parent")
            read.assert_called_once_with("GET", path)
            client._continuations.remove(cid)
            read.return_value = {"uuid": cid}
            await maybe_await(chat.prepare())
            self.assertIsNone(client._continuations.get(cid))
        elif name == "gemini":
            _, metadata, _ = await maybe_await(provider._load_chat_continuation(request, None))
            self.assertEqual(metadata[:3], [cid, "r_prepare", "rc_prepare"])
            self.assertEqual(
                await maybe_await(provider._resolve_project_context(project_id, None)),
                (project_id, [1, "project context"]),
            )
            project_read.assert_called_once_with(project_id)
            read.assert_called_once_with("hNvQHb", [cid, 1, None, 1, [1], [4], None, 1], "/app")
        else:
            parent = await maybe_await(provider._resolve_parent_response_id(request, [], []))
            self.assertEqual(parent, "parent")
            read.assert_called_once_with(cid)


class ProviderResourceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.account = WebAIAccount.from_cookies("claude", {"sessionKey": "session"})
        self.provider = AsyncWebAIProvider(self.account)
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

        async def do_patch(project_id, *, current=None, **fields):
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
            "project-1", "text", name="Notes", text="A note", current=ANY
        )
        self.assertEqual(added.id, "resource-2")

        await self.provider.projects.update(
            project, title="Updated", instruction="New instructions"
        )
        self.provider._patch_project.assert_awaited_once_with(
            "project-1", name="Updated", instructions="New instructions", current=ANY
        )
        self.assertEqual(project.title, "Updated")
        self.assertEqual(await project.instruction.get(), "New instructions")

        self.provider._get_project_data.reset_mock()
        await project.attachments.delete("resource-2")
        self.provider._remove_project_resource.assert_awaited_once_with(
            "project-1", "resource-2", current=ANY
        )
        self.provider._get_project_data.assert_not_awaited()
        state["project"] = original
        loaded_projects = await self.provider.projects.list(limit=None)
        loaded = loaded_projects[0]
        self.assertEqual(loaded.remote_id, "project-1")
        self.assertEqual(await loaded.instruction.get(), "Be concise")
        self.assertEqual((await loaded.attachments.list())[0].id, "resource-1")

    async def test_project_attachment_waiter_uses_the_committed_snapshot(self) -> None:
        project = self.provider.projects._new(remote_id="project")
        project._provider_project = None
        first_started, release = asyncio.Event(), asyncio.Event()
        first = Attachment("first", "Notes")
        second = Attachment("second", "Notes")

        async def add(_project_id, _kind, *, current, **_options):
            if current is None:
                first_started.set()
                await release.wait()
                return NativeProject("project", "Project", resources=(first,))
            self.assertEqual([item.id for item in current.resources], ["first"])
            return NativeProject("project", "Project", resources=(second, first))

        self.provider._add_project_resource = AsyncMock(side_effect=add)
        first_task = asyncio.create_task(project.attachments.create("A", "text", name="Notes"))
        await asyncio.wait_for(first_started.wait(), 1)
        second_task = asyncio.create_task(project.attachments.create("B", "text", name="Notes"))
        await asyncio.sleep(0)
        release.set()
        attachments = await asyncio.wait_for(asyncio.gather(first_task, second_task), 1)
        self.assertEqual([attachment.id for attachment in attachments], ["first", "second"])

    async def test_project_chat_pages_and_move_actions(self) -> None:
        from webaiprovider.providers.grok import AsyncGrokChat
        from webaiprovider.resources.project.project import AsyncProject

        account = WebAIAccount.from_cookies("grok", {"sso": "session"})
        provider = AsyncWebAIProvider(account)
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
        provider._get_chat_metadata = AsyncMock(
            side_effect=(Conversation("chat-2", "Loose chat"), chat1)
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
        provider._move_chat_data.assert_awaited_once_with(
            "chat-2", "project-1", source_project_id=None
        )
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
        self.assertIsInstance(events[0], ThinkingBegin)
        self.assertEqual(
            [event.delta for event in events if isinstance(event, ThinkingDelta)],
            ["Step 1", " and Step 2"],
        )
        thinking_end = next(event for event in events if isinstance(event, ThinkingEnd))
        self.assertEqual(thinking_end.text, "Step 1 and Step 2")
        self.assertTrue(any(isinstance(event, OutputBegin) for event in events))
        self.assertTrue(any(isinstance(event, OutputReplace) for event in events))

    async def test_ordered_live_parts_and_prepare_wait_for_verification(self) -> None:
        release = asyncio.Event()
        warmed = asyncio.Event()
        chat = await self.provider.chats.create()
        chat.remote_id = "confirmed-chat"

        async def generate(_request):
            yield TextDelta("First thought", "analysis", "t1")
            yield PartClosed("t1", "analysis")
            yield TextDelta("Second thought", "analysis", "t2")
            yield PartClosed("t2", "analysis")
            yield TextDelta("Intro", part_id="a1")
            yield TextDelta("Draft", part_id="a2")
            yield TextReplacement("Answer", part_id="a2")
            yield ResponseComplete()
            yield ResponseVerifying()
            await release.wait()
            yield MessageMetadata(message_id="reply-1")

        async def warm(target):
            self.assertIs(target, chat)
            warmed.set()

        self.provider._stream_provider_events = generate
        self.provider._warm_chat = warm

        async def delete_remote(_remote_id):
            self.assertIsNotNone(op.response)
            self.assertEqual(
                (await op.wait_for(OperationEvent.STATUS_TERMINAL)).status, "completed"
            )

        self.provider._delete_chat_data = AsyncMock(side_effect=delete_remote)
        op = chat.responses.create("Question")
        first = await op.wait_for(OperationEvent.THINKING_BEGIN, timeout=1)
        second = await op.wait_for(OperationEvent.THINKING_BEGIN, after=first, timeout=1)
        self.assertNotEqual(first.part_id, second.part_id)
        self.assertIsNotNone(await op.wait_for(OperationEvent.VERIFICATION_BEGIN, timeout=1))
        terminal_hooks = []

        async def terminal_hook(event):
            # A terminal hook can run another mutation on the same chat.
            await chat.prepare()
            terminal_hooks.append(event)

        chat.hooks.on(OperationEvent.STATUS_TERMINAL, terminal_hook)
        prepare = asyncio.create_task(chat.prepare())
        delete = asyncio.create_task(chat.delete())
        try:
            await asyncio.sleep(0)
            self.assertFalse(warmed.is_set())
            self.provider._delete_chat_data.assert_not_awaited()
            self.assertIsNone(await op.wait_for(OperationEvent.STATUS_TERMINAL, timeout=0.01))
        finally:
            release.set()
        result = await asyncio.wait_for(op.wait(), 1)
        await asyncio.wait_for(prepare, 1)
        await asyncio.wait_for(delete, 1)
        self.provider._delete_chat_data.assert_awaited_once_with("confirmed-chat")
        self.assertEqual(terminal_hooks, [await op.wait_for(OperationEvent.STATUS_TERMINAL)])
        self.assertTrue(warmed.is_set())
        self.assertEqual(result.id, "reply-1")
        self.assertEqual(
            [(type(part), part.text) for part in result.parts],
            [
                (ThinkingPart, "First thought"),
                (ThinkingPart, "Second thought"),
                (TextPart, "Intro"),
                (TextPart, "Answer"),
            ],
        )
        self.assertEqual(result.text, "Intro\nAnswer")
        events = [event.event async for event in op.stream()]
        self.assertEqual(events.count("generation.end"), 1)
        self.assertEqual(events[-3:], ["generation.end", "verification.begin", "status.terminal"])

    async def test_generation_end_does_not_report_failed_verification_as_success(self) -> None:
        async def generate(_request):
            yield TextDelta("Answer")
            yield ResponseComplete()
            yield ResponseVerifying()
            raise WebAIProviderError("Revision could not be confirmed")

        self.provider._stream_provider_events = generate
        chat = await self.provider.chats.create()
        op = chat.responses.create("Question")
        generated = await op.wait_for(OperationEvent.GENERATION_END, timeout=1)
        self.assertIsNotNone(generated)
        terminal = await op.wait_for(OperationEvent.STATUS_TERMINAL, after=generated, timeout=1)
        self.assertEqual(terminal.status, "failed")
        with self.assertRaisesRegex(WebAIProviderError, "could not be confirmed"):
            await op.wait()

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
        account = WebAIAccount.from_cookies("chatgpt", {"session_token": "tok"})
        provider = AsyncWebAIProvider(account)
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
        provider._list_project_chats_data.assert_awaited_with("proj-1", cursor=None, limit=None)
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
        ctx = provider._new_context(
            "chat.send.prepare",
            resource_id=temp_chat.id,
            operation=op,
            prompt=op.message.content,
            model=op.message.model,
            attachments=tuple(op.message.attachments),
            parameters={"chat": temp_chat, "message": op.message},
        )
        prepared = await async_run_pipeline(provider._send_pipeline, ctx)
        self.assertTrue(prepared.request.temporary)


class ProviderResourceFamilyTests(unittest.TestCase):
    def test_sync_prepare_runs_on_caller_and_can_follow_repeated_events(self) -> None:
        provider = WebAIProvider(WebAIAccount.from_cookies("claude", {}))
        provider._prepare_session = Mock()
        provider._close_session = Mock()
        thread_ids = []
        provider._warm_chat = lambda _chat: thread_ids.append(threading.get_ident())

        def generate(_request):
            yield TextDelta("Thought", "analysis", "t1")
            yield TextDelta("Intro", part_id="a1")
            yield TextDelta("More thinking", "analysis", "t2")
            yield TextDelta("Answer", part_id="a2")
            yield ResponseComplete()

        provider._stream_provider_events = generate
        with provider:
            chat = provider.chats.create()
            chat.prepare()
            op = chat.responses.create("hello")
            first = op.wait_for(OperationEvent.THINKING_BEGIN)
            second = op.wait_for(OperationEvent.THINKING_BEGIN, after=first)
            self.assertNotEqual(first.part_id, second.part_id)
            with self.assertRaisesRegex(WebAIProviderError, "paused send"):
                chat.prepare()
            response = op.wait()
            chat.prepare()

        self.assertEqual(thread_ids, [threading.get_ident()] * 2)
        self.assertEqual(
            [type(part) for part in response.parts],
            [ThinkingPart, TextPart, ThinkingPart, TextPart],
        )
        self.assertEqual(response.text, "Intro\nAnswer")

    def test_resource_types_survive_history_generation_and_sync_wrapping(self) -> None:
        for provider_id, name in (
            ("chatgpt", "ChatGPT"),
            ("claude", "Claude"),
            ("gemini", "Gemini"),
            ("grok", "Grok"),
        ):
            with self.subTest(provider=provider_id):
                module = import_module(f"webaiprovider.providers.{provider_id}.resources")
                provider = WebAIProvider(WebAIAccount.from_cookies(provider_id, {}))
                provider._prepare_session = Mock()
                provider._close_session = Mock()
                provider._get_chat_data = Mock(
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
                provider._get_project_data = Mock(
                    return_value=NativeProject(
                        "project-1", "Fixture", resources=(Attachment("attachment-1", "note.txt"),)
                    )
                )

                def _stream_sync(_request):
                    yield TextDelta("Next answer")
                    yield MessageMetadata(message_id="answer-2")
                    yield ResponseComplete()

                provider._stream_provider_events = _stream_sync
                with provider:
                    chat, responses = provider.chats.get_with_responses("chat-1", limit=1)
                    response = responses[0]
                    self.assertIs(type(chat), getattr(module, f"{name}Chat"))
                    self.assertFalse(hasattr(chat, "_resource"))
                    self.assertIs(type(response), getattr(module, f"{name}Response"))
                    self.assertIs(type(response.message), getattr(module, f"{name}Message"))
                    self.assertFalse(hasattr(response.message, "_resource"))
                    self.assertIs(
                        type(next(iter(response.sources))), getattr(module, f"{name}Source")
                    )
                    self.assertIs(
                        type(next(iter(response.artifacts))), getattr(module, f"{name}Artifact")
                    )
                    project = provider.projects.get("project-1")
                    self.assertIs(type(project), getattr(module, f"{name}Project"))
                    self.assertFalse(hasattr(project, "_resource"))
                    self.assertIs(
                        type(project.attachments.list()[0]), getattr(module, f"{name}Attachment")
                    )
                    draft = chat.responses.create("With attachment")
                    attachment = draft.message.attachments.create(b"note", name="draft.txt")
                    self.assertIs(type(attachment), getattr(module, f"{name}Attachment"))
                    sent = chat.ask("Next question")
                    self.assertIs(type(sent), type(response))
                    self.assertIs(type(sent.message), type(response.message))
                    self.assertEqual(sent.remote_id, "answer-2")

                    standalone = provider.ask("New conversation")
                    self.assertIs(type(standalone), type(response))
                    self.assertIs(type(standalone.chat), type(chat))
                    self.assertIs(type(standalone.message), type(response.message))

    def test_sync_response_parts_are_populated_at_generation_end_for_all_providers(self) -> None:
        from webaiprovider import (
            GenerationEnd,
            MemoryCredentialStore,
            WebAIAccount,
            WebAIProvider,
            WebAIProviderType,
        )

        for ptype in WebAIProviderType:
            with self.subTest(provider=ptype.value):
                account = WebAIAccount(
                    id="probe", provider=ptype.value, credentials=MemoryCredentialStore()
                )
                provider = WebAIProvider(account, type=ptype)
                provider._opened = True

                def _stream_sync(_request):
                    yield ThoughtDelta("Plan", part_id="t1")
                    yield TextDelta("Final answer", part_id="a1")
                    yield ResponseComplete()
                    yield ResponseVerifying()
                    yield MessageMetadata(message_id="msg-final")

                provider._stream_provider_events = _stream_sync
                with provider:
                    chat = provider.chats._new(id="chat-1", remote_id="chat-1")
                    op = chat.responses.create("Prompt")
                    seen_at_generation_end = None
                    for event in op.stream():
                        if isinstance(event, GenerationEnd):
                            seen_at_generation_end = op.response
                            self.assertIsNotNone(seen_at_generation_end)
                            self.assertEqual(
                                [(type(p), p.text) for p in seen_at_generation_end.parts],
                                [(ThinkingPart, "Plan"), (TextPart, "Final answer")],
                            )
                            self.assertEqual(seen_at_generation_end.text, "Final answer")
                            self.assertIn(seen_at_generation_end, chat.responses._turns)
                    final = op.wait()
                    self.assertIs(final, seen_at_generation_end)
                    self.assertEqual(final.remote_id, "msg-final")
