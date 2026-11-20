from __future__ import annotations

import asyncio
import threading
import unittest
from functools import partial
from importlib import import_module
from unittest.mock import ANY, AsyncMock, patch

import webaiprovider
from tests.streaming_support import StreamTestProvider
from webaiprovider import (
    Account,
    AmbiguousMatchError,
    Artifact,
    AsyncChatGPTProvider,
    AsyncGrokProvider,
    AsyncMessage,
    AsyncMessageOperation,
    AsyncProvider,
    Attachment,
    Attachments,
    ChatGPTModels,
    ChatGPTProvider,
    ClaudeModels,
    GeminiModels,
    GrokModels,
    Message,
    Models,
    OutputBegin,
    OutputDelta,
    OutputEnd,
    Provider,
    ProviderType,
    Response,
    SelectionResolver,
    Source,
    ThinkingBegin,
    ThinkingDelta,
    ThinkingEnd,
)
from webaiprovider.accounts.status import SessionState, SessionStatus
from webaiprovider.errors import ProviderError, RemoteNotFound, UnsupportedOperationError
from webaiprovider.resources.chat.types import Conversation
from webaiprovider.resources.message.protocol import TextDelta
from webaiprovider.resources.model.model import (
    EffortSet,
    ModelRef,
)
from webaiprovider.resources.project.types import ProjectData


class TestApi(unittest.IsolatedAsyncioTestCase):
    async def open_provider(
        self,
        account: Account,
        provider: AsyncProvider | None = None,
    ) -> AsyncProvider:
        provider = provider or AsyncProvider(account)
        for name in (
            "_prepare_session",
            "_close_session",
            "_authenticate",
            "_session_status",
            "_forget_session",
        ):
            setattr(provider, name, AsyncMock())
        provider._load_catalog = AsyncMock()
        for name in (
            "_list_chats_data",
            "_get_chat_data",
            "_branch_chat_data",
            "_conversation_attachment_count",
            "_rename_chat_data",
            "_delete_chat_data",
            "_move_chat_data",
        ):
            setattr(provider, name, AsyncMock())
        for name in (
            "_list_projects_data",
            "_get_project_data",
            "_list_project_chats_data",
            "_create_project_data",
            "_patch_project",
            "_project_options",
            "_add_project_resource",
            "_remove_project_resource",
            "_delete_project_data",
        ):
            setattr(provider, name, AsyncMock())
        provider._upload_file = AsyncMock()
        provider._resolve_download = AsyncMock()
        await provider.open()
        self.addAsyncCleanup(provider.close)
        return provider

    def test_account_creation_and_session(self) -> None:
        acc = Account.from_cookies("chatgpt", {"session_token": "mock-token-xyz"})
        self.assertEqual(acc.provider, "chatgpt")
        self.assertTrue(acc.is_active)
        self.assertIn("session_token", acc.store().load())
        self.assertFalse(hasattr(acc, "connect"))

        provider = AsyncProvider(acc, type=ProviderType.CHATGPT)
        self.assertIsInstance(provider, AsyncProvider)
        self.assertEqual(provider.provider, "chatgpt")
        self.assertIsNotNone(provider.chats)
        self.assertIsNotNone(provider.projects)

    def test_account_browser_selector_is_forwarded(self) -> None:
        with patch(
            "webaiprovider.accounts.importer.import_cookies",
            return_value=("chromium", {"session_token": "mock-token-xyz"}),
        ) as read_cookies:
            Account.from_browser("ChatGPT", browser="brave")

        read_cookies.assert_called_once_with("chatgpt", browser="brave")

    async def test_lazy_message_creation_and_hooks(self) -> None:
        acc = Account.from_cookies("chatgpt", {"cookie": "val"})
        provider = await self.open_provider(acc)
        mock_provider = provider
        chat = await provider.chats.create(title="Test Chat")

        op = chat.responses.create(
            "Hello AI",
            model=ChatGPTModels.GPT_6_THINKING.select(ChatGPTModels.GPT_6_THINKING.efforts.MEDIUM),
        )

        self.assertIsInstance(op, AsyncMessageOperation)
        self.assertEqual(op.message.content, "Hello AI")
        self.assertEqual(op.message.model.model, ChatGPTModels.GPT_6_THINKING)
        self.assertEqual(op.message.model.effort.id, "standard")

        deltas: list[str] = []

        @op.hooks.on("output.delta")
        def on_delta(delta: OutputDelta) -> None:
            deltas.append(delta.delta)

        thoughts: list[str] = []

        def on_thought(td: ThinkingDelta) -> None:
            thoughts.append(td.delta)

        op.hooks.add("thinking.delta", on_thought)

        async def fake_stream(_operation: AsyncMessageOperation):
            yield ThinkingBegin()
            yield ThinkingDelta(delta="Reasoning step 1")
            yield ThinkingEnd()
            yield OutputBegin()
            yield OutputDelta(delta="Hello ")
            yield OutputDelta(delta="World!")
            yield OutputEnd()

        mock_provider._execute_stream = fake_stream
        events = []
        async for ev in op.stream():
            events.append(ev)

        self.assertEqual(len(events), 7)
        self.assertEqual(thoughts, ["Reasoning step 1"])
        self.assertEqual(deltas, ["Hello ", "World!"])

        res = await op
        self.assertIsInstance(res, Response)
        self.assertEqual(res.text, "Hello World!")
        self.assertEqual(res.content, "Hello World!")
        self.assertEqual(res.message.content, "Hello AI")
        self.assertEqual(res.message.role, "user")

        stream_events = [ev async for ev in chat.stream("Hello via stream")]
        self.assertTrue(any(isinstance(ev, OutputDelta) for ev in stream_events))
        asked = await chat.ask("Hello via ask")
        self.assertEqual(asked.text, "Hello World!")
        self.assertEqual(asked.message.content, "Hello via ask")

    async def test_cancelling_one_waiter_does_not_cancel_shared_send(self) -> None:
        provider = await self.open_provider(Account.from_cookies("chatgpt", {"cookie": "val"}))
        chat = await provider.chats.create()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def fake_stream(_operation: AsyncMessageOperation):
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            yield OutputBegin()
            yield OutputDelta(delta="shared result")
            yield OutputEnd()

        provider._execute_stream = fake_stream
        operation = chat.responses.create("prompt")
        first = asyncio.create_task(operation.start())
        second = asyncio.create_task(operation.wait())
        await started.wait()
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first

        release.set()
        response = await second
        self.assertEqual(response.text, "shared result")
        self.assertEqual(calls, 1)

    async def test_hooks_await_future_results_before_continuing(self) -> None:
        from webaiprovider.core.async_hooks import AsyncOperationHooks

        hooks = AsyncOperationHooks()
        pending = asyncio.get_running_loop().create_future()
        hooks.on("output.delta", lambda event: pending)
        emission = asyncio.create_task(hooks.emit(OutputDelta(delta="text")))
        await asyncio.sleep(0)
        self.assertFalse(emission.done())
        pending.set_result(None)
        await emission

    async def test_operation_uses_one_send_across_entrypoints(self) -> None:
        provider = await self.open_provider(Account.from_cookies("chatgpt", {"cookie": "val"}))
        chat = await provider.chats.create()
        entered = asyncio.Event()
        release = asyncio.Event()
        send_count = 0

        async def fake_stream(_operation: AsyncMessageOperation):
            nonlocal send_count
            send_count += 1
            entered.set()
            yield OutputBegin()
            await release.wait()
            yield OutputDelta(delta="done")
            yield OutputEnd()

        async def consume(stream):
            return [event async for event in stream]

        operation = chat.responses.create("send once")
        operation._executor = fake_stream

        stage_wait = asyncio.create_task(operation.wait_for("output.delta", timeout=1))
        missing_stage = asyncio.create_task(operation.wait_for("thinking.end"))
        await asyncio.wait_for(entered.wait(), timeout=1)
        stream_task = asyncio.create_task(consume(operation.stream()))
        start_task = asyncio.create_task(operation.start())
        await asyncio.sleep(0)
        release.set()

        event = await stage_wait
        response = await start_task
        events = await stream_task
        replayed = await consume(operation.stream())

        self.assertIsInstance(event, OutputDelta)
        self.assertIs(await operation.wait_for("output.delta"), event)
        self.assertIsNone(await missing_stage)
        self.assertEqual((await operation.wait_for("status.terminal")).status, "completed")
        self.assertEqual(response.text, "done")
        self.assertEqual(
            [item.event for item in events], ["output.begin", "output.delta", "output.end"]
        )
        self.assertEqual(
            [item.event for item in replayed], ["output.begin", "output.delta", "output.end"]
        )
        self.assertEqual(send_count, 1)

    async def test_message_send_requires_provider_session(self) -> None:
        provider = AsyncProvider(Account.from_cookies("chatgpt", {"cookie": "val"}))
        chat = await provider.chats.create()

        with self.assertRaisesRegex(ProviderError, "Open the provider before using its resources"):
            await chat.responses.create("must not be echoed as a response").wait()

    async def test_response_with_sources_and_artifacts(self) -> None:
        acc = Account.from_cookies("gemini", {"cookie": "gemini-val"})
        provider = await self.open_provider(acc)
        mock_provider = provider
        chat = await provider.chats.create()
        op = chat.responses.create("Explain relativity")

        async def fake_stream(_operation: AsyncMessageOperation):
            yield OutputBegin()
            yield OutputDelta(delta="Relativity explained.")
            from webaiprovider.resources.message.operation import (
                ArtifactDiscovered,
                SourceDiscovered,
            )

            yield SourceDiscovered(
                source=Source(
                    id="s1",
                    url="https://example.com/einstein",
                    title="Einstein Paper",
                    snippet="...",
                )
            )
            yield ArtifactDiscovered(
                artifact=Artifact(
                    id="a1",
                    kind="chart",
                    name="spacetime.png",
                    download_url="https://example.com/spacetime.png",
                )
            )
            yield OutputEnd()

        mock_provider._execute_stream = fake_stream
        response = await op.start()
        self.assertEqual(response.text, "Relativity explained.")
        self.assertEqual(len(response.sources), 1)
        self.assertEqual(response.sources.list()[0].title, "Einstein Paper")
        self.assertEqual(len(response.artifacts), 1)
        self.assertEqual(response.artifacts.list()[0].name, "spacetime.png")

    async def test_chat_lifecycle_methods(self) -> None:
        from webaiprovider.collections import Page
        from webaiprovider.resources.chat.types import Conversation
        from webaiprovider.resources.message.types import Content
        from webaiprovider.resources.message.types import MessageData as NativeMessage

        acc = Account.from_cookies("claude", {"sessionKey": "claude-key"})
        session = await self.open_provider(acc)
        state = {"title": "My Claude Chat", "messages": ()}

        def current_conv() -> Conversation:
            return Conversation(
                "chat-123",
                state["title"],
                state["messages"],
            )

        async def rename_chat(_cid: str, title: str) -> None:
            state["title"] = title

        session._rename_chat_data = AsyncMock(side_effect=rename_chat)
        session._list_chats_data = AsyncMock(side_effect=lambda **_kw: Page((current_conv(),)))
        session._get_chat_data = AsyncMock(side_effect=lambda _cid: current_conv())

        chat = await session.chats.create(title="My Claude Chat")
        chat.remote_id = "chat-123"

        await chat.rename("Renamed Chat")
        session._rename_chat_data.assert_awaited_once_with("chat-123", "Renamed Chat")
        self.assertEqual(chat.title, "Renamed Chat")

        await session.chats.update(chat, title="Updated Via Collection")
        await session.chats.update(chat.remote_id, title="Updated Via ID")
        self.assertEqual(state["title"], "Updated Via ID")
        self.assertEqual(
            (await session.chats.find("Updated", startswith=True)).remote_id, chat.remote_id
        )
        self.assertEqual((await session.chats.find("ID", endswith=True)).remote_id, chat.remote_id)

        with patch(
            "webaiprovider.resources.chat.chat.download_source",
            AsyncMock(return_value=b"file-bytes"),
        ):
            dl = await chat.download("https://example.com/test.png")
        session._resolve_download.assert_awaited_once_with(
            "https://example.com/test.png", conversation_id="chat-123", message_id=None
        )
        self.assertEqual(dl, b"file-bytes")

        op = chat.responses.create("Hello test")
        state["messages"] = (
            NativeMessage(op.message.id, "user", (Content("text", "Hello test"),)),
        )
        self.assertEqual((await chat.get(op.message.id)).message.content, "Hello test")
        await chat.delete()
        session._delete_chat_data.assert_awaited_once_with("chat-123")

    async def test_project_lifecycle_methods(self) -> None:
        from dataclasses import replace

        from webaiprovider.resources.message.attachment import Attachment
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        acc = Account.from_cookies("claude", {"sessionKey": "claude-key"})
        session = await self.open_provider(acc)
        remote = [
            NativeProject(
                "proj-abc",
                "Test Project",
                "Always be concise",
            )
        ]

        async def create_project(name, *, instructions=""):
            remote[0] = replace(remote[0], name=name, instructions=instructions)
            return remote[0]

        async def get_project(_project_id):
            return remote[0]

        async def patch_project(project_id, *, name=None, instructions=None, current=None):
            del project_id
            remote[0] = replace(
                remote[0],
                name=name if name is not None else remote[0].name,
                instructions=instructions if instructions is not None else remote[0].instructions,
            )
            return remote[0]

        async def add_resource(_project_id, kind, **options):
            resource = Attachment(
                f"remote-{options['name']}",
                options["name"],
                "text/plain",
                metadata={"kind": kind},
            )
            remote[0] = replace(remote[0], resources=(*remote[0].resources, resource))
            return remote[0]

        async def remove_resource(_project_id, resource_id, *, current=None):
            remote[0] = replace(
                remote[0],
                resources=tuple(r for r in remote[0].resources if r.id != resource_id),
            )
            return remote[0]

        session._create_project_data.side_effect = create_project
        session._get_project_data.side_effect = get_project
        session._patch_project.side_effect = patch_project
        session._add_project_resource.side_effect = add_resource
        session._remove_project_resource.side_effect = remove_resource

        project = await session.projects.create(
            title="Test Project", instruction="Always be concise"
        )
        project.remote_id = "proj-abc"
        await project.rename("Renamed Project")
        self.assertEqual(project.title, "Renamed Project")

        await project.instruction.edit("New instruction")
        self.assertEqual(await project.instruction.get(), "New instruction")
        await project.instruction.clear()
        self.assertEqual(await project.instruction.get(), "")

        added = await project.attachments.create("Doc", resource_type="text", name="res-1")
        self.assertEqual(added.id, "remote-res-1")
        session._add_project_resource.assert_awaited_once_with(
            "proj-abc", "text", name="res-1", text="Doc", current=ANY
        )
        self.assertEqual(await project.attachments.download(added.id), "Doc")

        await session.projects.update(
            project, title="Updated Project", instruction="Updated Instruction"
        )
        session._patch_project.assert_awaited_with(
            "proj-abc", name="Updated Project", instructions="Updated Instruction", current=ANY
        )
        self.assertEqual(project.title, "Updated Project")
        self.assertEqual(await project.instruction.get(), "Updated Instruction")

        with self.assertRaises(UnsupportedOperationError):
            await project.attachments.update(added.id, content="Updated Doc")
        self.assertEqual((await project.attachments.get(added.id)).content, "Doc")

        await project.attachments.delete(added.id)
        session._remove_project_resource.assert_awaited_once_with("proj-abc", added.id, current=ANY)
        await project.delete()
        session._delete_project_data.assert_awaited_once_with("proj-abc")

    async def test_message_branch_retry_edit(self) -> None:
        from webaiprovider.resources.chat.types import Conversation

        acc = Account.from_cookies("chatgpt", {"session_token": "token"})
        session = await self.open_provider(acc, AsyncChatGPTProvider(acc))
        chat = await session.chats.create()
        chat.remote_id = "remote-chat-id"
        msg = AsyncMessage(
            content="Original prompt",
            role="user",
            id="msg-orig-1",
            metadata={"parent_message_id": "root"},
            chat=chat,
        )

        resp = Response("Original answer", message=msg, id="resp-orig-1", chat=chat)
        retry_op = resp.retry()
        self.assertIsInstance(retry_op, AsyncMessageOperation)
        self.assertEqual(retry_op.message.metadata.get("action"), "retry")
        self.assertEqual(retry_op.message.metadata.get("parent_message_id"), "resp-orig-1")
        edit_op = msg.edit("Edited prompt")
        self.assertEqual(edit_op.message.content, "Edited prompt")
        self.assertEqual(edit_op.message.metadata.get("action"), "edit")
        update_op = chat.responses.update(msg, content="Updated via responses.update")
        self.assertEqual(update_op.message.content, "Updated via responses.update")
        self.assertEqual(update_op.message.metadata.get("action"), "edit")

        branch_meta = {
            "pending_branch": {"conversation_id": "remote-chat-id", "message_id": "resp-orig-1"}
        }
        session._branch_chat_data.return_value = Conversation(
            "branch-chat-id",
            "Branch chat",
            metadata=branch_meta,
        )
        branched_chat = await resp.branch(title="Branch chat")
        session._branch_chat_data.assert_awaited_once_with("remote-chat-id", "resp-orig-1")
        self.assertEqual(branched_chat.remote_id, "branch-chat-id")
        self.assertEqual(branched_chat.metadata, branch_meta)
        branched_chat.metadata["local_only"] = True
        self.assertNotIn("local_only", branch_meta)

        from webaiprovider.providers.chatgpt import ChatGPTProvider

        sync_provider = ChatGPTProvider(acc)
        try:
            sync_branched = sync_provider._wrap_chat(branched_chat)
            self.assertEqual(sync_branched.metadata, {**branch_meta, "local_only": True})
            sync_branched.metadata = {"custom": 1}
            self.assertEqual(branched_chat.metadata, {"custom": 1})
        finally:
            sync_provider.close()

    async def test_generic_constructors_select_native_provider_classes(self) -> None:
        for kind, title, operations in (
            ("chatgpt", "ChatGPT", {"branch", "move_to"}),
            ("claude", "Claude", {"move_to"}),
            ("gemini", "Gemini", {"branch"}),
            ("grok", "Grok", {"move_to"}),
        ):
            account = Account.from_cookies(kind, {})
            with self.subTest(provider=kind):
                native_async = getattr(webaiprovider, f"Async{title}Provider")
                native_sync = getattr(webaiprovider, f"{title}Provider")
                module = f"webaiprovider.providers.{kind}"
                shared = getattr(import_module(f"{module}.base"), f"Base{title}Provider")
                self.assertEqual(native_async.__module__, f"{module}.async_provider")
                self.assertEqual(native_sync.__module__, f"{module}.provider")
                self.assertTrue(issubclass(native_async, shared))
                self.assertTrue(issubclass(native_sync, shared))
                self.assertTrue(issubclass(shared, webaiprovider.BaseProvider))
                asynchronous = AsyncProvider(account)
                synchronous = Provider(account, type=ProviderType.parse(kind))
                try:
                    self.assertIs(type(asynchronous), native_async)
                    self.assertIsInstance(asynchronous, AsyncProvider)
                    self.assertEqual(asynchronous.account_id, account.id)
                    self.assertIs(asynchronous.type, ProviderType.parse(kind))
                    self.assertIs(type(synchronous), native_sync)
                    self.assertIs(type(synchronous._provider), native_async)
                    self.assertIs(synchronous.capabilities, synchronous._provider.capabilities)
                    for chat_type in (
                        type(await asynchronous.chats.create()),
                        type(synchronous.chats._wrap(asynchronous.chats._new())),
                    ):
                        for operation in ("branch", "move_to"):
                            self.assertEqual(
                                callable(getattr(chat_type, operation, None)),
                                operation in operations,
                            )
                finally:
                    await asynchronous.close()
                    synchronous.close()
        with self.assertRaisesRegex(ValueError, "must match"):
            AsyncProvider(Account.from_cookies("chatgpt", {}), type=ProviderType.GROK)

    async def test_provider_open_is_idempotent_and_closes_provider_resources(self) -> None:
        account = Account.from_cookies("gemini", {"cookie": "test"})
        provider = AsyncProvider(account, type=ProviderType.GEMINI)
        provider._prepare_session = AsyncMock()
        provider._close_session = AsyncMock()
        self.assertIs(await provider.open(), provider)
        self.assertIs(await provider.open(), provider)
        provider._prepare_session.assert_awaited_once_with()
        await provider.close()
        provider._close_session.assert_awaited_once_with()

    async def test_chatgpt_project_creation_accepts_memory_share_flag(self) -> None:
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        provider = AsyncChatGPTProvider(account)
        provider._prepare_session = AsyncMock()
        provider._close_session = AsyncMock()
        provider._create_project_data = AsyncMock(
            return_value=NativeProject("project-1", "Private notes")
        )
        await provider.open()
        project = await provider.projects.create("Private notes", memory_share=False)
        self.assertEqual(project.title, "Private notes")
        self.assertEqual(provider._create_project_data.await_args.kwargs["memory_share"], False)
        await provider.close()

    async def test_sync_provider_rejects_blocking_inside_running_loop(self) -> None:
        provider = Provider(Account.from_cookies("chatgpt", {"cookie": "test"}))
        with self.assertRaisesRegex(RuntimeError, "cannot block an event loop"):
            provider.open()
        provider.close()

    def test_catalogs_and_selection_resolver(self) -> None:
        self.assertEqual(ChatGPTModels.GPT_6_INSTANT.id, "gpt-6-instant")
        self.assertEqual(ClaudeModels.SONNET_5_5.id, "claude-sonnet-5-5")
        self.assertEqual(GeminiModels.HYDROGEN.id, "gemini-hydrogen")
        self.assertEqual(GeminiModels.NEON.id, "gemini-neon")
        self.assertEqual(GeminiModels.CARBON.id, "gemini-carbon")
        self.assertEqual(
            [model.id for model in GeminiModels.list()],
            [
                "gemini-hydrogen",
                "gemini-neon",
                "gemini-carbon",
            ],
        )
        self.assertEqual(GrokModels.GROK_3.id, "fast")

        candidates = [
            ModelRef(provider="test", id="fast", name="Model Fast", rank=0, efforts=EffortSet),
            ModelRef(provider="test", id="smart", name="Model Smart", rank=1, efforts=EffortSet),
            ModelRef(provider="test", id="expert", name="Model Expert", rank=2, efforts=EffortSet),
        ]

        self.assertEqual(SelectionResolver.resolve(Models.LOW, candidates).id, "fast")
        self.assertEqual(SelectionResolver.resolve(Models.MEDIUM, candidates).id, "smart")
        self.assertEqual(SelectionResolver.resolve(Models.HIGH, candidates).id, "expert")
        self.assertEqual(
            SelectionResolver.resolve(Models.AUTO, candidates, default_id="smart").id,
            "smart",
        )

        with self.assertRaises(AmbiguousMatchError):
            SelectionResolver.resolve(
                "Model",
                [
                    ModelRef(provider="test", id="m1", name="Model One", efforts=EffortSet),
                    ModelRef(provider="test", id="m2", name="Model Two", efforts=EffortSet),
                ],
            )

    def test_attachments_integration(self) -> None:
        raw_bytes = b"Hello world text data"
        att = Attachment.from_bytes(raw_bytes, name="hello.txt", mime_type="text/plain")
        attachments = Attachments([att])
        self.assertEqual(attachments.total_size, len(raw_bytes))

        msg = Message("With attachment", attachments=attachments)
        self.assertEqual(len(msg.attachments), 1)
        self.assertEqual(msg.attachments.get(att.id).name, "hello.txt")

    async def test_provider_stream_preserves_typed_errors(self) -> None:
        from webaiprovider.errors import RateLimited

        provider = AsyncGrokProvider(Account.from_cookies("grok", {"sso": "grok-tok"}))
        provider._prepare_session = AsyncMock()
        provider._close_session = AsyncMock()

        async def fail_stream(_request):
            raise RateLimited(retry_after=42.0)
            yield TextDelta("unreachable")

        provider._stream_provider_events = fail_stream
        await provider.open()
        chat = await provider.chats.create()
        operation = chat.responses.create("Hello")
        with self.assertRaises(RateLimited) as ctx:
            await operation.start()
        self.assertEqual(ctx.exception.retry_after, 42.0)

        async def fail_validation(_request):
            raise ValueError("invalid effort level")
            yield TextDelta("unreachable")

        provider._stream_provider_events = fail_validation
        with self.assertRaisesRegex(ValueError, "invalid effort level"):
            await chat.responses.create("Hello").start()
        self.assertTrue(repr(provider).startswith("<AsyncGrokProvider "))
        await provider.close()

    async def test_prepare_operation_preserves_attachment_mime_type(self) -> None:
        from webaiprovider.core.operations import run_pipeline_unwrapped

        provider = AsyncGrokProvider(Account.from_cookies("grok", {"sso": "grok-tok"}))
        provider._prepare_session = AsyncMock()
        provider._close_session = AsyncMock()
        await provider.open()
        try:
            chat = await provider.chats.create()
            att = Attachment.from_bytes(b"hello", name="notes.txt", mime_type="text/plain")
            msg = Message("Read", attachments=[att])
            op = chat.responses.create(msg)
            ctx = provider._new_context(
                "chat.send.prepare",
                resource_id=chat.id,
                operation=op,
                prompt=op.message.content,
                model=op.message.model,
                attachments=tuple(op.message.attachments),
                parameters={"chat": chat, "message": op.message},
            )
            request = (await run_pipeline_unwrapped(provider._send_pipeline, ctx)).request
            self.assertEqual(request.attachments[0]["mime_type"], "text/plain")
            self.assertNotIn("mime", request.attachments[0])
            self.assertEqual(request.attachments[0]["bytes"], b"hello")
        finally:
            await provider.close()

    async def test_provider_ask_convenience_method(self) -> None:
        p = AsyncChatGPTProvider(Account.from_cookies("chatgpt", {"session_token": "mock-tok"}))
        p._prepare_session = AsyncMock()
        p._close_session = AsyncMock()

        async def fake_stream(_op):
            yield TextDelta("Direct answer")

        p._stream_provider_events = fake_stream
        await p.open()
        res = await p.ask("What is 2+2?")
        self.assertEqual(res.text, "Direct answer")
        self.assertEqual(res.message.content, "What is 2+2?")
        await p.close()

    async def test_model_resolution_from_string_and_policy(self) -> None:
        from webaiprovider.resources.model import ModelCatalogState
        from webaiprovider.resources.model.model import EffortOption, ModelCatalog
        from webaiprovider.resources.model.model import Model as CatalogModel

        account = Account.from_cookies("chatgpt", {"session_token": "tok"})
        provider = StreamTestProvider(account.provider, account.id, (TextDelta("hi"),))
        provider = await self.open_provider(account, provider)
        provider._load_catalog = AsyncMock(
            return_value=ModelCatalog(
                ModelCatalogState.AVAILABLE,
                (
                    CatalogModel("gpt-6-instant", "GPT-6 Instant"),
                    CatalogModel(
                        "gpt-6-thinking",
                        "GPT-6 Thinking",
                        (
                            EffortOption("standard", "Standard"),
                            EffortOption("extended", "Extended"),
                        ),
                    ),
                ),
                default_model="gpt-6-instant",
            )
        )
        chat = await provider.chats.create()

        with patch.object(
            chat.provider.models, "resolve", wraps=chat.provider.models.resolve
        ) as resolve_spy:
            operation = chat.responses.create("test", model=ChatGPTModels.GPT_6_THINKING)
            await operation.start()
            self.assertEqual(provider.requests[-1].model, "gpt-6-thinking")
            self.assertIsNone(provider.requests[-1].effort)

            operation = chat.responses.create(
                "test", model=ChatGPTModels.GPT_6_THINKING.efforts.HIGH
            )
            await operation.start()
            self.assertEqual(provider.requests[-1].model, "gpt-6-thinking")
            self.assertEqual(provider.requests[-1].effort, "extended")

            operation = chat.responses.create(
                "test", model=ChatGPTModels.GPT_6_THINKING.select("standard")
            )
            await operation.start()
            self.assertEqual(provider.requests[-1].model, "gpt-6-thinking")
            self.assertEqual(provider.requests[-1].effort, "standard")

            with self.assertRaises(RemoteNotFound):
                await chat.responses.create("test", model=GeminiModels.HYDROGEN).start()

            resolve_spy.assert_not_awaited()
            provider._load_catalog.assert_not_awaited()

        operation = chat.responses.create("test", model="gpt-6-instant")
        await operation.start()
        self.assertEqual(provider.requests[-1].model, "gpt-6-instant")
        operation = chat.responses.create("test", model=Models.AUTO)
        await operation.start()
        self.assertEqual(provider.requests[-1].model, "gpt-6-instant")
        operation = chat.responses.create("test", model=Models.HIGH)
        await operation.start()
        self.assertEqual(provider.requests[-1].model, "gpt-6-thinking")

    def test_incomplete_collection_cannot_be_instantiated(self) -> None:
        from webaiprovider.collections import BaseCollection

        with self.assertRaises(TypeError):
            type("IncompleteCollection", (BaseCollection,), {})()

    async def test_include_projects_paginates_all_pages_and_bounds_concurrency(self) -> None:
        project_reads_started = asyncio.Event()
        from webaiprovider.collections import Page
        from webaiprovider.resources.chat.types import Conversation
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        class PaginatedProvider(AsyncChatGPTProvider):
            def __init__(self, account: Account) -> None:
                super().__init__(account)
                self.active_fetches = 0
                self.max_active_fetches = 0
                self.page_calls: list[tuple[str, str | None]] = []
                self.project_page_calls: list[str | None] = []

            async def _prepare_session(self) -> None:
                return None

            async def _close_session(self) -> None:
                return None

            async def _list_chats_data(self, *, cursor: str | None = None, limit: int = 50):
                del cursor, limit
                self.active_fetches += 1
                try:
                    await project_reads_started.wait()
                    return Page(
                        (
                            Conversation("c-loose", "Loose"),
                            Conversation("c-shared", "Shared"),
                        ),
                        None,
                    )
                finally:
                    self.active_fetches -= 1

            async def _list_projects_data(self, *, cursor: str | None = None):
                self.project_page_calls.append(cursor)
                projects = tuple(NativeProject(f"p-{idx}", f"Project {idx}") for idx in range(1, 7))
                return (
                    Page(projects[:3], "projects-page-2") if cursor is None else Page(projects[3:])
                )

            async def _list_project_chats_data(
                self, project_id: str, *, cursor: str | None = None, limit=None
            ):
                project_reads_started.set()
                self.page_calls.append((project_id, cursor))
                self.active_fetches += 1
                self.max_active_fetches = max(self.max_active_fetches, self.active_fetches)
                try:
                    await asyncio.sleep(0)
                finally:
                    self.active_fetches -= 1

                p1 = NativeProject("p-1", "Project 1")
                p2 = NativeProject("p-2", "Project 2")
                if project_id == "p-1":
                    if cursor is None:
                        return Page(
                            (Conversation("c-shared", "Shared", project=p1),),
                            "p1-page-2",
                        )
                    if cursor == "p1-page-2":
                        return Page(
                            (Conversation("c-p1-2", "P1 Two", project=p1),),
                            None,
                        )
                if project_id == "p-2":
                    if cursor is None:
                        return Page(
                            (Conversation("c-p2-1", "P2 One", project=p2),),
                            "p2-page-2",
                        )
                    if cursor == "p2-page-2":
                        return Page(
                            (
                                Conversation("c-p2-1", "P2 One Dup", project=p2),
                                Conversation("c-p2-2", "P2 Two", project=p2),
                            ),
                            "p2-page-3",
                        )
                    if cursor == "p2-page-3":
                        return Page(
                            (Conversation("c-p2-3", "P2 Three", project=p2),),
                            None,
                        )
                return Page((), None)

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        async with PaginatedProvider(account) as provider:
            chats = await asyncio.wait_for(
                provider.chats.list(limit=None, include_projects=True), timeout=2
            )

        self.assertLessEqual(provider.max_active_fetches, 4)
        self.assertEqual(provider.project_page_calls, [None, "projects-page-2"])
        self.assertIn(("p-1", None), provider.page_calls)
        self.assertIn(("p-1", "p1-page-2"), provider.page_calls)
        self.assertIn(("p-2", None), provider.page_calls)
        self.assertIn(("p-2", "p2-page-2"), provider.page_calls)
        self.assertIn(("p-2", "p2-page-3"), provider.page_calls)
        ids = [chat.remote_id for chat in chats]
        self.assertEqual(ids, ["c-loose", "c-shared", "c-p1-2", "c-p2-1", "c-p2-2", "c-p2-3"])
        shared = next(chat for chat in chats if chat.remote_id == "c-shared")
        self.assertIsNotNone(shared.project)
        assert shared.project is not None
        self.assertEqual(shared.project.remote_id, "p-1")

    async def test_include_projects_rejects_repeated_cursor_and_cancels_siblings(self) -> None:
        from webaiprovider.collections import Page
        from webaiprovider.resources.chat.types import Conversation
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        cancelled = asyncio.Event()

        class RepeatingProvider(AsyncChatGPTProvider):
            async def _prepare_session(self) -> None:
                return None

            async def _close_session(self) -> None:
                return None

            async def _list_chats_data(self, *, cursor: str | None = None, limit: int = 50):
                del cursor, limit
                return Page((Conversation("c-loose", "Loose"),), None)

            async def _list_projects_data(self, *, cursor: str | None = None):
                del cursor
                return Page(
                    (
                        NativeProject("p-1", "Alpha"),
                        NativeProject("p-2", "Beta"),
                    ),
                    None,
                )

            async def _list_project_chats_data(
                self, project_id: str, *, cursor: str | None = None, limit=None
            ):
                del cursor
                if project_id == "p-1":
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        cancelled.set()
                        raise
                if project_id == "p-2":
                    return Page((), "repeat-cursor")
                return Page((), None)

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        async with RepeatingProvider(account) as provider:
            with self.assertRaisesRegex(ProviderError, "cursor"):
                await provider.chats.list(limit=None, include_projects=True)
            self.assertTrue(cancelled.is_set())

    async def test_include_projects_drains_sibling_when_top_level_listing_fails(self) -> None:
        started = asyncio.Event()
        cancelled = asyncio.Event()

        class FailingProvider(AsyncChatGPTProvider):
            async def _prepare_session(self):
                return None

            async def _close_session(self):
                return None

            async def _list_chats_data(self, **options):
                await started.wait()
                raise ProviderError("conversation listing failed")

            async def _list_projects_data(self, **options):
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        async with FailingProvider(account) as provider:
            with self.assertRaisesRegex(ProviderError, "conversation listing failed"):
                await asyncio.wait_for(provider.chats.list(include_projects=True), timeout=2)
            self.assertTrue(cancelled.is_set())

    async def test_include_projects_stops_early_when_limit_is_bounded(self) -> None:
        from webaiprovider.collections import Page
        from webaiprovider.resources.chat.types import Conversation
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        class BoundedProvider(AsyncChatGPTProvider):
            def __init__(self, account: Account) -> None:
                super().__init__(account)
                self.chat_cursors: list[str | None] = []
                self.project_chat_calls: list[tuple[str, str | None]] = []

            async def _prepare_session(self) -> None:
                return None

            async def _close_session(self) -> None:
                return None

            async def _list_chats_data(self, *, cursor: str | None = None, limit: int = 50):
                self.chat_cursors.append(cursor)
                if cursor is not None:
                    raise AssertionError("normal chat pagination should stop after limit=2")
                return Page(
                    tuple(Conversation(f"c-{i}", f"Chat {i}") for i in range(limit)),
                    "page-2",
                )

            async def _list_projects_data(self, *, cursor: str | None = None):
                del cursor
                return Page((NativeProject("p-1", "Project 1"),), None)

            async def _list_project_chats_data(
                self, project_id: str, *, cursor: str | None = None, limit=None
            ):
                self.project_chat_calls.append((project_id, cursor))
                if cursor is not None:
                    raise AssertionError("project chat pagination should stop after limit=2")
                p1 = NativeProject("p-1", "Project 1")
                return Page(
                    (
                        Conversation("cp-1", "P1 One", project=p1),
                        Conversation("cp-2", "P1 Two", project=p1),
                        Conversation("cp-3", "P1 Three", project=p1),
                    ),
                    "p1-page-2",
                )

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        async with BoundedProvider(account) as provider:
            chats = await provider.chats.list(limit=2, include_projects=True)
        self.assertEqual([chat.remote_id for chat in chats], ["c-0", "c-1"])
        self.assertEqual(provider.chat_cursors, [None])
        self.assertEqual(provider.project_chat_calls, [("p-1", None)])

        async with BoundedProvider(account) as provider:
            provider._list_chats_data = AsyncMock(return_value=Page(()))
            first = Conversation("cp-1", "P1 One")
            second = Conversation("cp-2", "P1 Two")
            provider._list_project_chats_data = AsyncMock(
                side_effect=[Page((first, first), "next"), Page((first, second))]
            )
            chats = await provider.chats.list(limit=2, include_projects=True)
            self.assertEqual([chat.remote_id for chat in chats], ["cp-1", "cp-2"])
            self.assertEqual(provider._list_project_chats_data.await_count, 2)

    async def test_project_refresh_rejects_repeated_cursor_cleanly(self) -> None:
        from webaiprovider.collections import Page

        class RepeatingProjectProvider(AsyncChatGPTProvider):
            def __init__(self, account: Account) -> None:
                super().__init__(account)
                self.project_cursors: list[str | None] = []

            async def _prepare_session(self) -> None:
                return None

            async def _close_session(self) -> None:
                return None

            async def _list_projects_data(self, *, cursor: str | None = None):
                self.project_cursors.append(cursor)
                return Page((), "same-page")

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        async with RepeatingProjectProvider(account) as provider:
            with self.assertRaisesRegex(ProviderError, "cursor") as ctx:
                await provider.projects.list(limit=None)
            self.assertEqual(getattr(ctx.exception, "partial_count", None), 0)
            self.assertEqual(provider.project_cursors, [None, "same-page"])


class TestSyncApi(unittest.TestCase):
    def test_sync_chat_project_and_stream_run_on_the_caller(self) -> None:
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        class RecordingChatGPTProvider(AsyncChatGPTProvider):
            def __init__(self, account: Account, **options) -> None:
                self.thread_ids: list[int] = []
                self.memory_share = None
                super().__init__(account, **options)

            async def _prepare_session(self) -> None:
                self.thread_ids.append(threading.get_ident())

            async def _close_session(self) -> None:
                self.thread_ids.append(threading.get_ident())

            async def _create_project_data(self, name, instructions="", **opts):
                self.thread_ids.append(threading.get_ident())
                self.memory_share = opts.get("memory_share", True)
                return NativeProject("project-1", name, instructions)

            async def _stream_provider_events(self, _request):
                self.thread_ids.append(threading.get_ident())
                yield TextDelta("done")

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        with (
            patch.object(
                ChatGPTProvider, "_make_async_provider", side_effect=RecordingChatGPTProvider
            ),
            ChatGPTProvider(account) as provider,
        ):
            project = provider.projects.create("Notes", memory_share=False)
            chat = provider.chats.create("Draft", project=project)

            @provider.hooks.before("chat.send")
            def before_send(context):
                self.assertEqual(threading.get_ident(), caller)
                calls.append(context.prompt)

            caller = threading.get_ident()
            calls = []
            operation = chat.responses.create("hello")
            events = list(operation.stream())
            response = operation.wait()

        self.assertEqual(calls, ["hello"])
        self.assertEqual(response.text, "done")
        self.assertEqual(
            [event.event for event in events],
            ["output.begin", "output.delta", "output.end"],
        )
        self.assertFalse(provider._provider.memory_share)
        self.assertTrue(provider._provider.thread_ids)
        self.assertEqual(set(provider._provider.thread_ids), {threading.get_ident()})

    def test_sync_include_projects_and_chatgpt_move_to(self) -> None:
        from webaiprovider.base import Capability
        from webaiprovider.collections import Page
        from webaiprovider.errors import FeatureUnavailable
        from webaiprovider.resources.chat.types import Conversation
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        class RecordingChatGPTProvider(AsyncChatGPTProvider):
            def __init__(self, account: Account, **options) -> None:
                self.moved: list[tuple[str, str | None]] = []
                super().__init__(account, **options)

            async def _prepare_session(self) -> None:
                return None

            async def _close_session(self) -> None:
                return None

            async def _list_projects_data(self, *, cursor: str | None = None):
                del cursor
                return Page(
                    (
                        NativeProject("p-1", "Alpha"),
                        NativeProject("p-2", "Beta"),
                    ),
                    None,
                )

            async def _list_project_chats_data(
                self, project_id: str, *, cursor: str | None = None, limit=None
            ):
                del cursor
                if project_id == "p-1":
                    return Page(
                        (
                            Conversation(
                                "c-proj",
                                "Proj Chat",
                                project=NativeProject("p-1", "Alpha"),
                            ),
                        ),
                        None,
                    )
                return Page((), None)

            async def _list_chats_data(self, *, cursor: str | None = None, limit: int = 50):
                del cursor, limit
                return Page(
                    (Conversation("c-loose", "Loose"),),
                    None,
                )

            async def _get_chat_data(self, conversation_id: str):
                del conversation_id
                raise FeatureUnavailable(Capability.CHAT)

            async def _get_project_data(self, project_id: str):
                del project_id
                raise FeatureUnavailable(Capability.PROJECTS)

            async def _get_chat_metadata(self, conversation_id: str) -> Conversation:
                project_id = self.moved[-1][1] if self.moved else None
                project = ProjectData(project_id, "") if project_id else None
                return Conversation(conversation_id, "Loose", project=project)

            async def _move_chat_data(
                self, conversation_id: str, project_id: str | None, *, source_project_id: str | None
            ) -> None:
                self.moved.append((conversation_id, project_id))

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        with (
            patch.object(
                ChatGPTProvider, "_make_async_provider", side_effect=RecordingChatGPTProvider
            ),
            ChatGPTProvider(account) as provider,
        ):
            self.assertEqual(provider.chats.search("Proj Chat"), [])
            found = provider.chats.search("Proj Chat", include_projects=True)
            self.assertEqual(len(found), 1)
            self.assertIsNotNone(found[0].project)
            assert found[0].project is not None
            self.assertEqual(found[0].project.remote_id, "p-1")
            self.assertEqual(found[0].project.title, "Alpha")

            loose = provider.chats.get("c-loose")
            self.assertIsNone(loose.project)
            p1 = provider.projects.get("p-1")
            p2 = provider.projects.get("p-2")

            loose.move_to(p1)
            self.assertIsNotNone(loose.project)
            assert loose.project is not None
            self.assertEqual(loose.project.remote_id, "p-1")

            moved = loose.move_to(p2)
            self.assertIsNotNone(moved.project)
            assert moved.project is not None
            self.assertEqual(moved.project.remote_id, "p-2")

            detached = moved.move_to(None)
            self.assertIsNone(detached.project)
            self.assertEqual(
                provider._provider.moved,
                [("c-loose", "p-1"), ("c-loose", "p-2"), ("c-loose", None)],
            )


class TestApiOptimizationContracts(unittest.IsolatedAsyncioTestCase):
    async def test_reauthentication_invalidates_loaded_models_on_both_auth_paths(self) -> None:
        for provider_type in (AsyncChatGPTProvider, AsyncGrokProvider):
            with self.subTest(provider=provider_type.__name__):
                name = "chatgpt" if provider_type is AsyncChatGPTProvider else "grok"
                provider = provider_type(Account.from_cookies(name, {}))
                provider._prepare_session = AsyncMock()
                provider._close_session = AsyncMock()
                provider._authenticate = AsyncMock(
                    return_value=SessionStatus(SessionState.AUTHENTICATED, True)
                )
                before, after = (ModelRef(name, item) for item in ("before", "after"))
                load = AsyncMock(side_effect=[[before], [after]])
                provider.models._loader = load

                async with provider:
                    self.assertEqual(await provider.models.list(), [before])
                    await provider.authenticate()
                    self.assertEqual(await provider.models.list(), [after])
                    self.assertEqual(await provider.models.list(), [after])
                self.assertEqual(load.await_count, 2)

    async def test_project_deletion_uses_the_collection_project_lock(self) -> None:
        provider = AsyncGrokProvider(Account.from_cookies("grok", {}))
        provider._prepare_session = AsyncMock()
        provider._close_session = AsyncMock()
        provider._delete_project_data = AsyncMock()
        project = provider.projects._new(remote_id="project")
        entered = asyncio.Event()
        provider.projects.hooks.before("project.delete", lambda _: entered.set())

        async with provider:
            async with provider._project_lock("project"):
                operation = asyncio.create_task(provider.projects.delete(project))
                await asyncio.wait_for(entered.wait(), timeout=1)
                provider._delete_project_data.assert_not_awaited()
            await operation
        provider._delete_project_data.assert_awaited_once_with("project")

    async def test_chatgpt_move_serializes_with_sends_and_applies_the_hook_target(self) -> None:
        account = Account.from_cookies("chatgpt", {})
        provider = AsyncChatGPTProvider(account)
        provider._prepare_session = AsyncMock()
        provider._close_session = AsyncMock()
        provider._get_chat_metadata = AsyncMock(
            return_value=Conversation("chat", "Chat", project=ProjectData("old", "Old"))
        )
        provider._move_chat_data = AsyncMock()
        chat = provider.chats._new(remote_id="chat")
        chat.project = provider.projects._new(remote_id="stale")
        selected = provider.projects._new(remote_id="original")
        hook_ran = asyncio.Event()

        @provider.chats.hooks.before("chat.move")
        def retarget(context):
            context.parameters["project_id"] = "updated"
            hook_ran.set()

        async with provider:
            with patch.object(provider, "_emit", new_callable=AsyncMock) as emit:
                for lock in (
                    provider._send_lock("chat"),
                    provider._project_lock("old"),
                    provider._project_lock("updated"),
                ):
                    chat.project = provider.projects._new(remote_id="stale")
                    hook_ran.clear()
                    provider._move_chat_data.reset_mock()
                    async with lock:
                        operation = asyncio.create_task(chat.move_to(selected))
                        await asyncio.wait_for(hook_ran.wait(), timeout=1)
                        provider._move_chat_data.assert_not_awaited()
                    self.assertIs(await operation, chat)

        provider._move_chat_data.assert_awaited_once_with(
            "chat", "updated", source_project_id="old"
        )
        assert chat.project is not None
        self.assertEqual(chat.project.remote_id, "updated")
        moved = next(call for call in emit.await_args_list if call.args == ("chat.moved",))
        self.assertEqual(moved.kwargs["payload"], {"project_id": "updated"})

    async def test_project_writes_confirm_incomplete_editor_results_without_replay(self) -> None:
        from dataclasses import replace
        from types import SimpleNamespace

        from webaiprovider.resources.message.attachment import Attachment
        from webaiprovider.resources.project.mutations import ProjectMutationsMixin
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        class ConfirmingProvider(ProjectMutationsMixin, AsyncProvider):
            def __init__(self, account: Account) -> None:
                super().__init__(account)
                self._gate = asyncio.Lock()
                self.project = NativeProject(
                    "project-1",
                    "Research",
                    "Old instructions",
                    (Attachment("resource-1", "Source", "text/plain"),),
                    metadata={"confirmed": "initial"},
                )
                self.get_calls = 0
                self.writes: list[str] = []
                self.editor = SimpleNamespace(
                    update=partial(self.write, "update"),
                    add_source=partial(self.write, "add_source"),
                    remove_source=partial(self.write, "remove_source"),
                )

            async def _get_project_data(self, _project_id: str) -> NativeProject:
                self.get_calls += 1
                return self.project

            async def write(self, action: str, _project_id: str, *, current=None, **changes):
                self.writes.append(action)
                if action == "update":
                    self.project = replace(
                        current,
                        **changes,
                        metadata={"confirmed": "update"},
                    )
                else:
                    self.project = replace(
                        self.project,
                        resources=tuple(
                            resource
                            for resource in self.project.resources
                            if resource.id != changes["resource_id"]
                        ),
                        metadata={"confirmed": "remove"},
                    )
                return None

        account = Account.from_cookies("claude", {"sessionKey": "fixture"})
        async with ConfirmingProvider(account) as provider:
            updated = await provider.update_project(
                replace(provider.project, name="Updated", instructions="New instructions")
            )
            self.assertEqual(updated.name, "Updated")
            self.assertEqual(updated.metadata["confirmed"], "update")

            removed = await provider._remove_project_resource_data("project-1", "resource-1")
            self.assertEqual(removed.resources, ())
            self.assertEqual(removed.metadata["confirmed"], "remove")

        self.assertEqual(provider.writes, ["update", "remove_source"])
        self.assertEqual(provider.get_calls, 4)

    async def test_direct_project_update_serializes_with_collection_update(self) -> None:
        from dataclasses import replace
        from types import SimpleNamespace

        from webaiprovider.resources.project.mutations import ProjectMutationsMixin
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        class ConcurrentProvider(ProjectMutationsMixin, AsyncProvider):
            def __init__(self, account: Account) -> None:
                super().__init__(account)
                self._gate = asyncio.Lock()
                self.remote = NativeProject("project-1", "Research", "Instructions")
                self.active_writes = 0
                self.max_active_writes = 0
                self.first_write_started = asyncio.Event()
                self.release_first_write = asyncio.Event()
                self.overlap_detected = asyncio.Event()
                self.editor = SimpleNamespace(update=self.update)

            async def _get_project_data(self, _project_id: str) -> NativeProject:
                return self.remote

            async def update(self, _project_id: str, *, current, **changes):
                self.active_writes += 1
                self.max_active_writes = max(self.max_active_writes, self.active_writes)
                if self.active_writes == 1:
                    self.first_write_started.set()
                    await self.release_first_write.wait()
                else:
                    self.overlap_detected.set()
                result = replace(current, **changes)
                self.active_writes -= 1
                return result

        account = Account.from_cookies("claude", {"sessionKey": "fixture"})
        async with ConcurrentProvider(account) as provider:
            project = provider.projects._new(
                title=provider.remote.name, remote_id=provider.remote.id
            )
            project._apply_provider_project(provider.remote)

            direct = asyncio.create_task(
                provider.update_project(replace(provider.remote, name="Direct update"))
            )
            await provider.first_write_started.wait()
            collection_started = asyncio.Event()

            async def update_collection() -> None:
                collection_started.set()
                await provider.projects.update(project, title="Collection update")

            collection = asyncio.create_task(update_collection())
            await collection_started.wait()
            self.assertFalse(provider.overlap_detected.is_set())
            provider.release_first_write.set()
            await asyncio.gather(direct, collection)

            self.assertEqual(provider.max_active_writes, 1)
            self.assertFalse(provider.overlap_detected.is_set())

    def test_sync_stream_batches_buffered_events_and_propagates_errors(self) -> None:
        from webaiprovider.resources.message.operation import MessageOperation
        from webaiprovider.resources.message.protocol import ConversationBound, ResponseComplete

        deltas = [TextDelta(f"t{i}") for i in range(12)]

        class BufferedStreamProvider(AsyncChatGPTProvider):
            async def _prepare_session(self) -> None:
                return None

            async def _close_session(self) -> None:
                return None

            async def _stream_provider_events(self, _request):
                yield ConversationBound("chat-1")
                for d in deltas:
                    yield d
                yield ResponseComplete()

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        with (
            patch.object(
                ChatGPTProvider, "_make_async_provider", side_effect=BufferedStreamProvider
            ),
            ChatGPTProvider(account) as provider,
        ):
            chat = provider.chats.create()
            with patch.object(provider, "_call", wraps=provider._call) as loop_calls:
                operation = chat.responses.create("hello")
                events = list(operation.stream())
            call_count = loop_calls.call_count

        received_deltas = [event.delta for event in events if isinstance(event, OutputDelta)]
        self.assertEqual(received_deltas, [f"t{i}" for i in range(12)])
        self.assertIsInstance(events[-1], OutputEnd)
        self.assertLessEqual(call_count, 5)

        class FailingBufferedProvider(BufferedStreamProvider):
            async def _stream_provider_events(self, _request):
                yield TextDelta("d0")
                yield TextDelta("d1")
                yield TextDelta("d2")
                raise ProviderError("stream aborted")

        with (
            patch.object(
                ChatGPTProvider, "_make_async_provider", side_effect=FailingBufferedProvider
            ),
            ChatGPTProvider(account) as provider,
        ):
            chat = provider.chats.create()
            collected = []
            with self.assertRaisesRegex(ProviderError, "stream aborted"):
                for event in chat.responses.create("hello").stream():
                    collected.append(event)
            self.assertEqual(
                [event.delta for event in collected if isinstance(event, OutputDelta)],
                ["d0", "d1", "d2"],
            )

            class CustomAsyncOp(AsyncMessageOperation):
                async def stream(self):
                    yield OutputBegin()
                    yield OutputDelta("custom")
                    yield OutputEnd()

            sync_base = chat.responses.create("hello")

            async def make_custom():
                return CustomAsyncOp(
                    sync_base._operation.chat,
                    sync_base._operation.message,
                    executor=sync_base._operation._executor,
                )

            custom_async_op = provider._call(make_custom())
            custom_sync_op = MessageOperation(provider, custom_async_op)
            self.assertEqual(
                [type(e) for e in custom_sync_op.stream()],
                [OutputBegin, OutputDelta, OutputEnd],
            )


if __name__ == "__main__":
    unittest.main()
