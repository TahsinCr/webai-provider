from __future__ import annotations

import asyncio
import threading
import unittest
from unittest.mock import AsyncMock, patch

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
from webaiprovider.errors import ProviderError, RemoteNotFound, UnsupportedOperationError
from webaiprovider.resources.message.protocol import TextDelta
from webaiprovider.resources.model.model import (
    EffortSet,
    ModelRef,
)


class TestApi(unittest.IsolatedAsyncioTestCase):
    async def open_provider(
        self,
        account: Account,
        provider: AsyncProvider | None = None,
    ) -> AsyncProvider:
        provider = provider or AsyncProvider(account)
        for name in ("prepare", "close", "authenticate", "session_status", "forget_session"):
            setattr(provider.adapter.session, name, AsyncMock())
        provider.adapter.models.load_catalog = AsyncMock()
        for name in (
            "list_chats",
            "get_chat",
            "branch_chat",
            "conversation_attachment_count",
            "rename_chat",
            "delete_chat",
            "move_chat",
        ):
            setattr(provider.adapter.chats, name, AsyncMock())
        for name in (
            "list_projects",
            "get_project",
            "list_project_chats",
            "create_project",
            "patch_project",
            "project_options",
            "add_project_resource",
            "remove_project_resource",
            "delete_project",
        ):
            setattr(provider.adapter.projects, name, AsyncMock())
        provider.adapter.attachments.upload_file = AsyncMock()
        provider.adapter.attachments.resolve_download = AsyncMock()
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

        op = chat.messages.create(
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

        mock_provider.execute_stream = fake_stream
        events = []
        async for ev in op.stream():
            events.append(ev)

        self.assertEqual(len(events), 7)
        self.assertEqual(thoughts, ["Reasoning step 1"])
        self.assertEqual(deltas, ["Hello ", "World!"])

        res = await op
        self.assertIsInstance(res, Response)
        self.assertEqual(res.text, "Hello World!")
        self.assertEqual(res.message.content, "Hello World!")
        self.assertEqual(res.message.role, "assistant")

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

        provider.execute_stream = fake_stream
        operation = chat.messages.create("prompt")
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
        from webaiprovider.resources.message.operation import OperationHooks

        hooks = OperationHooks()
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

        operation = chat.messages.create("send once")
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
            await chat.messages.create("must not be echoed as a response").wait()

    async def test_response_with_sources_and_artifacts(self) -> None:
        acc = Account.from_cookies("gemini", {"cookie": "gemini-val"})
        provider = await self.open_provider(acc)
        mock_provider = provider
        chat = await provider.chats.create()
        op = chat.messages.create("Explain relativity")

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

        mock_provider.execute_stream = fake_stream
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
        chats_adapter = session.adapter.chats
        state = {"title": "My Claude Chat", "messages": ()}

        def current_conv() -> Conversation:
            return Conversation(
                "chat-123",
                state["title"],
                state["messages"],
            )

        async def rename_chat(_cid: str, title: str) -> None:
            state["title"] = title

        chats_adapter.rename_chat = AsyncMock(side_effect=rename_chat)
        chats_adapter.list_chats = AsyncMock(side_effect=lambda **_kw: Page((current_conv(),)))
        chats_adapter.get_chat = AsyncMock(side_effect=lambda _cid: current_conv())

        chat = await session.chats.create(title="My Claude Chat")
        chat.remote_id = "chat-123"

        await chat.rename("Renamed Chat")
        chats_adapter.rename_chat.assert_awaited_once_with("chat-123", "Renamed Chat")
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
        session.adapter.attachments.resolve_download.assert_awaited_once_with(
            "https://example.com/test.png", conversation_id="chat-123", message_id=None
        )
        self.assertEqual(dl, b"file-bytes")

        op = chat.messages.create("Hello test")
        state["messages"] = (
            NativeMessage(op.message.id, "user", (Content("text", "Hello test"),)),
        )
        self.assertEqual((await chat.get(op.message.id)).content, "Hello test")
        await chat.delete()
        chats_adapter.delete_chat.assert_awaited_once_with("chat-123")

    async def test_project_lifecycle_methods(self) -> None:
        from dataclasses import replace

        from webaiprovider.resources.message.attachment import Attachment
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        acc = Account.from_cookies("claude", {"sessionKey": "claude-key"})
        session = await self.open_provider(acc)
        projects_adapter = session.adapter.projects
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

        async def patch_project(project_id, *, name=None, instructions=None):
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

        async def remove_resource(_project_id, resource_id):
            remote[0] = replace(
                remote[0],
                resources=tuple(r for r in remote[0].resources if r.id != resource_id),
            )
            return remote[0]

        projects_adapter.create_project.side_effect = create_project
        projects_adapter.get_project.side_effect = get_project
        projects_adapter.patch_project.side_effect = patch_project
        projects_adapter.add_project_resource.side_effect = add_resource
        projects_adapter.remove_project_resource.side_effect = remove_resource

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

        added = await project.resources.add("Doc", resource_type="text", name="res-1")
        self.assertEqual(added["id"], "remote-res-1")
        projects_adapter.add_project_resource.assert_awaited_once_with(
            "proj-abc", "text", name="res-1", text="Doc"
        )
        self.assertEqual(await project.resources.download(added["id"]), "Doc")

        created_res = await project.resources.create("Doc 2", resource_type="text", name="res-2")
        self.assertEqual(created_res["data"], "Doc 2")
        await session.projects.update(
            project, title="Updated Project", instruction="Updated Instruction"
        )
        projects_adapter.patch_project.assert_awaited_with(
            "proj-abc", name="Updated Project", instructions="Updated Instruction"
        )
        self.assertEqual(project.title, "Updated Project")
        self.assertEqual(await project.instruction.get(), "Updated Instruction")

        with self.assertRaises(UnsupportedOperationError):
            await project.resources.update(added["id"], data="Updated Doc")
        self.assertEqual((await project.resources.get(added["id"]))["data"], "Doc")

        await project.resources.delete(created_res["id"])
        await project.resources.remove(added["id"])
        self.assertEqual(projects_adapter.remove_project_resource.await_count, 2)
        await project.delete()
        projects_adapter.delete_project.assert_awaited_once_with("proj-abc")

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

        retry_op = msg.retry()
        self.assertIsInstance(retry_op, AsyncMessageOperation)
        self.assertEqual(retry_op.message.metadata.get("action"), "retry")
        edit_op = msg.edit("Edited prompt")
        self.assertEqual(edit_op.message.content, "Edited prompt")
        self.assertEqual(edit_op.message.metadata.get("action"), "edit")
        update_op = chat.messages.update(msg, content="Updated via messages.update")
        self.assertEqual(update_op.message.content, "Updated via messages.update")
        self.assertEqual(update_op.message.metadata.get("action"), "edit")

        branch_meta = {
            "pending_branch": {"conversation_id": "remote-chat-id", "message_id": "msg-orig-1"}
        }
        session.adapter.chats.branch_chat.return_value = Conversation(
            "branch-chat-id",
            "Branch chat",
            metadata=branch_meta,
        )
        branched_chat = await chat.branch(msg.id, title="Branch chat")
        session.adapter.chats.branch_chat.assert_awaited_once_with("remote-chat-id", "msg-orig-1")
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
                    for chat_type in (asynchronous._chat_type, synchronous.chat_wrapper_type):
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
        provider.adapter.session.prepare = AsyncMock()
        provider.adapter.session.close = AsyncMock()
        self.assertIs(await provider.open(), provider)
        self.assertIs(await provider.open(), provider)
        provider.adapter.session.prepare.assert_awaited_once_with()
        await provider.close()
        provider.adapter.session.close.assert_awaited_once_with()

    async def test_chatgpt_project_creation_accepts_memory_share_flag(self) -> None:
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        provider = AsyncChatGPTProvider(account)
        provider.adapter.session.prepare = AsyncMock()
        provider.adapter.session.close = AsyncMock()
        provider.adapter.projects.create_project = AsyncMock(
            return_value=NativeProject("project-1", "Private notes")
        )
        await provider.open()
        project = await provider.projects.create("Private notes", memory_share=False)
        self.assertEqual(project.title, "Private notes")
        self.assertEqual(
            provider.adapter.projects.create_project.await_args.kwargs["memory_share"], False
        )
        await provider.close()

    async def test_sync_provider_rejects_blocking_inside_running_loop(self) -> None:
        provider = Provider(Account.from_cookies("chatgpt", {"cookie": "test"}))
        with self.assertRaisesRegex(RuntimeError, "cannot block an event loop"):
            provider.open()
        provider.close()

    def test_catalogs_and_selection_resolver(self) -> None:
        self.assertEqual(ChatGPTModels.GPT_6_INSTANT.id, "gpt-6-instant")
        self.assertEqual(ClaudeModels.SONNET_5_5.id, "claude-sonnet-5-5")
        self.assertEqual(GeminiModels.FLASH.id, "gemini-flash")
        self.assertEqual(GeminiModels.HYDROGEN.id, "gemini-hydrogen")
        self.assertEqual(
            [model.id for model in GeminiModels.list()],
            [
                "gemini-flash-lite",
                "gemini-flash",
                "gemini-hydrogen",
                "gemini-pro",
                "gemini-argon",
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
        provider.adapter.session.prepare = AsyncMock()
        provider.adapter.session.close = AsyncMock()

        async def fail_stream(_request):
            raise RateLimited(retry_after=42.0)
            yield TextDelta("unreachable")

        provider.adapter.messages.stream = fail_stream
        await provider.open()
        chat = await provider.chats.create()
        operation = chat.messages.create("Hello")
        with self.assertRaises(RateLimited) as ctx:
            await operation.start()
        self.assertEqual(ctx.exception.retry_after, 42.0)

        async def fail_validation(_request):
            raise ValueError("invalid effort level")
            yield TextDelta("unreachable")

        provider.adapter.messages.stream = fail_validation
        with self.assertRaisesRegex(ValueError, "invalid effort level"):
            await chat.messages.create("Hello").start()
        self.assertTrue(repr(provider).startswith("<AsyncGrokProvider "))
        await provider.close()

    async def test_prepare_operation_preserves_attachment_mime_type(self) -> None:
        from webaiprovider.core.operations import run_pipeline_unwrapped

        provider = AsyncGrokProvider(Account.from_cookies("grok", {"sso": "grok-tok"}))
        provider.adapter.session.prepare = AsyncMock()
        provider.adapter.session.close = AsyncMock()
        await provider.open()
        try:
            chat = await provider.chats.create()
            att = Attachment.from_bytes(b"hello", name="notes.txt", mime_type="text/plain")
            msg = Message("Read", attachments=[att])
            op = chat.messages.create(msg)
            ctx = provider.runtime.new_context(
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
        p.adapter.session.prepare = AsyncMock()
        p.adapter.session.close = AsyncMock()

        async def fake_stream(_op):
            yield TextDelta("Direct answer")

        p.adapter.messages.stream = fake_stream
        await p.open()
        res = await p.ask("What is 2+2?")
        self.assertEqual(res.text, "Direct answer")
        self.assertEqual(res.message.content, "Direct answer")
        await p.close()

    async def test_model_resolution_from_string_and_policy(self) -> None:
        from webaiprovider.resources.model import ModelCatalogState
        from webaiprovider.resources.model.model import EffortOption, ModelCatalog
        from webaiprovider.resources.model.model import Model as CatalogModel

        account = Account.from_cookies("chatgpt", {"session_token": "tok"})
        provider = StreamTestProvider(account.provider, account.id, (TextDelta("hi"),))
        provider = await self.open_provider(account, provider)
        provider.adapter.models.load_catalog = AsyncMock(
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
            chat.session.models, "resolve", wraps=chat.session.models.resolve
        ) as resolve_spy:
            operation = chat.messages.create("test", model=ChatGPTModels.GPT_6_THINKING)
            await operation.start()
            self.assertEqual(provider.requests[-1].model, "gpt-6-thinking")
            self.assertIsNone(provider.requests[-1].effort)

            operation = chat.messages.create(
                "test", model=ChatGPTModels.GPT_6_THINKING.efforts.HIGH
            )
            await operation.start()
            self.assertEqual(provider.requests[-1].model, "gpt-6-thinking")
            self.assertEqual(provider.requests[-1].effort, "extended")

            operation = chat.messages.create(
                "test", model=ChatGPTModels.GPT_6_THINKING.select("standard")
            )
            await operation.start()
            self.assertEqual(provider.requests[-1].model, "gpt-6-thinking")
            self.assertEqual(provider.requests[-1].effort, "standard")

            with self.assertRaises(RemoteNotFound):
                await chat.messages.create("test", model=GeminiModels.HYDROGEN).start()

            resolve_spy.assert_not_awaited()
            provider.adapter.models.load_catalog.assert_not_awaited()

        operation = chat.messages.create("test", model="gpt-6-instant")
        await operation.start()
        self.assertEqual(provider.requests[-1].model, "gpt-6-instant")
        operation = chat.messages.create("test", model=Models.AUTO)
        await operation.start()
        self.assertEqual(provider.requests[-1].model, "gpt-6-instant")
        operation = chat.messages.create("test", model=Models.HIGH)
        await operation.start()
        self.assertEqual(provider.requests[-1].model, "gpt-6-thinking")

    def test_incomplete_collection_cannot_be_instantiated(self) -> None:
        from webaiprovider.collections import ManageableCollection

        with self.assertRaises(TypeError):
            type("IncompleteManageable", (ManageableCollection,), {})()

    async def test_include_projects_paginates_all_pages_and_bounds_concurrency(self) -> None:
        from webaiprovider.base import (
            BaseChatAdapter,
            BaseProjectAdapter,
            BaseProviderAdapter,
            BaseSessionAdapter,
        )
        from webaiprovider.collections import Page
        from webaiprovider.resources.chat.types import Conversation
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        class PaginatedChats(BaseChatAdapter):
            def __init__(self, account_id: str) -> None:
                self.account_id = account_id

            async def list_chats(self, *, cursor: str | None = None, limit: int = 50):
                del cursor, limit
                return Page(
                    (
                        Conversation("c-loose", "Loose"),
                        Conversation("c-shared", "Shared"),
                    ),
                    None,
                )

        class PaginatedProjects(BaseProjectAdapter):
            def __init__(self, account_id: str) -> None:
                self.account_id = account_id
                self.active_fetches = 0
                self.max_active_fetches = 0
                self.page_calls: list[tuple[str, str | None]] = []
                self.project_page_calls: list[str | None] = []

            async def list_projects(self, *, cursor: str | None = None):
                self.project_page_calls.append(cursor)
                projects = tuple(NativeProject(f"p-{idx}", f"Project {idx}") for idx in range(1, 7))
                return (
                    Page(projects[:3], "projects-page-2") if cursor is None else Page(projects[3:])
                )

            async def list_project_chats(self, project_id: str, *, cursor: str | None = None):
                self.page_calls.append((project_id, cursor))
                self.active_fetches += 1
                self.max_active_fetches = max(self.max_active_fetches, self.active_fetches)
                try:
                    await asyncio.sleep(0.01)
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
        projects_adapter = PaginatedProjects(account.id)
        adapter = BaseProviderAdapter(
            provider_id="chatgpt",
            account_id=account.id,
            capabilities=AsyncChatGPTProvider.capabilities,
            session=BaseSessionAdapter(),
            chats=PaginatedChats(account.id),
            projects=projects_adapter,
        )
        async with AsyncChatGPTProvider(account, adapter=adapter) as provider:
            chats = await provider.chats.refresh(include_projects=True)

        self.assertLessEqual(projects_adapter.max_active_fetches, 4)
        self.assertEqual(projects_adapter.project_page_calls, [None, "projects-page-2"])
        self.assertIn(("p-1", None), projects_adapter.page_calls)
        self.assertIn(("p-1", "p1-page-2"), projects_adapter.page_calls)
        self.assertIn(("p-2", None), projects_adapter.page_calls)
        self.assertIn(("p-2", "p2-page-2"), projects_adapter.page_calls)
        self.assertIn(("p-2", "p2-page-3"), projects_adapter.page_calls)
        ids = [chat.remote_id for chat in chats]
        self.assertEqual(ids, ["c-loose", "c-shared", "c-p1-2", "c-p2-1", "c-p2-2", "c-p2-3"])
        shared = next(chat for chat in chats if chat.remote_id == "c-shared")
        self.assertIsNotNone(shared.project)
        assert shared.project is not None
        self.assertEqual(shared.project.remote_id, "p-1")

    async def test_include_projects_rejects_repeated_cursor_and_cancels_siblings(self) -> None:
        from webaiprovider.base import (
            BaseChatAdapter,
            BaseProjectAdapter,
            BaseProviderAdapter,
            BaseSessionAdapter,
        )
        from webaiprovider.collections import Page
        from webaiprovider.resources.chat.types import Conversation
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        cancelled = asyncio.Event()

        class RepeatingChats(BaseChatAdapter):
            def __init__(self, account_id: str) -> None:
                self.account_id = account_id

            async def list_chats(self, *, cursor: str | None = None, limit: int = 50):
                del cursor, limit
                return Page(
                    (Conversation("c-loose", "Loose"),),
                    None,
                )

        class RepeatingProjects(BaseProjectAdapter):
            def __init__(self, account_id: str) -> None:
                self.account_id = account_id

            async def list_projects(self, *, cursor: str | None = None):
                del cursor
                return Page(
                    (
                        NativeProject("p-1", "Alpha"),
                        NativeProject("p-2", "Beta"),
                    ),
                    None,
                )

            async def list_project_chats(self, project_id: str, *, cursor: str | None = None):
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
        adapter = BaseProviderAdapter(
            provider_id="chatgpt",
            account_id=account.id,
            capabilities=AsyncChatGPTProvider.capabilities,
            session=BaseSessionAdapter(),
            chats=RepeatingChats(account.id),
            projects=RepeatingProjects(account.id),
        )
        async with AsyncChatGPTProvider(account, adapter=adapter) as provider:
            with self.assertRaisesRegex(ProviderError, "cursor"):
                await provider.chats.refresh(include_projects=True)
            self.assertTrue(cancelled.is_set())

    async def test_project_refresh_rejects_repeated_cursor_cleanly(self) -> None:
        from webaiprovider.base import (
            BaseProjectAdapter,
            BaseProviderAdapter,
            BaseSessionAdapter,
        )
        from webaiprovider.collections import Page

        class RepeatingProjectAdapter(BaseProjectAdapter):
            def __init__(self) -> None:
                self.project_cursors: list[str | None] = []

            async def list_projects(self, *, cursor: str | None = None):
                self.project_cursors.append(cursor)
                return Page((), "same-page")

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        projects_adapter = RepeatingProjectAdapter()
        adapter = BaseProviderAdapter(
            provider_id="chatgpt",
            account_id=account.id,
            capabilities=AsyncChatGPTProvider.capabilities,
            session=BaseSessionAdapter(),
            projects=projects_adapter,
        )
        async with AsyncChatGPTProvider(account, adapter=adapter) as provider:
            with self.assertRaisesRegex(ProviderError, "cursor") as ctx:
                await provider.projects.refresh()
            self.assertEqual(getattr(ctx.exception, "partial_count", None), 0)
            self.assertEqual(projects_adapter.project_cursors, [None, "same-page"])


class TestSyncApi(unittest.TestCase):
    def test_sync_chat_project_and_stream_share_one_portal(self) -> None:
        from webaiprovider.base import (
            BaseMessageAdapter,
            BaseProjectAdapter,
            BaseProviderAdapter,
            BaseSessionAdapter,
        )
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        class RecordingChatGPTProvider(AsyncChatGPTProvider):
            def __init__(self, account: Account, **options) -> None:
                self.thread_ids: list[int] = []
                self.memory_share = None
                super().__init__(account, **options)

            def _create_adapter(self) -> BaseProviderAdapter:
                owner = self

                class SessionAdp(BaseSessionAdapter):
                    async def prepare(self) -> None:
                        owner.thread_ids.append(threading.get_ident())

                    async def close(self) -> None:
                        owner.thread_ids.append(threading.get_ident())

                class ProjectAdp(BaseProjectAdapter):
                    async def create_project(self, name, instructions="", **opts):
                        owner.thread_ids.append(threading.get_ident())
                        owner.memory_share = opts.get("memory_share", True)
                        return NativeProject(
                            "project-1",
                            name,
                            instructions,
                        )

                class MsgAdp(BaseMessageAdapter):
                    async def stream(self, _request):
                        owner.thread_ids.append(threading.get_ident())
                        yield TextDelta("done")

                return BaseProviderAdapter(
                    provider_id="chatgpt",
                    account_id=self.account_id,
                    capabilities=self.capabilities,
                    session=SessionAdp(),
                    projects=ProjectAdp(),
                    messages=MsgAdp(),
                )

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        with (
            patch.object(ChatGPTProvider, "async_provider_type", RecordingChatGPTProvider),
            ChatGPTProvider(account) as provider,
        ):
            project = provider.projects.create("Notes", memory_share=False)
            chat = provider.chats.create("Draft", project=project)
            operation = chat.messages.create("hello")
            events = list(operation.stream())
            response = operation.wait()

        self.assertEqual(response.text, "done")
        self.assertEqual(
            [event.event for event in events],
            ["output.begin", "output.delta", "output.end"],
        )
        self.assertFalse(provider._provider.memory_share)
        self.assertTrue(provider._provider.thread_ids)
        self.assertEqual(len(set(provider._provider.thread_ids)), 1)

    def test_sync_include_projects_and_chatgpt_move_project_chat(self) -> None:
        from webaiprovider.base import (
            BaseChatAdapter,
            BaseProjectAdapter,
            BaseProviderAdapter,
            BaseSessionAdapter,
            Capability,
        )
        from webaiprovider.collections import Page
        from webaiprovider.errors import FeatureUnavailable, UnsupportedOperationError
        from webaiprovider.resources.chat.types import Conversation
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        class RecordingChatGPTProvider(AsyncChatGPTProvider):
            def __init__(self, account: Account, **options) -> None:
                self.moved: list[tuple[str, str | None]] = []
                super().__init__(account, **options)

            def _create_adapter(self) -> BaseProviderAdapter:
                owner = self

                class ProjectAdp(BaseProjectAdapter):
                    async def list_projects(self, *, cursor: str | None = None):
                        del cursor
                        return Page(
                            (
                                NativeProject("p-1", "Alpha"),
                                NativeProject("p-2", "Beta"),
                            ),
                            None,
                        )

                    async def list_project_chats(
                        self, project_id: str, *, cursor: str | None = None
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

                class ChatAdp(BaseChatAdapter):
                    async def list_chats(self, *, cursor: str | None = None, limit: int = 50):
                        del cursor, limit
                        return Page(
                            (Conversation("c-loose", "Loose"),),
                            None,
                        )

                    async def get_chat(self, conversation_id: str):
                        del conversation_id
                        raise FeatureUnavailable(Capability.CHAT)

                    async def move_chat(self, conversation_id: str, project_id: str | None) -> None:
                        owner.moved.append((conversation_id, project_id))

                return BaseProviderAdapter(
                    provider_id="chatgpt",
                    account_id=self.account_id,
                    capabilities=self.capabilities,
                    session=BaseSessionAdapter(),
                    chats=ChatAdp(),
                    projects=ProjectAdp(),
                )

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        with (
            patch.object(ChatGPTProvider, "async_provider_type", RecordingChatGPTProvider),
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

            with self.assertRaises(UnsupportedOperationError):
                provider.move_project_chat(loose, p1)

            loose.move_to(p1)
            self.assertIsNotNone(loose.project)
            assert loose.project is not None
            self.assertEqual(loose.project.remote_id, "p-1")

            with self.assertRaises(UnsupportedOperationError):
                loose.move_to(p2)

            moved = provider.move_project_chat(loose, p2)
            self.assertIsNotNone(moved.project)
            assert moved.project is not None
            self.assertEqual(moved.project.remote_id, "p-2")

            detached = provider.move_project_chat(moved, None)
            self.assertIsNone(detached.project)
            self.assertEqual(
                provider._provider.moved,
                [("c-loose", "p-1"), ("c-loose", "p-2"), ("c-loose", None)],
            )


class TestApiOptimizationContracts(unittest.IsolatedAsyncioTestCase):
    async def test_project_writes_confirm_incomplete_editor_results_without_replay(self) -> None:
        from dataclasses import replace
        from types import SimpleNamespace

        from webaiprovider.base import (
            BaseProviderAdapter,
            BaseSessionAdapter,
            EditorProjectAdapter,
        )
        from webaiprovider.resources.message.attachment import Attachment
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        class ConfirmingProjects(EditorProjectAdapter):
            def __init__(self, account_id: str) -> None:
                del account_id
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
                self.editor = SimpleNamespace(write=self.write)

            def _get_editor(self):
                return self.editor

            def _get_gate(self):
                return self._gate

            async def get_project(self, _project_id: str) -> NativeProject:
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
        proj_adapter = ConfirmingProjects(account.id)
        adapter = BaseProviderAdapter(
            provider_id="claude",
            account_id=account.id,
            session=BaseSessionAdapter(),
            projects=proj_adapter,
        )
        async with AsyncProvider(account, adapter=adapter) as provider:
            updated = await provider.update_project(
                replace(proj_adapter.project, name="Updated", instructions="New instructions")
            )
            self.assertEqual(updated.name, "Updated")
            self.assertEqual(updated.metadata["confirmed"], "update")

            removed = await provider._remove_project_resource_data("project-1", "resource-1")
            self.assertEqual(removed.resources, ())
            self.assertEqual(removed.metadata["confirmed"], "remove")

        self.assertEqual(proj_adapter.writes, ["update", "remove_source"])
        self.assertEqual(proj_adapter.get_calls, 4)

    async def test_direct_project_update_serializes_with_collection_update(self) -> None:
        from dataclasses import replace
        from types import SimpleNamespace

        from webaiprovider.base import (
            BaseProviderAdapter,
            BaseSessionAdapter,
            EditorProjectAdapter,
        )
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        class ConcurrentProjects(EditorProjectAdapter):
            def __init__(self, account_id: str) -> None:
                del account_id
                self._gate = asyncio.Lock()
                self.remote = NativeProject("project-1", "Research", "Instructions")
                self.active_writes = 0
                self.max_active_writes = 0
                self.first_write_started = asyncio.Event()
                self.release_first_write = asyncio.Event()
                self.overlap_detected = asyncio.Event()
                self.editor = SimpleNamespace(write=self.write)

            def _get_editor(self):
                return self.editor

            def _get_gate(self):
                return self._gate

            async def get_project(self, _project_id: str) -> NativeProject:
                return self.remote

            async def write(self, _action: str, _project_id: str, *, current, **changes):
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
        proj_adapter = ConcurrentProjects(account.id)
        adapter = BaseProviderAdapter(
            provider_id="claude",
            account_id=account.id,
            session=BaseSessionAdapter(),
            projects=proj_adapter,
        )
        async with AsyncProvider(account, adapter=adapter) as provider:
            project = provider._project_type(
                title=proj_adapter.remote.name,
                remote_id=proj_adapter.remote.id,
                session=provider,
            )
            project._apply_provider_project(proj_adapter.remote)

            direct = asyncio.create_task(
                provider.update_project(replace(proj_adapter.remote, name="Direct update"))
            )
            await proj_adapter.first_write_started.wait()
            collection_started = asyncio.Event()

            async def update_collection() -> None:
                collection_started.set()
                await provider.projects.update(project, title="Collection update")

            collection = asyncio.create_task(update_collection())
            await collection_started.wait()
            self.assertFalse(proj_adapter.overlap_detected.is_set())
            proj_adapter.release_first_write.set()
            await asyncio.gather(direct, collection)

            self.assertEqual(proj_adapter.max_active_writes, 1)
            self.assertFalse(proj_adapter.overlap_detected.is_set())

    def test_sync_stream_batches_buffered_events_and_propagates_errors(self) -> None:
        from webaiprovider.base import (
            BaseMessageAdapter,
            BaseProviderAdapter,
            BaseSessionAdapter,
        )
        from webaiprovider.resources.message.operation import MessageOperation
        from webaiprovider.resources.message.protocol import ConversationBound, ResponseComplete

        deltas = [TextDelta(f"t{i}") for i in range(12)]

        class BufferedMessages(BaseMessageAdapter):
            async def stream(self, _request):
                yield ConversationBound("chat-1")
                for d in deltas:
                    yield d
                yield ResponseComplete()

        class BufferedStreamProvider(AsyncChatGPTProvider):
            def _create_adapter(self) -> BaseProviderAdapter:
                return BaseProviderAdapter(
                    provider_id="chatgpt",
                    account_id=self.account_id,
                    capabilities=self.capabilities,
                    session=BaseSessionAdapter(),
                    messages=BufferedMessages(),
                )

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        with (
            patch.object(ChatGPTProvider, "async_provider_type", BufferedStreamProvider),
            ChatGPTProvider(account) as provider,
        ):
            chat = provider.chats.create()
            with patch.object(provider, "_call", wraps=provider._call) as portal_calls:
                operation = chat.messages.create("hello")
                events = list(operation.stream())
            call_count = portal_calls.call_count

        received_deltas = [event.delta for event in events if isinstance(event, OutputDelta)]
        self.assertEqual(received_deltas, [f"t{i}" for i in range(12)])
        self.assertIsInstance(events[-1], OutputEnd)
        self.assertLessEqual(call_count, 5)

        class FailingMessages(BaseMessageAdapter):
            async def stream(self, _request):
                yield TextDelta("d0")
                yield TextDelta("d1")
                yield TextDelta("d2")
                raise ProviderError("stream aborted")

        class FailingBufferedProvider(BufferedStreamProvider):
            def _create_adapter(self) -> BaseProviderAdapter:
                return BaseProviderAdapter(
                    provider_id="chatgpt",
                    account_id=self.account_id,
                    capabilities=self.capabilities,
                    session=BaseSessionAdapter(),
                    messages=FailingMessages(),
                )

        with (
            patch.object(ChatGPTProvider, "async_provider_type", FailingBufferedProvider),
            ChatGPTProvider(account) as provider,
        ):
            chat = provider.chats.create()
            collected = []
            with self.assertRaisesRegex(ProviderError, "stream aborted"):
                for event in chat.messages.create("hello").stream():
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

            sync_base = chat.messages.create("hello")

            async def make_custom():
                return CustomAsyncOp(
                    sync_base._operation.chat,
                    sync_base._operation.message,
                    executor=sync_base._operation._executor,
                )

            custom_async_op = provider._portal.call(make_custom())
            custom_sync_op = MessageOperation(provider, custom_async_op)
            self.assertEqual(
                [type(e) for e in custom_sync_op.stream()],
                [OutputBegin, OutputDelta, OutputEnd],
            )


if __name__ == "__main__":
    unittest.main()
