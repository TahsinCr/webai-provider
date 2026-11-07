from __future__ import annotations

import asyncio
import importlib.util
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import webaiprovider
from webaiprovider import (
    Account,
    AmbiguousMatchError,
    Artifact,
    Attachment,
    Attachments,
    ChatGPTModels,
    ChatGPTProvider,
    ClaudeModels,
    ClaudeProvider,
    GeminiModels,
    GeminiProvider,
    GrokModels,
    GrokProvider,
    Message,
    MessageOperation,
    Models,
    OutputBegin,
    OutputDelta,
    OutputEnd,
    ProviderSession,
    Response,
    SelectionResolver,
    Source,
    ThinkingBegin,
    ThinkingDelta,
    ThinkingEnd,
)
from webaiprovider.bridge import ProviderBridge
from webaiprovider.contracts import EffortSet, ModelRef
from webaiprovider.errors import ProviderError, UnsupportedOperationError


class TestApi(unittest.IsolatedAsyncioTestCase):
    def test_canonical_package_namespace(self) -> None:
        self.assertIsNotNone(importlib.util.find_spec("webaiprovider"))
        self.assertIsNone(importlib.util.find_spec("webai_provider"))
        self.assertIs(webaiprovider.Account, Account)
        self.assertIs(webaiprovider.Message, Message)
        self.assertIs(webaiprovider.ChatGPTProvider, ChatGPTProvider)
        self.assertIs(webaiprovider.SelectionResolver, SelectionResolver)

    def test_account_creation_and_session(self) -> None:
        acc = Account.from_cookies("chatgpt", {"session_token": "mock-token-xyz"})
        self.assertEqual(acc.provider, "chatgpt")
        self.assertTrue(acc.is_active)
        self.assertIn("session_token", acc.store().read())

        session = acc.connect()
        self.assertIsInstance(session, ProviderSession)
        self.assertEqual(session.provider, "chatgpt")
        self.assertIsNotNone(session.chats)
        self.assertIsNotNone(session.projects)

    def test_account_browser_selector_is_forwarded(self) -> None:
        with patch(
            "webaiprovider.accounts.importer.import_cookies",
            return_value=("chromium", {"session_token": "mock-token-xyz"}),
        ) as read_cookies:
            Account.from_browser("ChatGPT", browser="brave")

        read_cookies.assert_called_once_with("chatgpt", browser="brave")

    async def test_lazy_message_creation_and_hooks(self) -> None:
        acc = Account.from_cookies("chatgpt", {"cookie": "val"})
        session = acc.connect()
        chat = await session.chats.create(title="Test Chat")

        op = chat.messages.create(
            "Hello AI",
            model=ChatGPTModels.GPT_6_THINKING.select(ChatGPTModels.GPT_6_THINKING.efforts.MEDIUM),
        )

        self.assertIsInstance(op, MessageOperation)
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

        mock_bridge = MagicMock(spec=ProviderBridge)

        async def fake_stream(_operation: MessageOperation):
            yield ThinkingBegin()
            yield ThinkingDelta(delta="Reasoning step 1")
            yield ThinkingEnd()
            yield OutputBegin()
            yield OutputDelta(delta="Hello ")
            yield OutputDelta(delta="World!")
            yield OutputEnd()

        mock_bridge.execute_stream = fake_stream
        session._bridge = mock_bridge

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

    async def test_operation_uses_one_send_across_entrypoints(self) -> None:
        session = Account.from_cookies("chatgpt", {"cookie": "val"}).connect()
        chat = await session.chats.create()
        entered = asyncio.Event()
        release = asyncio.Event()
        send_count = 0

        async def fake_stream(_operation: MessageOperation):
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
        self.assertEqual(response.text, "done")
        self.assertEqual(
            [item.event for item in events], ["output.begin", "output.delta", "output.end"]
        )
        self.assertEqual(
            [item.event for item in replayed], ["output.begin", "output.delta", "output.end"]
        )
        self.assertEqual(send_count, 1)

    async def test_message_send_requires_provider_session(self) -> None:
        session = Account.from_cookies("chatgpt", {"cookie": "val"}).connect()
        chat = await session.chats.create()
        session._bridge = None

        with self.assertRaisesRegex(ProviderError, "No provider session is attached"):
            await chat.messages.create("must not be echoed as a response").wait()

    async def test_response_with_sources_and_artifacts(self) -> None:
        acc = Account.from_cookies("gemini", {"cookie": "gemini-val"})
        session = acc.connect()
        chat = await session.chats.create()
        op = chat.messages.create("Explain relativity")

        mock_bridge = MagicMock(spec=ProviderBridge)

        async def fake_stream(_operation: MessageOperation):
            yield OutputBegin()
            yield OutputDelta(delta="Relativity explained.")
            from webaiprovider.contracts import ArtifactDiscovered, SourceDiscovered

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

        mock_bridge.execute_stream = fake_stream
        session._bridge = mock_bridge

        response = await op.start()
        self.assertEqual(response.text, "Relativity explained.")
        self.assertEqual(len(response.sources), 1)
        self.assertEqual(response.sources[0].title, "Einstein Paper")
        self.assertEqual(len(response.artifacts), 1)
        self.assertEqual(response.artifacts[0].name, "spacetime.png")

    async def test_chat_lifecycle_methods(self) -> None:
        acc = Account.from_cookies("claude", {"sessionKey": "claude-key"})
        session = acc.connect()

        mock_bridge = AsyncMock(spec=ProviderBridge)
        session._bridge = mock_bridge

        chat = await session.chats.create(title="My Claude Chat")
        chat.remote_id = "chat-123"

        await chat.rename("Renamed Chat")
        mock_bridge.rename_chat.assert_awaited_once_with(chat, "Renamed Chat")
        self.assertEqual(chat.title, "Renamed Chat")

        await session.chats.update(chat, title="Updated Via Collection")
        self.assertEqual(chat.title, "Updated Via Collection")

        await session.chats.update(chat.id, title="Updated Via ID")
        self.assertEqual(chat.title, "Updated Via ID")

        found_start = session.chats.find("Updated", startswith=True)
        self.assertEqual(found_start.id, chat.id)

        found_end = session.chats.find("ID", endswith=True)
        self.assertEqual(found_end.id, chat.id)

        mock_bridge.download.return_value = b"file-bytes"
        dl = await chat.download("https://example.com/test.png")
        mock_bridge.download.assert_awaited_once_with(
            "https://example.com/test.png", conversation_id=chat.remote_id
        )
        self.assertEqual(dl, b"file-bytes")

        op = chat.messages.create("Hello test")
        self.assertEqual(chat.get(op.message.id).content, "Hello test")

        mock_bridge.branch_chat.return_value = MagicMock(
            remote_id="branch-789", title="Branch Chat"
        )
        branched = await chat.branch("msg-999")
        mock_bridge.branch_chat.assert_awaited_once_with(chat, "msg-999", title=None)
        self.assertIsNotNone(branched)

        await chat.delete()
        mock_bridge.delete_chat.assert_awaited_once_with(chat)

    async def test_project_lifecycle_methods(self) -> None:
        acc = Account.from_cookies("claude", {"sessionKey": "claude-key"})
        session = acc.connect()

        mock_bridge = AsyncMock(spec=ProviderBridge)
        session._bridge = mock_bridge

        async def update_project(project, *, title=None, instructions=None) -> None:
            if title is not None:
                project.title = title
            if instructions is not None:
                project.instruction._text = instructions

        async def add_resource(project, item):
            return {**item, "id": f"remote-{item['name']}"}

        mock_bridge.update_project.side_effect = update_project
        mock_bridge.add_project_resource.side_effect = add_resource

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
        self.assertEqual(mock_bridge.add_project_resource.await_args.args[1]["type"], "text")

        res = project.resources.get(added["id"])
        self.assertEqual(res["id"], added["id"])
        self.assertEqual(await project.resources.download(added["id"]), "Doc")

        created_res = await project.resources.create("Doc 2", resource_type="text", name="res-2")
        self.assertEqual(created_res["data"], "Doc 2")

        await session.projects.update(
            project, title="Updated Project", instruction="Updated Instruction"
        )
        self.assertEqual(project.title, "Updated Project")
        self.assertEqual(await project.instruction.get(), "Updated Instruction")
        self.assertEqual(
            mock_bridge.update_project.await_args.kwargs,
            {
                "title": "Updated Project",
                "instructions": "Updated Instruction",
            },
        )

        with self.assertRaises(UnsupportedOperationError):
            await project.resources.update(added["id"], data="Updated Doc")
        self.assertEqual(project.resources.get(added["id"])["data"], "Doc")

        await project.resources.delete(created_res["id"])
        await project.resources.remove(added["id"])
        self.assertEqual(mock_bridge.remove_project_resource.await_count, 2)

        await project.delete()
        mock_bridge.delete_project.assert_awaited_once_with(project)

    async def test_message_branch_retry_edit(self) -> None:
        acc = Account.from_cookies("grok", {"sso": "grok-sso"})
        session = acc.connect()
        chat = await session.chats.create()

        mock_bridge = AsyncMock(spec=ProviderBridge)
        session._bridge = mock_bridge

        msg = Message(
            content="Original prompt",
            role="user",
            id="msg-orig-1",
            metadata={"parent_message_id": "root"},
            chat=chat,
        )

        retry_op = msg.retry()
        self.assertIsInstance(retry_op, MessageOperation)
        self.assertEqual(retry_op.message.metadata.get("action"), "retry")

        edit_op = msg.edit("Edited prompt")
        self.assertIsInstance(edit_op, MessageOperation)
        self.assertEqual(edit_op.message.content, "Edited prompt")
        self.assertEqual(edit_op.message.metadata.get("action"), "edit")

        update_op = chat.messages.update(msg, content="Updated via messages.update")
        self.assertIsInstance(update_op, MessageOperation)
        self.assertEqual(update_op.message.content, "Updated via messages.update")
        self.assertEqual(update_op.message.metadata.get("action"), "edit")

        mock_bridge.branch_chat.return_value = MagicMock(remote_id="branch-chat-id")
        branched_chat = await msg.branch("Dallanmış Sohbet")
        mock_bridge.branch_chat.assert_awaited_once_with(
            chat, "msg-orig-1", title="Dallanmış Sohbet"
        )
        self.assertIsNotNone(branched_chat)

    def test_provider_facades(self) -> None:
        p1 = ChatGPTProvider(cookies={"token": "t1"})
        self.assertEqual(p1.provider_id, "chatgpt")
        self.assertEqual(p1.account.provider, "chatgpt")

        p2 = ClaudeProvider(cookies={"token": "t2"})
        self.assertEqual(p2.provider_id, "claude")

        p3 = GeminiProvider(cookies={"token": "t3"})
        self.assertEqual(p3.provider_id, "gemini")

        p4 = GrokProvider(cookies={"token": "t4"})
        self.assertEqual(p4.provider_id, "grok")

    def test_catalogs_and_selection_resolver(self) -> None:
        self.assertEqual(ChatGPTModels.GPT_6_INSTANT.id, "gpt-6-instant")
        self.assertEqual(ClaudeModels.SONNET_5_5.id, "claude-sonnet-5-5")
        self.assertEqual(GeminiModels.FLASH.id, "gemini-flash")
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
        self.assertEqual(msg.attachments[0].name, "hello.txt")

    async def test_bridge_provider_instantiation_for_all_backends(self) -> None:
        p_chatgpt = ChatGPTProvider(cookies={"session_token": "token-1"})
        s_chatgpt = p_chatgpt.connect()
        adapter_chatgpt = s_chatgpt._bridge._get_provider()
        self.assertEqual(adapter_chatgpt.id, "chatgpt")
        await s_chatgpt.close()

        p_gemini = GeminiProvider(cookies={"__Secure-1PSID": "token-2"})
        s_gemini = p_gemini.connect()
        adapter_gemini = s_gemini._bridge._get_provider()
        self.assertEqual(adapter_gemini.id, "gemini")
        await s_gemini.close()

        p_claude = ClaudeProvider(cookies={"sessionKey": "token-3"})
        s_claude = p_claude.connect()
        adapter_claude = s_claude._bridge._get_provider()
        self.assertEqual(adapter_claude.id, "claude")
        await s_claude.close()

        p_grok = GrokProvider(cookies={"sso": "token-4"})
        s_grok = p_grok.connect()
        adapter_grok = s_grok._bridge._get_provider()
        self.assertEqual(adapter_grok.id, "grok")
        await s_grok.close()

    async def test_bridge_preserves_typed_provider_errors(self) -> None:
        from webaiprovider.errors import RateLimited

        p = GrokProvider(cookies={"sso": "grok-tok"})
        session = p.connect()
        chat = await session.chats.create()
        op = chat.messages.create("Hello")

        mock_bridge = MagicMock(spec=ProviderBridge)

        async def fail_stream(_op):
            raise RateLimited(retry_after=42.0)
            yield OutputBegin()

        mock_bridge.execute_stream = fail_stream
        session._bridge = mock_bridge

        with self.assertRaises(RateLimited) as ctx:
            await op.start()
        self.assertEqual(ctx.exception.retry_after, 42.0)
        await session.close()

    async def test_provider_ask_convenience_method(self) -> None:
        p = ChatGPTProvider(cookies={"session_token": "mock-tok"})
        mock_bridge = MagicMock(spec=ProviderBridge)

        async def fake_stream(_op):
            yield OutputBegin()
            yield OutputDelta(delta="Direct answer")
            yield OutputEnd()

        mock_bridge.execute_stream = fake_stream

        with patch("webaiprovider.bridge.get_bridge", return_value=mock_bridge):
            res = await p.ask("What is 2+2?")
            self.assertEqual(res.text, "Direct answer")
            self.assertEqual(res.message.content, "Direct answer")

    async def test_bridge_model_resolution_from_string_and_policy(self) -> None:
        acc = Account.from_cookies("chatgpt", {"session_token": "tok"})
        bridge = ProviderBridge(acc)

        captured_reqs = []
        mock_provider = MagicMock()
        mock_provider.id = acc.provider
        mock_provider.account_id = acc.id
        from webaiprovider.models import Choice, FeatureState, ModelCatalog
        from webaiprovider.models import Model as CatalogModel

        mock_provider.models = AsyncMock(
            return_value=ModelCatalog(
                FeatureState.AVAILABLE,
                (
                    CatalogModel("gpt-6-instant", "GPT-6 Instant"),
                    CatalogModel(
                        "gpt-6-thinking",
                        "GPT-6 Thinking",
                        (Choice("standard", "Standard"), Choice("extended", "Extended")),
                    ),
                ),
                default_model="gpt-6-instant",
            )
        )
        mock_provider.close = AsyncMock()

        async def fake_stream(req):
            captured_reqs.append(req)
            yield OutputBegin()
            yield OutputDelta("hi")
            yield OutputEnd()

        mock_provider.stream = fake_stream
        bridge._provider_instance = mock_provider

        session = ProviderSession(acc, bridge=bridge)
        chat = await session.chats.create()

        op1 = chat.messages.create("test", model="gpt-6-instant")
        await op1.start()
        self.assertEqual(captured_reqs[-1].model, "gpt-6-instant")

        op2 = chat.messages.create("test", model=ChatGPTModels.GPT_6_THINKING)
        await op2.start()
        self.assertEqual(captured_reqs[-1].model, "gpt-6-thinking")

        op2_effort = chat.messages.create("test", model=ChatGPTModels.GPT_6_THINKING.efforts.HIGH)
        await op2_effort.start()
        self.assertEqual(captured_reqs[-1].model, "gpt-6-thinking")
        self.assertEqual(captured_reqs[-1].effort, "extended")

        op_auto = chat.messages.create("test", model=Models.AUTO)
        await op_auto.start()
        self.assertEqual(captured_reqs[-1].model, "gpt-6-instant")

        op3 = chat.messages.create("test", model=Models.HIGH)
        await op3.start()
        self.assertEqual(captured_reqs[-1].model, "gpt-6-thinking")
        await session.close()

    def test_collections_abstract_base_classes_and_no_shims(self) -> None:
        from abc import ABC

        import webaiprovider.collections
        import webaiprovider.resources
        from webaiprovider.collections.base import (
            AsyncQueryableCollection,
            ManageableCollection,
            QueryableCollection,
        )

        self.assertTrue(issubclass(QueryableCollection, ABC))
        self.assertTrue(issubclass(ManageableCollection, ABC))
        self.assertTrue(issubclass(ManageableCollection, QueryableCollection))
        self.assertTrue(issubclass(AsyncQueryableCollection, ABC))

        with self.assertRaises(TypeError):
            type("IncompleteManageable", (ManageableCollection,), {})()

        self.assertFalse(hasattr(webaiprovider, "ResourceCollection"))
        self.assertFalse(hasattr(webaiprovider.collections, "ResourceCollection"))
        self.assertFalse(hasattr(webaiprovider.resources, "ProjectInstructions"))


if __name__ == "__main__":
    unittest.main()
