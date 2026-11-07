from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import webai_provider
import webaiprovider
from webai_provider import (
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
from webai_provider.bridge import ProviderBridge
from webai_provider.contracts import EffortSet, ModelRef


class TestV2Api(unittest.IsolatedAsyncioTestCase):
    def test_package_exports_parity(self) -> None:
        self.assertEqual(webaiprovider.__version__, webai_provider.__version__)
        self.assertIs(webaiprovider.Account, webai_provider.Account)
        self.assertIs(webaiprovider.Message, webai_provider.Message)
        self.assertIs(webaiprovider.ChatGPTProvider, webai_provider.ChatGPTProvider)
        self.assertIs(webaiprovider.SelectionResolver, webai_provider.SelectionResolver)

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

    async def test_response_with_sources_and_artifacts(self) -> None:
        acc = Account.from_cookies("gemini", {"cookie": "gemini-val"})
        session = acc.connect()
        chat = await session.chats.create()
        op = chat.messages.create("Explain relativity")

        mock_bridge = MagicMock(spec=ProviderBridge)

        async def fake_stream(_operation: MessageOperation):
            yield OutputBegin()
            yield OutputDelta(delta="Relativity explained.")
            from webai_provider.contracts import ArtifactDiscovered, SourceDiscovered

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

        await chat.move_to("proj-456")
        mock_bridge.move_chat.assert_awaited_once_with(chat, "proj-456")
        self.assertEqual(chat.project_id, "proj-456")

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

        project = await session.projects.create(
            title="Test Project", instruction="Always be concise"
        )
        project.remote_id = "proj-abc"

        await project.rename("Renamed Project")
        mock_bridge.rename_project.assert_awaited_once_with(project, "Renamed Project")
        self.assertEqual(project.title, "Renamed Project")

        await project.set_instruction("New instruction")
        mock_bridge.update_project_instruction.assert_awaited_once_with(project, "New instruction")
        self.assertEqual(await project.instructions.get(), "New instruction")

        added = await project.add_resource("Doc", title="res-1")
        mock_bridge.add_project_resource.assert_awaited_once_with(project, added)

        await project.remove_resource(added["id"])
        mock_bridge.remove_project_resource.assert_awaited_once_with(project, added["id"])

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
        from webai_provider.errors import RateLimited

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

        with patch("webai_provider.bridge.get_bridge", return_value=mock_bridge):
            res = await p.ask("What is 2+2?")
            self.assertEqual(res.text, "Direct answer")
            self.assertEqual(res.message.content, "Direct answer")

    async def test_bridge_model_resolution_from_string_and_policy(self) -> None:
        acc = Account.from_cookies("chatgpt", {"session_token": "tok"})
        bridge = ProviderBridge(acc)

        captured_reqs = []
        mock_provider = MagicMock()
        from webai_provider.models import Choice, FeatureState, ModelCatalog
        from webai_provider.models import Model as CatalogModel

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


if __name__ == "__main__":
    unittest.main()
