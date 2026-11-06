from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, MagicMock

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
    Efforts,
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
from webai_provider.contracts import ModelRef


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
            model=ChatGPTModels.GPT_4O,
            effort=Efforts.MEDIUM,
        )

        self.assertIsInstance(op, MessageOperation)
        self.assertEqual(op.message.content, "Hello AI")
        self.assertEqual(op.message.model, ChatGPTModels.GPT_4O)
        self.assertEqual(op.message.effort, Efforts.MEDIUM)

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
        self.assertEqual(ChatGPTModels.GPT_4O.id, "gpt-4o")
        self.assertEqual(ClaudeModels.CLAUDE_3_5_SONNET.id, "claude-3-5-sonnet")
        self.assertEqual(GeminiModels.GEMINI_2_5_PRO.id, "gemini-2.5-pro")
        self.assertEqual(GrokModels.GROK_3.id, "grok-3")

        candidates = [
            ModelRef(provider="test", id="fast", name="Model Fast"),
            ModelRef(provider="test", id="smart", name="Model Smart"),
            ModelRef(provider="test", id="expert", name="Model Expert"),
        ]

        self.assertEqual(SelectionResolver.resolve_model(Models.LOW, candidates).id, "fast")
        self.assertEqual(SelectionResolver.resolve_model(Models.MEDIUM, candidates).id, "smart")
        self.assertEqual(SelectionResolver.resolve_model(Models.HIGH, candidates).id, "expert")
        self.assertEqual(SelectionResolver.resolve_model(Models.AUTO, candidates).id, "smart")

        with self.assertRaises(AmbiguousMatchError):
            SelectionResolver.resolve_model(
                "Model",
                [
                    ModelRef(provider="test", id="m1", name="Model One"),
                    ModelRef(provider="test", id="m2", name="Model Two"),
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


if __name__ == "__main__":
    unittest.main()
