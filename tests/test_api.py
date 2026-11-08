from __future__ import annotations

import asyncio
import importlib.util
import threading
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import webaiprovider
from tests.streaming_support import StreamTestAdapter
from webaiprovider import (
    Account,
    AmbiguousMatchError,
    Artifact,
    Attachment,
    Attachments,
    ChatGPTModels,
    ChatGPTProjectsCollection,
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
    Provider,
    ProviderType,
    Response,
    SelectionResolver,
    Source,
    SyncChatGPTProvider,
    SyncProvider,
    ThinkingBegin,
    ThinkingDelta,
    ThinkingEnd,
)
from webaiprovider.base import _TextDelta
from webaiprovider.bridge import ProviderBridge
from webaiprovider.contracts import EffortSet, ModelRef
from webaiprovider.errors import ProviderError, UnsupportedOperationError


class TestApi(unittest.IsolatedAsyncioTestCase):
    async def open_provider(
        self,
        account: Account,
        bridge: ProviderBridge | None = None,
    ) -> tuple[Provider, ProviderBridge]:
        if bridge is None:
            bridge = AsyncMock(spec=ProviderBridge)
        provider = Provider(account)
        with patch("webaiprovider.bridge.ProviderBridge", return_value=bridge):
            await provider.open()
        self.addAsyncCleanup(provider.close)
        return provider, bridge

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
        self.assertFalse(hasattr(acc, "connect"))

        provider = Provider(acc, type=ProviderType.CHATGPT)
        self.assertIsInstance(provider, Provider)
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
        provider, mock_bridge = await self.open_provider(acc)
        chat = await provider.chats.create(title="Test Chat")

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

        async def fake_stream(_operation: MessageOperation):
            yield ThinkingBegin()
            yield ThinkingDelta(delta="Reasoning step 1")
            yield ThinkingEnd()
            yield OutputBegin()
            yield OutputDelta(delta="Hello ")
            yield OutputDelta(delta="World!")
            yield OutputEnd()

        mock_bridge.execute_stream = fake_stream
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
        provider, _ = await self.open_provider(Account.from_cookies("chatgpt", {"cookie": "val"}))
        chat = await provider.chats.create()
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
        provider = Provider(Account.from_cookies("chatgpt", {"cookie": "val"}))
        chat = await provider.chats.create()

        with self.assertRaisesRegex(ProviderError, "No provider session is attached"):
            await chat.messages.create("must not be echoed as a response").wait()

    async def test_response_with_sources_and_artifacts(self) -> None:
        acc = Account.from_cookies("gemini", {"cookie": "gemini-val"})
        provider, mock_bridge = await self.open_provider(acc)
        chat = await provider.chats.create()
        op = chat.messages.create("Explain relativity")

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
        response = await op.start()
        self.assertEqual(response.text, "Relativity explained.")
        self.assertEqual(len(response.sources), 1)
        self.assertEqual(response.sources[0].title, "Einstein Paper")
        self.assertEqual(len(response.artifacts), 1)
        self.assertEqual(response.artifacts[0].name, "spacetime.png")

    async def test_chat_lifecycle_methods(self) -> None:
        acc = Account.from_cookies("claude", {"sessionKey": "claude-key"})
        session, mock_bridge = await self.open_provider(acc)

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
        session, mock_bridge = await self.open_provider(acc)

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
        session, mock_bridge = await self.open_provider(acc)
        chat = await session.chats.create()
        chat.remote_id = "remote-chat-id"

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
        p1 = ChatGPTProvider(Account.from_cookies("chatgpt", {"token": "t1"}))
        self.assertEqual(p1.type, ProviderType.CHATGPT)
        self.assertEqual(p1.account.provider, "chatgpt")
        self.assertIsInstance(p1.projects, ChatGPTProjectsCollection)

        p2 = ClaudeProvider(Account.from_cookies("claude", {"token": "t2"}))
        self.assertEqual(p2.type, ProviderType.CLAUDE)

        p3 = GeminiProvider(Account.from_cookies("gemini", {"token": "t3"}))
        self.assertEqual(p3.type, ProviderType.GEMINI)

        p4 = GrokProvider(Account.from_cookies("grok", {"token": "t4"}))
        self.assertEqual(p4.type, ProviderType.GROK)
        with self.assertRaisesRegex(ValueError, "must match"):
            Provider(Account.from_cookies("chatgpt", {}), type=ProviderType.GROK)

    async def test_provider_open_is_explicit_idempotent_and_closes_owned_bridge(self) -> None:
        account = Account.from_cookies("gemini", {"cookie": "test"})
        bridge = MagicMock(spec=ProviderBridge)
        bridge.open = AsyncMock()
        bridge.close = AsyncMock()
        with patch("webaiprovider.bridge.ProviderBridge", return_value=bridge) as create_bridge:
            provider = Provider(account, type=ProviderType.GEMINI)
            self.assertIs(await provider.open(), provider)
            self.assertIs(await provider.open(), provider)
            create_bridge.assert_called_once_with(account)
            bridge.open.assert_awaited_once_with()
            await provider.close()
            bridge.close.assert_awaited_once_with()

    async def test_chatgpt_project_creation_accepts_memory_share_flag(self) -> None:
        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        bridge = MagicMock(spec=ProviderBridge)
        bridge.create_chatgpt_project = AsyncMock()
        provider = ChatGPTProvider(account)
        with patch("webaiprovider.bridge.ProviderBridge", return_value=bridge):
            await provider.open()

        project = await provider.projects.create("Private notes", memory_share=False)

        self.assertEqual(project.title, "Private notes")
        self.assertEqual(
            bridge.create_chatgpt_project.await_args.kwargs["memory_share"],
            False,
        )

    async def test_sync_provider_rejects_blocking_inside_running_loop(self) -> None:
        provider = SyncProvider(Account.from_cookies("chatgpt", {"cookie": "test"}))
        with self.assertRaisesRegex(RuntimeError, "cannot block an event loop"):
            provider.open()
        provider.close()

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
        providers = (
            (
                ChatGPTProvider(Account.from_cookies("chatgpt", {"session_token": "token-1"})),
                "chatgpt",
            ),
            (
                GeminiProvider(Account.from_cookies("gemini", {"__Secure-1PSID": "token-2"})),
                "gemini",
            ),
            (ClaudeProvider(Account.from_cookies("claude", {"sessionKey": "token-3"})), "claude"),
            (GrokProvider(Account.from_cookies("grok", {"sso": "token-4"})), "grok"),
        )
        for provider, provider_id in providers:
            bridge = ProviderBridge(provider.account)
            bridge.open = AsyncMock()
            bridge.close = AsyncMock()
            with patch("webaiprovider.bridge.ProviderBridge", return_value=bridge):
                await provider.open()
            self.assertEqual(bridge._get_provider().id, provider_id)
            await provider.close()

    async def test_bridge_preserves_typed_provider_errors(self) -> None:
        from webaiprovider.errors import RateLimited

        p = GrokProvider(Account.from_cookies("grok", {"sso": "grok-tok"}))
        chat = await p.chats.create()
        op = chat.messages.create("Hello")

        mock_bridge = MagicMock(spec=ProviderBridge)

        async def fail_stream(_op):
            raise RateLimited(retry_after=42.0)
            yield OutputBegin()

        mock_bridge.execute_stream = fail_stream
        with patch("webaiprovider.bridge.ProviderBridge", return_value=mock_bridge):
            await p.open()

        with self.assertRaises(RateLimited) as ctx:
            await op.start()
        self.assertEqual(ctx.exception.retry_after, 42.0)
        await p.close()

    async def test_provider_ask_convenience_method(self) -> None:
        p = ChatGPTProvider(Account.from_cookies("chatgpt", {"session_token": "mock-tok"}))
        mock_bridge = MagicMock(spec=ProviderBridge)

        async def fake_stream(_op):
            yield OutputBegin()
            yield OutputDelta(delta="Direct answer")
            yield OutputEnd()

        mock_bridge.execute_stream = fake_stream

        with patch("webaiprovider.bridge.ProviderBridge", return_value=mock_bridge):
            await p.open()
        res = await p.ask("What is 2+2?")
        self.assertEqual(res.text, "Direct answer")
        self.assertEqual(res.message.content, "Direct answer")

    async def test_bridge_model_resolution_from_string_and_policy(self) -> None:
        acc = Account.from_cookies("chatgpt", {"session_token": "tok"})
        bridge = ProviderBridge(acc)
        from webaiprovider.models import Choice, FeatureState, ModelCatalog
        from webaiprovider.models import Model as CatalogModel

        adapter = StreamTestAdapter(acc.provider, acc.id, (_TextDelta("hi"),))
        adapter.models = AsyncMock(
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
        bridge._provider_instance = adapter
        bridge.open = AsyncMock()

        session, _ = await self.open_provider(acc, bridge)
        chat = await session.chats.create()

        operation = chat.messages.create("test", model="gpt-6-instant")
        await operation.start()
        self.assertEqual(adapter.requests[-1].model, "gpt-6-instant")

        operation = chat.messages.create("test", model=ChatGPTModels.GPT_6_THINKING)
        await operation.start()
        self.assertEqual(adapter.requests[-1].model, "gpt-6-thinking")

        operation = chat.messages.create("test", model=ChatGPTModels.GPT_6_THINKING.efforts.HIGH)
        await operation.start()
        self.assertEqual(adapter.requests[-1].model, "gpt-6-thinking")
        self.assertEqual(adapter.requests[-1].effort, "extended")

        operation = chat.messages.create("test", model=Models.AUTO)
        await operation.start()
        self.assertEqual(adapter.requests[-1].model, "gpt-6-instant")

        operation = chat.messages.create("test", model=Models.HIGH)
        await operation.start()
        self.assertEqual(adapter.requests[-1].model, "gpt-6-thinking")
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


class TestSyncApi(unittest.TestCase):
    def test_sync_chat_project_and_stream_share_one_portal(self) -> None:
        class Bridge:
            def __init__(self) -> None:
                self.thread_ids: list[int] = []
                self.memory_share = None

            async def open(self) -> None:
                self.thread_ids.append(threading.get_ident())

            async def create_chatgpt_project(
                self, project, instruction=None, *, memory_share=True
            ) -> None:
                self.thread_ids.append(threading.get_ident())
                self.memory_share = memory_share
                project.remote_id = "project-1"

            async def execute_stream(self, operation):
                self.thread_ids.append(threading.get_ident())
                yield OutputBegin()
                yield OutputDelta(delta="done")
                yield OutputEnd()

            async def close(self) -> None:
                self.thread_ids.append(threading.get_ident())

        bridge = Bridge()
        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        with patch("webaiprovider.bridge.ProviderBridge", return_value=bridge):
            with SyncChatGPTProvider(account) as provider:
                project = provider.projects.create("Notes", memory_share=False)
                chat = provider.chats.create("Draft", project=project)
                operation = chat.messages.create("hello")
                events = list(operation.stream())
                response = operation.wait()

        self.assertEqual(response.text, "done")
        self.assertEqual(events[0].event, "output.begin")
        self.assertEqual(events[1].event, "output.delta")
        self.assertEqual(events[2].event, "output.end")
        self.assertEqual(
            bridge.memory_share,
            False,
        )
        self.assertTrue(bridge.thread_ids)
        self.assertEqual(len(set(bridge.thread_ids)), 1)


if __name__ == "__main__":
    unittest.main()
