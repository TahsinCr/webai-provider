from __future__ import annotations

import asyncio
import importlib.util
import threading
import unittest
from unittest.mock import AsyncMock, patch

import webaiprovider
from tests.streaming_support import StreamTestProvider
from webaiprovider import (
    Account,
    AmbiguousMatchError,
    Artifact,
    AsyncChatGPTProjectsCollection,
    AsyncChatGPTProvider,
    AsyncClaudeProvider,
    AsyncGeminiProvider,
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
from webaiprovider.errors import ProviderError, UnsupportedOperationError
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
        for name in (
            "prepare",
            "_close_provider",
            "_load_model_catalog",
            "authenticate",
            "session_status",
            "forget_session",
            "list_chats",
            "get_chat",
            "branch_chat",
            "conversation_attachment_count",
            "rename_chat",
            "delete_chat",
            "upload_file",
            "list_projects",
            "get_project",
            "list_project_chats",
            "create_project",
            "update_project",
            "project_options",
            "add_project_resource",
            "remove_project_resource",
            "delete_project",
            "move_chat",
            "resolve_download",
        ):
            setattr(provider, name, AsyncMock())
        await provider.open()
        self.addAsyncCleanup(provider.close)
        return provider

    def test_canonical_package_namespace(self) -> None:
        self.assertIsNotNone(importlib.util.find_spec("webaiprovider"))
        self.assertIsNone(importlib.util.find_spec("webai_provider"))
        self.assertIs(webaiprovider.Account, Account)
        self.assertIs(webaiprovider.Message, Message)
        self.assertIs(webaiprovider.AsyncChatGPTProvider, AsyncChatGPTProvider)
        self.assertIs(webaiprovider.SelectionResolver, SelectionResolver)

    def test_account_creation_and_session(self) -> None:
        acc = Account.from_cookies("chatgpt", {"session_token": "mock-token-xyz"})
        self.assertEqual(acc.provider, "chatgpt")
        self.assertTrue(acc.is_active)
        self.assertIn("session_token", acc.store().read())
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
        self.assertEqual(response.sources[0].title, "Einstein Paper")
        self.assertEqual(len(response.artifacts), 1)
        self.assertEqual(response.artifacts[0].name, "spacetime.png")

    async def test_chat_lifecycle_methods(self) -> None:
        acc = Account.from_cookies("claude", {"sessionKey": "claude-key"})
        session = await self.open_provider(acc)
        mock_provider = session
        chat = await session.chats.create(title="My Claude Chat")
        chat.remote_id = "chat-123"

        await chat.rename("Renamed Chat")
        mock_provider.rename_chat.assert_awaited_once_with("chat-123", "Renamed Chat")
        self.assertEqual(chat.title, "Renamed Chat")

        await session.chats.update(chat, title="Updated Via Collection")
        await session.chats.update(chat.id, title="Updated Via ID")
        self.assertEqual(chat.title, "Updated Via ID")
        self.assertEqual((await session.chats.find("Updated", startswith=True)).id, chat.id)
        self.assertEqual((await session.chats.find("ID", endswith=True)).id, chat.id)

        with patch(
            "webaiprovider.resources.chat.chat.download_source",
            AsyncMock(return_value=b"file-bytes"),
        ):
            dl = await chat.download("https://example.com/test.png")
        mock_provider.resolve_download.assert_awaited_once_with(
            "https://example.com/test.png", conversation_id="chat-123"
        )
        self.assertEqual(dl, b"file-bytes")

        op = chat.messages.create("Hello test")
        self.assertEqual((await chat.get(op.message.id)).content, "Hello test")
        await chat.delete()
        mock_provider.delete_chat.assert_awaited_once_with("chat-123")

    async def test_project_lifecycle_methods(self) -> None:
        from dataclasses import replace

        from webaiprovider.resources.common import (
            Origin,
            ResourceData,
        )
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        acc = Account.from_cookies("claude", {"sessionKey": "claude-key"})
        session = await self.open_provider(acc)
        mock_provider = session
        remote = [
            NativeProject(
                Origin(acc.provider, acc.id, "proj-abc"),
                "Test Project",
                "Always be concise",
            )
        ]

        async def create_project(name, *, instructions):
            remote[0] = replace(remote[0], name=name, instructions=instructions)
            return remote[0]

        async def get_project(_project_id):
            return remote[0]

        async def update_project(project):
            remote[0] = project
            return project

        async def add_resource(_project_id, kind, **options):
            resource = ResourceData(
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

        mock_provider.create_project.side_effect = create_project
        mock_provider.get_project.side_effect = get_project
        mock_provider.update_project.side_effect = update_project
        mock_provider.add_project_resource.side_effect = add_resource
        mock_provider.remove_project_resource.side_effect = remove_resource

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
        mock_provider.add_project_resource.assert_awaited_once_with(
            "proj-abc", "text", name="res-1", text="Doc"
        )
        self.assertEqual(await project.resources.download(added["id"]), "Doc")

        created_res = await project.resources.create("Doc 2", resource_type="text", name="res-2")
        self.assertEqual(created_res["data"], "Doc 2")
        await session.projects.update(
            project, title="Updated Project", instruction="Updated Instruction"
        )
        submitted = mock_provider.update_project.await_args.args[0]
        self.assertEqual(submitted.name, "Updated Project")
        self.assertEqual(submitted.instructions, "Updated Instruction")
        self.assertEqual(project.title, "Updated Project")
        self.assertEqual(await project.instruction.get(), "Updated Instruction")

        with self.assertRaises(UnsupportedOperationError):
            await project.resources.update(added["id"], data="Updated Doc")
        self.assertEqual((await project.resources.get(added["id"]))["data"], "Doc")

        await project.resources.delete(created_res["id"])
        await project.resources.remove(added["id"])
        self.assertEqual(mock_provider.remove_project_resource.await_count, 2)
        await project.delete()
        mock_provider.delete_project.assert_awaited_once_with("proj-abc")

    async def test_message_branch_retry_edit(self) -> None:
        from webaiprovider.resources.chat.types import Conversation
        from webaiprovider.resources.common import Origin

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

        session.branch_chat.return_value = Conversation(
            Origin(acc.provider, acc.id, "branch-chat-id"), "Branch chat"
        )
        branched_chat = await chat.branch(msg.id, title="Branch chat")
        session.branch_chat.assert_awaited_once_with("remote-chat-id", "msg-orig-1")
        self.assertEqual(branched_chat.remote_id, "branch-chat-id")

    def test_generic_constructors_select_native_provider_classes(self) -> None:
        for kind, title in (
            ("chatgpt", "ChatGPT"),
            ("claude", "Claude"),
            ("gemini", "Gemini"),
            ("grok", "Grok"),
        ):
            account = Account.from_cookies(kind, {})
            with self.subTest(provider=kind):
                native_async = getattr(webaiprovider, f"Async{title}Provider")
                native_sync = getattr(webaiprovider, f"{title}Provider")
                self.assertIs(type(AsyncProvider(account)), native_async)
                provider = Provider(account, type=ProviderType.parse(kind))
                self.assertIs(type(provider), native_sync)
                self.assertIs(type(provider._provider), native_async)
                provider.close()

    def test_provider_facades(self) -> None:
        from webaiprovider.providers.chatgpt import ChatGPTChat
        from webaiprovider.providers.claude import ClaudeChat
        from webaiprovider.providers.gemini import GeminiChat
        from webaiprovider.providers.grok import GrokChat
        from webaiprovider.resources.chat.chat import Chat
        from webaiprovider.resources.chat.collections import ChatsCollection
        from webaiprovider.resources.message.collections import MessagesCollection
        from webaiprovider.resources.project.chats import ProjectChatsCollection

        self.assertFalse(hasattr(Chat, "branch"))
        self.assertFalse(hasattr(Chat, "move_to"))
        self.assertFalse(hasattr(Message, "branch"))
        self.assertFalse(hasattr(ChatsCollection, "branch"))
        self.assertFalse(hasattr(ChatsCollection, "move"))
        self.assertFalse(hasattr(MessagesCollection, "branch"))
        self.assertFalse(hasattr(ProjectChatsCollection, "add"))
        self.assertFalse(hasattr(ProjectChatsCollection, "remove"))
        self.assertFalse(hasattr(ProjectChatsCollection, "add"))
        self.assertFalse(hasattr(ProjectChatsCollection, "remove"))
        self.assertTrue(hasattr(ChatGPTChat, "branch"))
        self.assertTrue(hasattr(ChatGPTChat, "move_to"))
        self.assertTrue(hasattr(ClaudeChat, "move_to"))
        self.assertFalse(hasattr(ClaudeChat, "branch"))
        self.assertTrue(hasattr(GeminiChat, "branch"))
        self.assertFalse(hasattr(GeminiChat, "move_to"))
        self.assertTrue(hasattr(GrokChat, "move_to"))
        self.assertFalse(hasattr(GrokChat, "branch"))

        p1 = AsyncChatGPTProvider(Account.from_cookies("chatgpt", {"token": "t1"}))
        self.assertEqual(p1.type, ProviderType.CHATGPT)
        self.assertEqual(p1.account.provider, "chatgpt")
        self.assertIsInstance(p1.projects, AsyncChatGPTProjectsCollection)

        p2 = AsyncClaudeProvider(Account.from_cookies("claude", {"token": "t2"}))
        self.assertEqual(p2.type, ProviderType.CLAUDE)

        p3 = AsyncGeminiProvider(Account.from_cookies("gemini", {"token": "t3"}))
        self.assertEqual(p3.type, ProviderType.GEMINI)

        p4 = AsyncGrokProvider(Account.from_cookies("grok", {"token": "t4"}))
        self.assertEqual(p4.type, ProviderType.GROK)
        with self.assertRaisesRegex(ValueError, "must match"):
            AsyncProvider(Account.from_cookies("chatgpt", {}), type=ProviderType.GROK)

    async def test_provider_open_is_idempotent_and_closes_provider_resources(self) -> None:
        account = Account.from_cookies("gemini", {"cookie": "test"})
        provider = AsyncProvider(account, type=ProviderType.GEMINI)
        provider.prepare = AsyncMock()
        provider._close_provider = AsyncMock()
        self.assertIs(await provider.open(), provider)
        self.assertIs(await provider.open(), provider)
        provider.prepare.assert_awaited_once_with()
        await provider.close()
        provider._close_provider.assert_awaited_once_with()

    async def test_chatgpt_project_creation_accepts_memory_share_flag(self) -> None:
        from webaiprovider.resources.common import Origin
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        provider = AsyncChatGPTProvider(account)
        provider.prepare = AsyncMock()
        provider._close_provider = AsyncMock()
        provider.create_project = AsyncMock(
            return_value=NativeProject(Origin("chatgpt", account.id, "project-1"), "Private notes")
        )
        await provider.open()
        project = await provider.projects.create("Private notes", memory_share=False)
        self.assertEqual(project.title, "Private notes")
        self.assertEqual(provider.create_project.await_args.kwargs["memory_share"], False)
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

    async def test_provider_types_implement_shared_provider_api(self) -> None:
        from webaiprovider.providers.chatgpt.provider import AsyncChatGPTProvider
        from webaiprovider.providers.claude.provider import AsyncClaudeProvider
        from webaiprovider.providers.gemini.provider import AsyncGeminiProvider
        from webaiprovider.providers.grok.provider import AsyncGrokProvider

        providers = (
            ("chatgpt", {"session_token": "token-1"}, AsyncChatGPTProvider),
            ("gemini", {"__Secure-1PSID": "token-2"}, AsyncGeminiProvider),
            ("claude", {"sessionKey": "token-3"}, AsyncClaudeProvider),
            ("grok", {"sso": "token-4"}, AsyncGrokProvider),
        )
        for provider_name, credentials, provider_type in providers:
            account = Account.from_cookies(provider_name, credentials)
            provider = provider_type(account)
            self.assertIsInstance(provider, AsyncProvider)
            self.assertEqual(provider.account_id, account.id)
            await provider.close()

    def test_sync_provider_exposes_native_capabilities(self) -> None:
        from types import SimpleNamespace

        from webaiprovider.base import Capability

        provider = object.__new__(Provider)
        capabilities = frozenset({Capability.CHAT, Capability.MODELS})
        provider._provider = SimpleNamespace(capabilities=capabilities)

        self.assertIs(provider.capabilities, capabilities)

    async def test_provider_stream_preserves_typed_errors(self) -> None:
        from webaiprovider.errors import RateLimited

        provider = AsyncGrokProvider(Account.from_cookies("grok", {"sso": "grok-tok"}))
        provider.prepare = AsyncMock()
        provider._close_provider = AsyncMock()

        async def fail_stream(_request):
            raise RateLimited(retry_after=42.0)
            yield TextDelta("unreachable")

        provider._stream_request = fail_stream
        await provider.open()
        chat = await provider.chats.create()
        operation = chat.messages.create("Hello")
        with self.assertRaises(RateLimited) as ctx:
            await operation.start()
        self.assertEqual(ctx.exception.retry_after, 42.0)
        await provider.close()

    async def test_provider_ask_convenience_method(self) -> None:
        p = AsyncChatGPTProvider(Account.from_cookies("chatgpt", {"session_token": "mock-tok"}))
        p.prepare = AsyncMock()
        p._close_provider = AsyncMock()

        async def fake_stream(_op):
            yield TextDelta("Direct answer")

        p._stream_request = fake_stream
        await p.open()
        res = await p.ask("What is 2+2?")
        self.assertEqual(res.text, "Direct answer")
        self.assertEqual(res.message.content, "Direct answer")

    async def test_model_resolution_from_string_and_policy(self) -> None:
        from webaiprovider.resources.common import Choice
        from webaiprovider.resources.model import ModelCatalogState
        from webaiprovider.resources.model.model import Model as CatalogModel
        from webaiprovider.resources.model.model import ModelCatalog

        account = Account.from_cookies("chatgpt", {"session_token": "tok"})
        provider = StreamTestProvider(account.provider, account.id, (TextDelta("hi"),))
        provider = await self.open_provider(account, provider)
        provider._load_model_catalog = AsyncMock(
            return_value=ModelCatalog(
                ModelCatalogState.AVAILABLE,
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
        chat = await provider.chats.create()

        operation = chat.messages.create("test", model="gpt-6-instant")
        await operation.start()
        self.assertEqual(provider.requests[-1].model, "gpt-6-instant")
        operation = chat.messages.create("test", model=ChatGPTModels.GPT_6_THINKING)
        await operation.start()
        self.assertEqual(provider.requests[-1].model, "gpt-6-thinking")
        operation = chat.messages.create("test", model=ChatGPTModels.GPT_6_THINKING.efforts.HIGH)
        await operation.start()
        self.assertEqual(provider.requests[-1].model, "gpt-6-thinking")
        self.assertEqual(provider.requests[-1].effort, "extended")
        operation = chat.messages.create("test", model=Models.AUTO)
        await operation.start()
        self.assertEqual(provider.requests[-1].model, "gpt-6-instant")
        operation = chat.messages.create("test", model=Models.HIGH)
        await operation.start()
        self.assertEqual(provider.requests[-1].model, "gpt-6-thinking")

    def test_collections_abstract_base_classes_and_no_shims(self) -> None:
        from abc import ABC

        import webaiprovider.collections
        import webaiprovider.resources
        from webaiprovider.collections import (
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
        from webaiprovider.resources.common import Origin
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        class RecordingChatGPTProvider(AsyncChatGPTProvider):
            def __init__(self, account: Account, **options) -> None:
                super().__init__(account, **options)
                self.thread_ids: list[int] = []
                self.memory_share = None

            async def prepare(self) -> None:
                self.thread_ids.append(threading.get_ident())

            async def create_project(self, name, *, instructions="", memory_share=True):
                self.thread_ids.append(threading.get_ident())
                self.memory_share = memory_share
                return NativeProject(
                    Origin("chatgpt", self.account_id, "project-1"),
                    name,
                    instructions,
                )

            async def _stream_request(self, _request):
                self.thread_ids.append(threading.get_ident())
                yield TextDelta("done")

            async def _close_provider(self) -> None:
                self.thread_ids.append(threading.get_ident())

        account = Account.from_cookies("chatgpt", {"cookie": "test"})
        with patch.object(ChatGPTProvider, "async_provider_type", RecordingChatGPTProvider):
            with ChatGPTProvider(account) as provider:
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


if __name__ == "__main__":
    unittest.main()
