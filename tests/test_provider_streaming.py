from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from tests.provider_fakes import provider_account
from tests.streaming_support import StreamTestProvider
from webaiprovider import (
    Account,
    AsyncChat,
    AsyncMessage,
    AsyncMessageOperation,
    AsyncProvider,
)
from webaiprovider.collections import Page
from webaiprovider.errors import (
    AccountPaused,
    OperationCancelledError,
    ProviderError,
    RateLimited,
)
from webaiprovider.providers.chatgpt.async_provider import AsyncChatGPTProvider
from webaiprovider.providers.claude.async_provider import AsyncClaudeProvider
from webaiprovider.providers.gemini.async_provider import AsyncGeminiProvider
from webaiprovider.providers.grok.async_provider import AsyncGrokProvider
from webaiprovider.resources.message.operation import (
    ArtifactDiscovered,
    OutputBegin,
    OutputDelta,
    OutputEnd,
    SourceDiscovered,
    ThinkingBegin,
    ThinkingDelta,
    ThinkingEnd,
)
from webaiprovider.resources.message.protocol import TextDelta
from webaiprovider.resources.model import ModelCatalogState
from webaiprovider.resources.model.model import EffortOption, ModelCatalog
from webaiprovider.resources.model.model import Model as CatalogModel


def _create_mock_session(provider_name: str, account_id: str = "acc-test") -> AsyncProvider:
    account = Account.from_cookies(provider_name, {"session": "token"}, id=account_id)
    session = AsyncProvider(account)
    models_catalog = [
        CatalogModel(
            "default-model",
            "Default Model",
            (EffortOption("standard", "Standard"), EffortOption("high", "High")),
            default_effort="standard",
        )
    ]
    session.models.list = AsyncMock(
        return_value=ModelCatalog(
            ModelCatalogState.AVAILABLE, tuple(models_catalog), "default-model"
        )
    )
    return session


class AsyncProviderCancellationCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_waits_for_resource_refreshes(self) -> None:
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()
        close_started = asyncio.Event()
        provider_closed = asyncio.Event()

        class SlowProvider(AsyncProvider):
            async def _close_session(self) -> None:
                provider_closed.set()

            async def _list_chats_data(self, *, cursor=None, limit=50) -> Page:
                del cursor, limit
                refresh_started.set()
                await release_refresh.wait()
                return Page(())

            async def _list_projects_data(self, *, cursor=None) -> Page:
                del cursor
                refresh_started.set()
                await release_refresh.wait()
                return Page(())

            async def _load_catalog(self, *, force: bool = False) -> ModelCatalog:
                del force
                refresh_started.set()
                await release_refresh.wait()
                model = CatalogModel(
                    "default-model",
                    "Default Model",
                    (EffortOption("standard", "Standard"),),
                    default_effort="standard",
                )
                return ModelCatalog(ModelCatalogState.AVAILABLE, (model,), model.id)

        def make_provider() -> AsyncProvider:
            account = Account.from_cookies("chatgpt", {"session": "token"})
            provider = SlowProvider(account)
            orig_finish = provider._finish_close

            async def tracked_finish():
                close_started.set()
                await orig_finish()

            provider._finish_close = tracked_finish  # type: ignore[method-assign]
            return provider

        for refresh in (
            lambda provider: provider.chats.list(limit=None),
            lambda provider: provider.projects.list(limit=None),
            lambda provider: provider.models.refresh(),
        ):
            refresh_started.clear()
            release_refresh.clear()
            close_started.clear()
            provider_closed.clear()
            with self.subTest(refresh=refresh):
                provider = make_provider()
                await provider.open()
                refresh_task = asyncio.create_task(refresh(provider))
                await refresh_started.wait()
                self.assertEqual(provider._active_operations, 1)

                close_task = asyncio.create_task(provider.close())
                await close_started.wait()
                self.assertFalse(close_task.done())
                self.assertFalse(provider_closed.is_set())

                release_refresh.set()
                await refresh_task
                await close_task
                self.assertTrue(provider_closed.is_set())

    async def test_send_hook_cannot_await_send_in_same_conversation(self) -> None:
        provider = StreamTestProvider("chatgpt", "acc-test", (TextDelta("answer"),))
        await provider.open()
        chat = await provider.chats.create(title="Reentrant send")
        operation = chat.responses.create("first")
        nested_errors: list[ProviderError] = []

        async def send_from_hook(_event) -> None:
            try:
                await chat.responses.create("nested").start()
            except ProviderError as error:
                nested_errors.append(error)

        operation.hooks.add("output.delta", send_from_hook)
        response = await operation.start()

        self.assertEqual(response.text, "answer")
        self.assertEqual(len(provider.requests), 1)
        self.assertEqual(len(nested_errors), 1)
        self.assertIn("same conversation", str(nested_errors[0]))
        await provider.close()

    async def test_child_task_can_send_and_close_after_parent_send_finishes(self) -> None:
        provider = StreamTestProvider("chatgpt", "acc-test", (TextDelta("answer"),))
        await provider.open()
        chat = await provider.chats.create(title="Context lifetime")
        send_lock = provider._send_lock(chat.id)
        self.assertIs(send_lock, provider._send_lock(chat.id))
        operation = chat.responses.create("first")
        first_finished = asyncio.Event()
        child_task: asyncio.Task | None = None

        async def spawn_followup(_event) -> None:
            nonlocal child_task
            if child_task is not None:
                return

            async def followup():
                await first_finished.wait()
                response = await chat.responses.create("second").start()
                await provider.close()
                return response

            child_task = asyncio.create_task(followup())

        operation.hooks.add("output.delta", spawn_followup)
        await operation.start()
        first_finished.set()
        assert child_task is not None
        response = await asyncio.wait_for(child_task, timeout=1)

        self.assertEqual(response.text, "answer")
        self.assertTrue(provider.closed)

    async def test_cancel_during_hook_closes_stream_and_releases_provider_resources(self) -> None:
        stream_closed = asyncio.Event()
        provider_closed = asyncio.Event()

        class CleanupProvider(AsyncProvider):
            async def _close_session(self) -> None:
                provider_closed.set()

            async def _stream_provider_events(self, _request):
                try:
                    yield TextDelta("partial")
                    await asyncio.Event().wait()
                finally:
                    stream_closed.set()

        account = Account.from_cookies("chatgpt", {"session": "token"})
        provider = CleanupProvider(account)
        await provider.open()
        chat = await provider.chats.create(title="Cancellation")
        operation = chat.responses.create("prompt")
        hook_started = asyncio.Event()
        release_hook = asyncio.Event()

        async def block_on_output(_event) -> None:
            hook_started.set()
            await release_hook.wait()

        operation.hooks.add("output.delta", block_on_output)
        waiter = asyncio.create_task(operation.start())
        await hook_started.wait()
        operation.cancel()

        with self.assertRaises(OperationCancelledError):
            await waiter

        self.assertTrue(stream_closed.is_set())
        self.assertEqual(provider._active_operations, 0)
        self.assertFalse(provider._send_lock(chat.id).locked())
        await asyncio.wait_for(provider.close(), timeout=1)
        self.assertTrue(provider_closed.is_set())


class AsyncClaudeProviderStreamingTests(unittest.IsolatedAsyncioTestCase):
    async def test_execute_stream_emits_events_and_updates_operation(self) -> None:
        session = _create_mock_session("claude")
        chat = AsyncChat(title="Test Chat", provider=session)
        message = AsyncMessage("Hello Claude", chat=chat)
        operation = AsyncMessageOperation(chat, message)

        provider = AsyncClaudeProvider(provider_account("claude", "cred_file", "acc-test"))
        provider.client.authenticated = True

        async def fake_events(*args, **kwargs):
            del args, kwargs
            yield {"type": "bound", "id": "claude-conv-123"}
            yield {"type": "delta", "text": "Analyzing query", "channel": "analysis"}
            yield {"type": "delta", "text": "Hello ", "channel": "final"}
            yield {"type": "delta", "text": "human!", "channel": "final"}
            yield {
                "type": "metadata",
                "message_id": "claude-asst-msg-1",
                "user_message_id": "claude-user-msg-1",
                "sources": [{"id": "s1", "url": "https://claude.ai/doc", "title": "Doc"}],
                "files": [{"id": "f1", "name": "result.pdf", "size": 1024}],
            }
            yield {"type": "done"}

        provider.client.events = fake_events

        with patch(
            "webaiprovider.providers.claude.async_provider.session_exists", return_value=True
        ):
            events = [event async for event in provider._execute_stream(operation)]

        self.assertEqual(chat.remote_id, "claude-conv-123")
        self.assertEqual(operation.message.id, "claude-user-msg-1")
        self.assertEqual(operation._assistant_message_id, "claude-asst-msg-1")

        event_types = [type(e) for e in events]
        self.assertIn(ThinkingBegin, event_types)
        self.assertIn(ThinkingDelta, event_types)
        self.assertIn(ThinkingEnd, event_types)
        self.assertIn(OutputBegin, event_types)
        self.assertIn(OutputDelta, event_types)
        self.assertIn(SourceDiscovered, event_types)
        self.assertIn(ArtifactDiscovered, event_types)
        self.assertIn(OutputEnd, event_types)

        source_event = next(e for e in events if isinstance(e, SourceDiscovered))
        self.assertEqual(source_event.source.id, "s1")
        self.assertEqual(source_event.source.url, "https://claude.ai/doc")

        artifact_event = next(e for e in events if isinstance(e, ArtifactDiscovered))
        self.assertEqual(artifact_event.artifact.id, "f1")
        self.assertEqual(artifact_event.artifact.name, "result.pdf")

    async def test_execute_stream_rejects_unauthenticated_session(self) -> None:
        session = _create_mock_session("claude")
        chat = AsyncChat(title="Test Chat", provider=session)
        message = AsyncMessage("Hello Claude", chat=chat)
        operation = AsyncMessageOperation(chat, message)

        provider = AsyncClaudeProvider(provider_account("claude", "cred_file", "acc-test"))
        provider.client.authenticated = False

        with (
            patch(
                "webaiprovider.providers.claude.async_provider.session_exists", return_value=False
            ),
            self.assertRaises(AccountPaused),
        ):
            _ = [event async for event in provider._execute_stream(operation)]


class AsyncGrokProviderStreamingTests(unittest.IsolatedAsyncioTestCase):
    async def test_execute_stream_emits_events_and_caches_continuation(self) -> None:
        session = _create_mock_session("grok")
        chat = AsyncChat(title="Test Chat", provider=session)
        message = AsyncMessage("Hello Grok", chat=chat)
        operation = AsyncMessageOperation(chat, message)

        provider = AsyncGrokProvider(provider_account("grok", "cred_file", "acc-test"))
        catalog = ModelCatalog(
            ModelCatalogState.AVAILABLE,
            (CatalogModel("grok-3", "Grok 3", metadata={"modelName": "grok-3"}),),
            "grok-3",
        )
        provider._load_catalog = AsyncMock(return_value=catalog)

        async def fake_events(*args, **kwargs):
            del args, kwargs
            yield {
                "result": {
                    "conversation": {"conversationId": "grok-conv-456"},
                    "response": {
                        "userResponse": {"responseId": "grok-user-456"},
                        "modelResponse": {
                            "responseId": "grok-resp-456",
                            "model": "grok-3",
                            "webSearchResults": [{"url": "https://x.com", "title": "X"}],
                        },
                        "token": "Grok answering",
                        "done": True,
                    },
                }
            }

        provider.client.events = fake_events

        events = [event async for event in provider._execute_stream(operation)]

        self.assertEqual(chat.remote_id, "grok-conv-456")
        self.assertEqual(operation.message.id, "grok-user-456")
        self.assertEqual(operation._assistant_message_id, "grok-resp-456")
        self.assertEqual(provider._take_continuation("grok-conv-456"), "grok-resp-456")

        event_types = [type(e) for e in events]
        self.assertIn(OutputBegin, event_types)
        self.assertIn(OutputDelta, event_types)
        self.assertIn(SourceDiscovered, event_types)
        self.assertIn(OutputEnd, event_types)

    async def test_execute_stream_raises_on_rate_limit(self) -> None:
        session = _create_mock_session("grok")
        chat = AsyncChat(title="Test Chat", provider=session)
        message = AsyncMessage("Hello Grok", chat=chat)
        operation = AsyncMessageOperation(chat, message)

        provider = AsyncGrokProvider(provider_account("grok", "cred_file", "acc-test"))
        catalog = ModelCatalog(
            ModelCatalogState.AVAILABLE,
            (CatalogModel("grok-3", "Grok 3", metadata={"modelName": "grok-3"}),),
            "grok-3",
        )
        provider._load_catalog = AsyncMock(return_value=catalog)

        async def fake_events(*args, **kwargs):
            del args, kwargs
            yield {
                "result": {
                    "conversationId": "grok-conv-789",
                    "error": {"code": "RATE_LIMIT_EXCEEDED", "retryAfter": 12.5},
                }
            }

        provider.client.events = fake_events

        with self.assertRaises(RateLimited) as cm:
            _ = [event async for event in provider._execute_stream(operation)]
        self.assertEqual(cm.exception.retry_after, 12.5)


class AsyncGeminiProviderStreamingTests(unittest.IsolatedAsyncioTestCase):
    async def test_execute_stream_emits_events_and_resolves_image_links(self) -> None:
        session = _create_mock_session("gemini")
        chat = AsyncChat(title="Test Chat", provider=session)
        message = AsyncMessage("Hello Gemini", chat=chat)
        operation = AsyncMessageOperation(chat, message)

        provider = AsyncGeminiProvider(provider_account("gemini", "cookie_file", "acc-test"))
        provider._connect = AsyncMock()

        mock_client = MagicMock()
        mock_client.models = ("gemini-2.5-flash",)
        mock_client.sync_activity = AsyncMock()

        async def fake_prepare(req):
            return mock_client, req, ["", "", ""], None, None

        provider._prepare_send_context = fake_prepare

        async def fake_generate(*args, **kwargs):
            del args, kwargs
            yield {
                "cid": "gemini-conv-321",
                "metadata": ["gemini-conv-321", "rc_turn_1", "rc_cid_1"],
                "thoughts": "Thinking deeply",
                "text": "Check image: http://google.com/image.png",
                "files": [
                    {
                        "alias": "http://google.com/image.png",
                        "name": "photo.png",
                        "url": "https://lh3.googleusercontent.com/blob",
                        "media_type": "image/png",
                    }
                ],
                "sources": [{"id": "g-src-1", "url": "https://google.com", "title": "Google"}],
                "response_complete": True,
            }

        with patch("webaiprovider.providers.gemini.generation.generate", fake_generate):
            events = [event async for event in provider._execute_stream(operation)]

        self.assertEqual(chat.remote_id, "gemini-conv-321")
        self.assertEqual(operation.message.id, "rc_turn_1:user")
        self.assertEqual(operation._assistant_message_id, "rc_turn_1:model")

        event_types = [type(e) for e in events]
        self.assertIn(ThinkingBegin, event_types)
        self.assertIn(ThinkingDelta, event_types)
        self.assertIn(ThinkingEnd, event_types)
        self.assertIn(OutputBegin, event_types)
        self.assertIn(OutputDelta, event_types)
        self.assertIn(SourceDiscovered, event_types)
        self.assertIn(ArtifactDiscovered, event_types)
        self.assertIn(OutputEnd, event_types)


class AsyncChatGPTProviderStreamingTests(unittest.IsolatedAsyncioTestCase):
    async def test_execute_stream_emits_events_and_updates_remote_ids(self) -> None:
        session = _create_mock_session("chatgpt")
        chat = AsyncChat(title="Test Chat", provider=session)
        message = AsyncMessage("Hello ChatGPT", chat=chat)
        operation = AsyncMessageOperation(chat, message)

        mock_client = MagicMock()

        def fake_send_prompt(cid, prompt, *, on_event=None, **kwargs):
            del cid, prompt, kwargs
            assert on_event is not None
            on_event({"type": "bound", "conversation_id": "chatgpt-conv-999"})
            on_event({"type": "delta", "text": "Reasoning steps", "channel": "analysis"})
            on_event({"type": "delta", "text": "Final answer", "channel": None})
            on_event({"type": "response_complete"})

            completed = {
                "messages": [
                    {"id": "chatgpt-u1", "author": {"role": "user"}},
                    {
                        "id": "chatgpt-a1",
                        "author": {"role": "assistant"},
                        "metadata": {"model_slug": "gpt-6"},
                    },
                ]
            }
            return "chatgpt-conv-999", completed, set()

        mock_client.send_prompt = fake_send_prompt
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        class DummyBrowserSession:
            class Paths:
                session_file = "dummy"

            paths = Paths()

            def load(self):
                pass

        provider = AsyncChatGPTProvider(
            provider_account("chatgpt", "cred_file", "acc-test"),
            session=DummyBrowserSession(),
            client_factory=lambda _: mock_client,
        )

        events = [event async for event in provider._execute_stream(operation)]

        self.assertEqual(chat.remote_id, "chatgpt-conv-999")
        self.assertEqual(operation.message.id, "chatgpt-u1")
        self.assertEqual(operation._assistant_message_id, "chatgpt-a1")

        event_types = [type(e) for e in events]
        self.assertIn(ThinkingBegin, event_types)
        self.assertIn(ThinkingDelta, event_types)
        self.assertIn(ThinkingEnd, event_types)
        self.assertIn(OutputBegin, event_types)
        self.assertIn(OutputDelta, event_types)
        self.assertIn(OutputEnd, event_types)
