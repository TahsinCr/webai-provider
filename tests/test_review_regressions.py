"""Credential and telemetry regression checks."""

import asyncio
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from webaiprovider import (
    Account,
    AsyncChat,
    AsyncMessage,
    AsyncMessageOperation,
    Attachment,
    OutputDelta,
    ProviderError,
)
from webaiprovider.core.telemetry import ProviderTelemetry
from webaiprovider.providers.claude.provider import AsyncClaudeProvider
from webaiprovider.resources.common import Origin
from webaiprovider.resources.message.operation import ThinkingEnd, ThinkingReplace
from webaiprovider.resources.message.protocol import TextDelta, TextReplacement, ThoughtDelta
from webaiprovider.resources.project.types import ProjectData


class IsolationTests(unittest.TestCase):
    def test_account_retains_updates_and_isolates_nested_values(self):
        original = {"cookies": {"session": "old"}}
        account = Account.from_cookies("gemini", original)
        store = account.store()
        self.assertIs(store, account.store())
        original["cookies"]["session"] = "outside"
        self.assertEqual(store.load()["cookies"]["session"], "old")
        store.save({"cookies": {"session": "new"}})
        loaded = store.load()
        loaded["cookies"]["session"] = "outside"
        self.assertEqual(account.store().load()["cookies"]["session"], "new")

    def test_even_sample_median(self):
        telemetry = ProviderTelemetry()
        for value in (1, 3):
            telemetry.record_ttft(value)
        self.assertEqual(telemetry.median_ttft, 2)


class StreamCompletionTests(unittest.IsolatedAsyncioTestCase):
    async def test_terminal_hook_does_not_deadlock_stream_completion(self):
        async def execute(_operation):
            yield OutputDelta(delta="answer")

        operation = AsyncMessageOperation(AsyncChat(), AsyncMessage("question"), executor=execute)
        hook_entered = asyncio.Event()
        release_hook = asyncio.Event()

        async def terminal_hook(_event):
            hook_entered.set()
            await release_hook.wait()

        operation.hooks.on("status.terminal", terminal_hook)
        consumer = asyncio.create_task(_collect(operation))
        await asyncio.wait_for(hook_entered.wait(), 1)
        await asyncio.sleep(0)
        release_hook.set()
        events = await asyncio.wait_for(consumer, 1)
        self.assertEqual([event.delta for event in events], ["answer"])

    async def test_nonpositive_stream_batch_limit_is_rejected(self):
        operation = AsyncMessageOperation(AsyncChat(), AsyncMessage("question"))
        with self.assertRaisesRegex(ValueError, "limit must be positive"):
            await operation._next_stream_batch(0, 0)


class ProviderLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def provider(self):
        provider = AsyncClaudeProvider(Account.from_cookies("claude", {}))
        provider.prepare = AsyncMock()
        provider._close_provider = AsyncMock()
        return provider

    async def test_concurrent_open_and_close_share_one_lifecycle(self):
        provider = self.provider()
        started, release = asyncio.Event(), asyncio.Event()
        calls = 0

        async def prepare():
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()

        provider.prepare = prepare
        first = asyncio.create_task(provider.open())
        await started.wait()
        second = asyncio.create_task(provider.open())
        release.set()
        await asyncio.gather(first, second)
        self.assertEqual(calls, 1)

        await asyncio.gather(provider.close(), provider.close())
        provider._close_provider.assert_awaited_once_with()
        with self.assertRaises(ProviderError):
            await provider.open()

    async def test_close_finishes_active_stream_even_when_waiter_is_cancelled(self):
        provider = self.provider()
        started, release = asyncio.Event(), asyncio.Event()

        async def stream(_request):
            started.set()
            await release.wait()
            yield TextDelta("answer")

        provider._stream_request = stream
        operation = SimpleNamespace(chat=AsyncChat())
        with patch("webaiprovider.async_provider._prepare_operation", new=AsyncMock()):
            consumer = asyncio.create_task(_collect_provider_stream(provider, operation))
            await started.wait()
            closing = asyncio.create_task(provider.close())
            await asyncio.sleep(0)
            self.assertFalse(closing.done())
            closing.cancel()
            await asyncio.sleep(0)
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await closing
            await consumer

        provider._close_provider.assert_awaited_once_with()
        with self.assertRaises(ProviderError):
            await provider.open()

    async def test_concurrent_ask_calls_keep_one_open_session_until_both_finish(self):
        provider = self.provider()
        started = {prompt: asyncio.Event() for prompt in ("first", "second")}
        release = {prompt: asyncio.Event() for prompt in ("first", "second")}

        class Operation:
            def __init__(self, prompt):
                self.prompt = prompt

            async def start(self):
                started[self.prompt].set()
                await release[self.prompt].wait()
                return self.prompt

        async def create_chat():
            return SimpleNamespace(
                messages=SimpleNamespace(create=lambda prompt, **_options: Operation(prompt))
            )

        provider.chats.create = create_chat
        first = asyncio.create_task(provider.ask("first"))
        await started["first"].wait()
        second = asyncio.create_task(provider.ask("second"))
        await asyncio.wait_for(started["second"].wait(), 1)
        release["first"].set()
        await asyncio.sleep(0)
        self.assertFalse(provider._closed)
        release["second"].set()
        self.assertEqual(await asyncio.gather(first, second), ["first", "second"])
        provider._close_provider.assert_awaited_once_with()

    async def test_sends_serialize_per_chat_without_blocking_other_chats(self):
        provider = self.provider()
        first_started, second_started, other_started = (
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
        )
        release_first = asyncio.Event()

        async def stream(request):
            if request.marker == "first":
                first_started.set()
                await release_first.wait()
            elif request.marker == "second":
                second_started.set()
            else:
                other_started.set()
            yield TextDelta("answer")

        provider._stream_request = stream
        shared_chat, other_chat = AsyncChat(), AsyncChat()
        requests = [
            SimpleNamespace(chat=shared_chat, marker="first"),
            SimpleNamespace(chat=shared_chat, marker="second"),
            SimpleNamespace(chat=other_chat, marker="other"),
        ]
        with patch(
            "webaiprovider.async_provider._prepare_operation",
            new=AsyncMock(side_effect=lambda operation, _provider: operation),
        ):
            tasks = [asyncio.create_task(_collect_provider_stream(provider, requests[0]))]
            await first_started.wait()
            tasks.extend(
                asyncio.create_task(_collect_provider_stream(provider, request))
                for request in requests[1:]
            )
            await asyncio.wait_for(other_started.wait(), 1)
            self.assertFalse(second_started.is_set())
            release_first.set()
            await asyncio.gather(*tasks)

        self.assertTrue(second_started.is_set())
        await provider.close()


class ThinkingReplacementTests(unittest.IsolatedAsyncioTestCase):
    async def test_analysis_replacement_and_new_segment_reset_have_replace_semantics(self):
        provider = AsyncClaudeProvider(Account.from_cookies("claude", {}))
        provider._close_provider = AsyncMock()

        async def stream(_request):
            yield ThoughtDelta("old thought")
            yield TextReplacement("replacement", channel="analysis")
            yield TextDelta("answer")
            yield ThoughtDelta("new segment")

        provider._stream_request = stream
        operation = SimpleNamespace(chat=AsyncChat())
        with patch(
            "webaiprovider.async_provider._prepare_operation", new=AsyncMock(return_value=None)
        ):
            events = await _collect_provider_stream(provider, operation)

        self.assertEqual(
            [event.text for event in events if isinstance(event, ThinkingReplace)],
            ["replacement"],
        )
        self.assertEqual(
            [event.text for event in events if isinstance(event, ThinkingEnd)],
            ["replacement", "new segment"],
        )
        await provider.close()


class ResourceOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def test_message_and_resource_mutations_keep_session_ownership(self):
        first = AsyncClaudeProvider(Account.from_cookies("claude", {}))
        second = AsyncClaudeProvider(Account.from_cookies("claude", {}))
        first._close_provider = AsyncMock()
        second._close_provider = AsyncMock()
        try:
            chat = await first.chats.create()
            foreign_chat = await second.chats.create()
            attachment = Attachment.from_bytes(b"content", "note.txt")
            original = chat.messages.create("question", attachments=[attachment]).message

            with self.assertRaisesRegex(ValueError, "different chat"):
                foreign_chat.messages.create(original)
            with self.assertRaisesRegex(ValueError, "already in this chat"):
                chat.messages.create(original)

            edited = chat.messages.edit(original.id, "edited").message
            retried = chat.messages.retry(original.id).message
            self.assertIsNot(edited.attachments, original.attachments)
            self.assertIsNot(retried.attachments, original.attachments)
            self.assertIsNot(next(iter(edited.attachments)), next(iter(original.attachments)))
            self.assertIsNot(next(iter(retried.attachments)), next(iter(original.attachments)))

            with self.assertRaisesRegex(ValueError, "different provider session"):
                await first.projects.update(second._project_type(title="foreign", session=second))
            with self.assertRaisesRegex(ValueError, "different provider session"):
                await first.projects.delete(second._project_type(title="foreign", session=second))
            with self.assertRaisesRegex(ValueError, "different provider session"):
                await first.chats.delete(foreign_chat)

            await chat.delete()
            self.assertNotIn(chat, first.chats._items)
        finally:
            await first.close()
            await second.close()

    async def test_parallel_project_field_updates_preserve_each_others_changes(self):
        provider = AsyncClaudeProvider(Account.from_cookies("claude", {}))
        provider._gate = asyncio.Lock()
        provider.prepare = AsyncMock()
        provider._close_provider = AsyncMock()
        await provider.open()
        remote = ProjectData(
            Origin("claude", provider.account_id, "project-1"),
            "Initial",
            "Original instructions",
        )
        project = provider._project_type(
            title=remote.name,
            remote_id=remote.origin.remote_id,
            session=provider,
        )
        project._apply_provider_project(remote)
        write_started, release_write = asyncio.Event(), asyncio.Event()
        writes = []

        async def write(_action, _project_id, *, current, **changes):
            nonlocal remote
            writes.append((current.name, current.instructions, dict(changes)))
            if changes.get("name") == "Renamed":
                write_started.set()
                await release_write.wait()
            remote = replace(current, **changes)
            return remote

        provider.editor = SimpleNamespace(write=AsyncMock(side_effect=write))
        try:
            rename = asyncio.create_task(provider.projects.update(project, title="Renamed"))
            await write_started.wait()
            instructions = asyncio.create_task(
                provider.projects.update(project, instruction="New instructions")
            )
            await asyncio.sleep(0)
            self.assertFalse(instructions.done())
            release_write.set()
            await asyncio.gather(rename, instructions)
            self.assertEqual(remote.name, "Renamed")
            self.assertEqual(remote.instructions, "New instructions")
            self.assertEqual(
                writes,
                [
                    ("Initial", "Original instructions", {"name": "Renamed"}),
                    ("Renamed", "Original instructions", {"instructions": "New instructions"}),
                ],
            )
        finally:
            await provider.close()


async def _collect(operation):
    return [event async for event in operation.stream()]


async def _collect_provider_stream(provider, operation):
    return [event async for event in provider.execute_stream(operation)]


class ReportTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_stream_limit_survives_partial_reads(self):
        from webaiprovider.transport.base import _AsyncStreamResponse, _StreamResponse

        response = SimpleNamespace(status_code=200, headers={})
        chunks = iter([b"abc", b"def"])
        response.iter_bytes = lambda: chunks
        sync = _StreamResponse(response, limit=5)
        reader = sync.iter_bytes()
        self.assertEqual(next(reader), b"abc")
        with self.assertRaisesRegex(RuntimeError, "Concurrent"):
            next(sync.iter_bytes())
        reader.close()
        with self.assertRaisesRegex(ProviderError, "size limit"):
            next(sync.iter_bytes())

        async def produce():
            yield b"abc"
            yield b"def"

        stream = _AsyncStreamResponse(response, produce(), limit=5)
        reader = stream.aiter_bytes()
        self.assertEqual(await anext(reader), b"abc")
        with self.assertRaisesRegex(RuntimeError, "Concurrent"):
            await anext(stream.aiter_bytes())
        await reader.aclose()
        with self.assertRaisesRegex(ProviderError, "size limit"):
            await anext(stream.aiter_bytes())

    def test_byte_limits_require_positive_integers(self):
        from webaiprovider.transport.base import TransportConfig

        for field in ("max_response_bytes", "max_stream_bytes"):
            for value in (True, False, 1.5, float("inf"), float("nan"), "10", 0, -1):
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    TransportConfig(**{field: value})

    async def test_hook_failure_is_sanitized_and_does_not_stop_later_hooks(self):
        from webaiprovider.resources.message.operation import OperationHooks

        hooks = OperationHooks()
        calls = []

        def fail(_event):
            raise ValueError("secret-token")

        hooks.on("*", fail)
        hooks.on("*", lambda event: calls.append(event))
        with self.assertLogs("webaiprovider.resources.message.operation", level="WARNING") as logs:
            await hooks.emit(OutputDelta(delta="private-message"))
        self.assertEqual(len(calls), 1)
        self.assertIn("ValueError", logs.output[0])
        self.assertNotIn("secret-token", logs.output[0])
        self.assertNotIn("private-message", logs.output[0])


class RefreshIdentityTests(unittest.IsolatedAsyncioTestCase):
    async def test_refresh_keeps_resources_and_unsent_messages(self):
        from webaiprovider.collections import Page
        from webaiprovider.resources.chat.types import Conversation
        from webaiprovider.resources.message.types import Content, MessageData

        provider = AsyncClaudeProvider(Account.from_cookies("claude", {}))
        provider.prepare = AsyncMock()
        provider._close_provider = AsyncMock()
        origin = Origin("claude", provider.account_id, "chat")
        remote = Conversation(
            origin, "Before", (MessageData("message", "user", (Content("text", text="old"),)),)
        )
        provider.list_chats = AsyncMock(return_value=Page((remote,)))
        provider.get_chat = AsyncMock(return_value=remote)
        project = ProjectData(Origin("claude", provider.account_id, "project"), "Before")
        provider.list_projects = AsyncMock(return_value=Page((project,)))
        async with provider:
            chat = (await provider.chats.refresh())[0]
            message = (await chat.messages.refresh())[0]
            pending = chat.messages.create("pending").message
            provider.list_chats.return_value = Page((replace(remote, title="After"),))
            self.assertIs((await provider.chats.refresh())[0], chat)
            self.assertEqual(chat.title, "After")
            provider.get_chat.return_value = replace(
                remote,
                messages=(replace(remote.messages[0], content=(Content("text", text="new"),)),),
            )
            messages = await chat.messages.refresh()
            self.assertIs(messages[0], message)
            self.assertEqual(message.content, "new")
            self.assertIn(pending, messages)
            first = (await provider.projects.refresh())[0]
            provider.list_projects.return_value = Page((replace(project, name="After"),))
            self.assertIs((await provider.projects.refresh())[0], first)
            self.assertEqual(first.title, "After")


class CacheSizeTests(unittest.TestCase):
    def test_nested_payload_is_counted_once_and_cycles_terminate(self):
        from webaiprovider.core.cache import _default_size_calculator

        shared = "a" * 10000
        value = {"messages": [shared, shared]}
        self.assertGreater(_default_size_calculator("key", value), 10000)
        self.assertLess(_default_size_calculator("key", value), 20000)
        value["self"] = value
        self.assertGreater(_default_size_calculator("key", value), 10000)


class CancellationOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def test_cross_thread_cancel_changes_state_on_owner_loop(self):
        started = asyncio.Event()

        async def execute(_operation):
            started.set()
            await asyncio.Event().wait()
            yield OutputDelta(delta="unused")

        operation = AsyncMessageOperation(AsyncChat(), AsyncMessage("question"), executor=execute)
        task = asyncio.create_task(operation.start())
        await started.wait()
        await asyncio.to_thread(operation.cancel)
        from webaiprovider.errors import OperationCancelledError

        with self.assertRaises(OperationCancelledError):
            await asyncio.wait_for(task, 1)
        self.assertEqual(operation.state.value, "CANCELLED")

    async def test_repeated_upload_cancellation_waits_for_blocking_close(self):
        import threading

        from webaiprovider.providers.chatgpt.provider import AsyncChatGPTProvider

        started = threading.Event()
        closing = threading.Event()
        release = threading.Event()

        class Client:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                pass

            def upload_file(self, *_args, cancel_event):
                started.set()
                release.wait(3)
                return None

            def close(self):
                closing.set()
                release.wait(3)

        provider = AsyncChatGPTProvider(
            Account.from_cookies("chatgpt", {}), client_factory=lambda _session: Client()
        )
        task = asyncio.create_task(provider.upload_file("fixture.txt"))
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
            task.cancel()
            self.assertTrue(await asyncio.to_thread(closing.wait, 1))
            task.cancel()
            await asyncio.sleep(0)
            self.assertFalse(task.done())
        finally:
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)


class NovemberAuditTests(unittest.IsolatedAsyncioTestCase):
    async def test_grok_real_open_is_idempotent_and_does_not_reenter_activity(self):
        from webaiprovider.providers.grok import AsyncGrokProvider

        provider = AsyncGrokProvider(Account.from_cookies("grok", {}))
        provider.client.authenticate = AsyncMock()
        provider.client.close = AsyncMock()
        provider._load_model_catalog = AsyncMock()
        try:
            await asyncio.wait_for(provider.open(), 1)
            await asyncio.wait_for(provider.open(), 1)
            provider.client.authenticate.assert_awaited_once_with()
            provider._load_model_catalog.assert_awaited_once_with()
        finally:
            await asyncio.wait_for(provider.close(), 1)
        provider.client.close.assert_awaited_once_with()

    async def test_claude_forget_waits_for_activity_and_deletes_credentials(self):
        provider = AsyncClaudeProvider(Account.from_cookies("claude", {"sessionKey": "fixture"}))
        provider.client.prepare = AsyncMock()
        provider.client.close = AsyncMock()
        provider.editor.close = AsyncMock()
        entered, release = asyncio.Event(), asyncio.Event()

        async def active():
            async with provider._activity():
                entered.set()
                await release.wait()

        await provider.open()
        task = asyncio.create_task(active())
        await entered.wait()
        forgetting = asyncio.create_task(provider.forget_session())
        await asyncio.sleep(0)
        self.assertFalse(forgetting.done())
        release.set()
        await task
        await asyncio.wait_for(asyncio.gather(forgetting, provider.forget_session()), 1)
        from webaiprovider.accounts import session_exists

        self.assertFalse(session_exists(provider.credential_file))
        self.assertTrue(provider._closed)
        provider.client.close.assert_awaited_once_with()
        with self.assertRaises(ProviderError):
            await provider.authenticate()

    async def test_refresh_preserves_unsent_chat_in_both_listing_modes(self):
        from webaiprovider.collections import Page

        provider = AsyncClaudeProvider(Account.from_cookies("claude", {}))
        provider.prepare = AsyncMock()
        provider._close_provider = AsyncMock()
        provider.list_chats = AsyncMock(return_value=Page(()))
        provider.list_projects = AsyncMock(return_value=Page(()))
        async with provider:
            chat = await provider.chats.create("Pending")
            for include_projects in (False, True):
                self.assertIn(chat, await provider.chats.refresh(include_projects=include_projects))

    async def test_stream_completion_reuses_assistant_loaded_during_send(self):
        entered, release = asyncio.Event(), asyncio.Event()
        chat = AsyncChat()

        async def execute(operation):
            operation._assistant_message_id = "assistant-1"
            yield OutputDelta(delta="answer")
            entered.set()
            await release.wait()

        operation = AsyncMessageOperation(chat, AsyncMessage("question"), executor=execute)
        task = asyncio.create_task(operation.start())
        await entered.wait()
        loaded = AsyncMessage("partial", role="assistant", id="assistant-1", chat=chat)
        loaded.remote_id = "assistant-1"
        chat.messages._items.append(loaded)
        release.set()
        response = await task
        self.assertIs(response.message, loaded)
        self.assertEqual(loaded.content, "answer")
        self.assertEqual(len(chat.messages._items), 1)

    async def test_iterator_cleanup_preserves_primary_failure_and_chains_cleanup(self):
        from webaiprovider.resources.message.operation import _close_async_iterator

        primary = ProviderError("stream failed")
        cleanup = RuntimeError("cleanup failed")
        stream = SimpleNamespace(aclose=AsyncMock(side_effect=cleanup))
        try:
            raise primary
        except ProviderError:
            with self.assertRaises(ProviderError) as caught:
                await _close_async_iterator(stream)
        self.assertIs(caught.exception, primary)
        self.assertIs(caught.exception.__cause__, cleanup)
        with self.assertRaises(RuntimeError):
            await _close_async_iterator(stream)

    def test_generic_tuple_size_does_not_trust_third_member(self):
        from webaiprovider.core.cache import _default_size_calculator

        self.assertGreater(
            _default_size_calculator("key", ("record", {"text": "x" * 100000}, 1)), 100000
        )
