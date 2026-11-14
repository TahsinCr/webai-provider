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
