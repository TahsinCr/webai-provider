"""Portable API tests: no application modules, private profiles or network access."""

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from webai_provider import (
    Capability,
    ConversationBound,
    ProviderClient,
    ReplaceText,
    SendRequest,
    create_provider,
)
from webai_provider.providers.gemini.adapter import GeminiProvider
from webai_provider.providers.gemini.errors import GeminiStreamError
from webai_provider.registry import PROVIDERS


class FakeAdapter:
    id = "claude"
    capabilities = frozenset({Capability.CHAT})

    def __init__(self, account_id, *, wait=False):
        self.account_id, self.wait = account_id, wait
        self.started, self.stopped = asyncio.Event(), asyncio.Event()
        self.closed = 0
        self.prepared = 0
        self.requests = []

    async def prepare(self):
        self.prepared += 1

    async def stream(self, request):
        self.requests.append(request)
        self.started.set()
        try:
            if self.wait:
                await asyncio.Event().wait()
            yield ConversationBound("remote-chat")
            yield ReplaceText(self.account_id, "final")
        finally:
            self.stopped.set()

    async def close(self):
        self.closed += 1


class PublicAPITests(unittest.IsolatedAsyncioTestCase):
    async def test_gemini_total_stream_deadline_uses_supported_asyncio_api(self):
        provider = GeminiProvider(Path("unused"), "test")
        provider.stream_total_timeout = 0.005
        stopped = asyncio.Event()

        async def stalled_response(_request):
            try:
                await asyncio.Event().wait()
                yield ReplaceText("late")
            finally:
                stopped.set()

        provider._stream = stalled_response
        with self.assertRaisesRegex(GeminiStreamError, "10 dakikalık süre sınırını"):
            _ = [
                event
                async for event in provider.stream(SendRequest("gemini", "test", "local", "Hello"))
            ]
        self.assertTrue(stopped.is_set())

    async def test_gemini_stream_preserves_caller_cancellation(self):
        provider = GeminiProvider(Path("unused"), "test")
        provider.stream_total_timeout = 30
        started = asyncio.Event()
        stopped = asyncio.Event()

        async def stalled_response(_request):
            started.set()
            try:
                await asyncio.Event().wait()
                yield ReplaceText("late")
            finally:
                stopped.set()

        provider._stream = stalled_response

        async def consume():
            return [
                event
                async for event in provider.stream(SendRequest("gemini", "test", "local", "Hello"))
            ]

        task = asyncio.create_task(consume())
        await started.wait()
        task.cancel("caller stopped")
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(stopped.is_set())

    async def test_routes_both_account_identity_and_conversation_project_context(self):
        async with ProviderClient() as client:
            first = client.add(FakeAdapter("first"))
            second = client.add(FakeAdapter("second"))
            request = SendRequest(
                "claude",
                "second",
                "local",
                "Hello",
                conversation_id="remote-chat",
                project_id="remote-project",
            )
            events = [event async for event in client.stream(request)]
            self.assertEqual(events[-1], ReplaceText("second", "final"))
            self.assertEqual(second.requests, [request])
            self.assertEqual(first.requests, [])
            self.assertIs(client.account("claude", "second"), second)
        self.assertEqual((first.closed, second.closed), (1, 1))
        await client.close()
        self.assertEqual(second.closed, 1)

    async def test_explicit_prepare_warms_only_the_requested_account(self):
        client = ProviderClient()
        adapter = client.add(FakeAdapter("personal"))
        send_preparations = 0

        async def prepare_for_send():
            nonlocal send_preparations
            send_preparations += 1

        adapter.prepare_for_send = prepare_for_send
        prepared = await client.prepare("claude", "personal")
        self.assertIs(prepared, adapter)
        self.assertEqual(adapter.prepared, 1)
        self.assertEqual(send_preparations, 0)

        await client.prepare("claude", "personal", for_send=True)
        self.assertEqual(adapter.prepared, 2)
        self.assertEqual(send_preparations, 1)
        await client.close()

    async def test_close_cancels_stream_before_closing_backend(self):
        client = ProviderClient()
        adapter = client.add(FakeAdapter("default", wait=True))

        async def consume():
            return [
                event
                async for event in client.stream(SendRequest("claude", "default", "local", "Hello"))
            ]

        task = asyncio.create_task(consume())
        await adapter.started.wait()
        await asyncio.wait_for(client.close(), 2)
        self.assertTrue(task.cancelled())
        self.assertTrue(adapter.stopped.is_set())
        self.assertEqual(adapter.closed, 1)
        self.assertFalse(client._streams)
        with self.assertRaises(RuntimeError):
            client.account("claude")

    async def test_concurrent_close_retries_only_backend_that_failed_to_close(self):
        client = ProviderClient()
        flaky = FakeAdapter("first")
        flaky.id = "flaky"
        healthy = FakeAdapter("second")
        healthy.id = "healthy"
        flaky_attempts = 0

        async def fail_once():
            nonlocal flaky_attempts
            flaky_attempts += 1
            if flaky_attempts == 1:
                raise RuntimeError("transient close failure")
            flaky.closed += 1

        flaky.close = fail_once
        client.add(flaky)
        client.add(healthy)
        results = await asyncio.gather(client.close(), client.close(), return_exceptions=True)
        self.assertTrue(all(isinstance(result, RuntimeError) for result in results))
        self.assertEqual(flaky_attempts, 1)
        self.assertEqual(healthy.closed, 1)
        self.assertEqual(client.accounts, (("flaky", "first"),))
        with self.assertRaises(RuntimeError):
            client.account("flaky", "first")

        await client.close()
        self.assertEqual(flaky_attempts, 2)
        self.assertEqual(flaky.closed, 1)
        self.assertEqual(healthy.closed, 1)
        self.assertEqual(client.accounts, ())

    async def test_early_stop_in_caller_task_can_close_suspended_iterator(self):
        client = ProviderClient()
        adapter = client.add(FakeAdapter("default"))
        stream = client.stream(SendRequest("claude", "default", "local", "Hello"))
        await anext(stream)
        await client.close()
        await stream.aclose()
        self.assertTrue(adapter.stopped.is_set())

    async def test_duplicate_identity_is_rejected_before_constructing_an_account(self):
        async with ProviderClient() as client:
            client.add(FakeAdapter("default"))
            with patch("webai_provider.client.create_provider") as factory:
                with self.assertRaises(ValueError):
                    client.connect("claude")
                factory.assert_not_called()

    async def test_all_bundled_factories_are_lazy_and_reject_wrong_account_send(self):
        with tempfile.TemporaryDirectory() as folder:
            async with ProviderClient() as client:
                for provider in PROVIDERS:
                    backend = client.add(
                        create_provider(
                            provider,
                            "personal",
                            credential_file=Path(folder) / provider / "session.json",
                        )
                    )
                    self.assertEqual(backend.id, provider)
                    with self.assertRaises(ValueError):
                        await anext(
                            backend.stream(SendRequest(provider, "other", "local", "Hello"))
                        )
            self.assertEqual(list(Path(folder).rglob("session.json")), [])

    def test_package_exports_version_symbols(self):
        import webai_provider

        self.assertEqual(webai_provider.__version__, "1.3.0b0")
        self.assertIn("__version__", webai_provider.__all__)
        self.assertNotIn("__verison__", webai_provider.__all__)
        self.assertFalse(hasattr(webai_provider, "__verison__"))

    def test_redact_sensitive_url(self):
        from webai_provider.api import redact_sensitive_url

        url = "https://user:pass@api.openai.com/v1/files?token=secret123&session_id=abc&format=json"
        redacted = redact_sensitive_url(url)
        self.assertNotIn("secret123", redacted)
        self.assertNotIn("pass", redacted)
        self.assertIn("format=json", redacted)
        self.assertIn("[REDACTED]", redacted)

    def test_validate_network_address_and_ssrf_block(self):
        from webai_provider.api import NativeDownloadClient, validate_network_address

        for blocked in (
            "127.0.0.1",
            "169.254.169.254",
            "10.0.0.1",
            "192.168.1.1",
            "::1",
            "0.0.0.0",
        ):
            with self.assertRaises(ValueError):
                validate_network_address(blocked, allow_private=False)

        with self.assertRaises(ValueError):
            validate_network_address("169.254.169.254")

        validate_network_address("93.184.216.34")
        validate_network_address("example.com")

        client = NativeDownloadClient()
        with self.assertRaises(ValueError):
            with client.stream("GET", "http://169.254.169.254/latest/meta-data/", headers={}):
                pass
