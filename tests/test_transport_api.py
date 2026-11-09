"""Public transport extension, stream lifetime and account boundary contracts."""

import asyncio
import threading
import unittest
from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx

from webaiprovider import Account, AsyncProvider, Provider, ProviderType
from webaiprovider.errors import ProviderError, RateLimited
from webaiprovider.providers.chatgpt import AsyncChatGPTProvider
from webaiprovider.providers.chatgpt.client import ChatGPTClient
from webaiprovider.providers.claude import AsyncClaudeProvider
from webaiprovider.providers.gemini import AsyncGeminiProvider
from webaiprovider.providers.grok import AsyncGrokProvider
from webaiprovider.resources.message.protocol import (
    ProviderEvent,
    ProviderRequest,
    TextDelta,
)
from webaiprovider.transport import (
    AsyncHTTPTransport,
    AsyncHTTPXTransport,
    HTTPTransport,
    ThreadedTransport,
    TransportConfig,
)


class AsyncTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_custom_implementation_uses_public_events_and_the_shared_operation_pipeline(self):
        class CustomProvider(AsyncProvider):
            provider_type = ProviderType.GROK

            async def _stream_request(self, request: ProviderRequest):
                payload = await self.transport.request(
                    "POST", "/fixture", json={"text": request.prompt}
                )
                event: ProviderEvent = TextDelta(payload["answer"])
                yield event

        client = SimpleNamespace(
            request=AsyncMock(return_value=httpx.Response(200, json={"answer": "reply"})),
            close=AsyncMock(),
        )
        transport = AsyncHTTPTransport("https://grok.com", client_factory=lambda **_: client)
        async with CustomProvider(
            Account.from_cookies("grok", {}), transport=transport
        ) as provider:
            chat = await provider.chats.create()
            operation = chat.messages.create("question")
            response = await operation.wait()
            self.assertEqual(response.text, "reply")
        client.request.assert_awaited_once()
        self.assertEqual(client.request.call_args.kwargs["json"], {"text": "question"})
        client.close.assert_awaited_once()

    async def test_httpx_strategy_uses_the_same_request_and_stream_contract(self):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"ok": True}))
        ) as client:
            transport = AsyncHTTPXTransport("https://claude.ai", client_getter=lambda: client)
            self.assertEqual(await transport.request("GET", "/fixture"), {"ok": True})
            async with transport.stream("GET", "/fixture") as response:
                content = b"".join([chunk async for chunk in response.aiter_bytes()])
            self.assertEqual(content, b'{"ok":true}')

    def transport(self, origin="https://claude.ai", **options):
        client = SimpleNamespace(
            request=AsyncMock(return_value=httpx.Response(200, json={"ok": True})),
            close=AsyncMock(),
        )
        return AsyncHTTPTransport(origin, client_getter=lambda: client, **options), client

    async def test_guarded_requests_and_config_are_shared_without_mutation_retries(self):
        transport, client = self.transport(
            config=TransportConfig(timeout=7), headers={"authorization": "fixture"}
        )
        for path in ("https://other.test/", "//other.test/", "/%2e%2e/private", "/x\\y", "/x\n"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                await transport.send("POST", path)
        for options in (
            {"follow_redirects": True},
            {"headers": {"Host": "other.test"}},
            {"headers": {"Host": "claude.ai", "host": "other.test"}},
            {"stream": True},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                await transport.send("POST", "/", **options)
        client.request.assert_not_awaited()
        self.assertEqual(await transport.request("POST", "/api/test"), {"ok": True})
        call = client.request.call_args
        self.assertEqual(call.kwargs["timeout"], 7)
        self.assertEqual(call.kwargs["headers"]["authorization"], "fixture")
        self.assertFalse(call.kwargs["allow_redirects"])
        client.request.return_value = httpx.Response(503)
        with self.assertRaises(ProviderError):
            await transport.send("POST", "/api/test")
        self.assertEqual(client.request.await_count, 2)

    async def test_origin_cannot_be_reassigned_after_binding_credentials(self):
        transport, _ = self.transport()
        with self.assertRaises(AttributeError):
            transport.origin = "https://other.test"

    async def test_rate_limits_are_isolated_between_accounts_on_the_same_origin(self):
        first, first_client = self.transport()
        second, _ = self.transport()
        first_client.request.return_value = httpx.Response(429, headers={"retry-after": "60"})
        for _ in range(2):
            with self.assertRaises(RateLimited):
                await first.request("GET", "/api/test")
        first_client.request.assert_awaited_once()
        self.assertEqual(await second.request("GET", "/api/test"), {"ok": True})

    async def test_cancelled_and_oversized_streams_close_the_response(self):
        for oversized in (False, True):
            with self.subTest(oversized=oversized):
                released = asyncio.Event()

                async def chunks():
                    yield b"first"
                    if oversized:
                        yield b"too large"
                    else:
                        await asyncio.Event().wait()

                @asynccontextmanager
                async def stream(*args, **options):
                    try:
                        yield SimpleNamespace(status_code=200, headers={}, aiter_content=chunks)
                    finally:
                        released.set()

                transport = AsyncHTTPTransport(
                    "https://claude.ai",
                    client_getter=lambda: SimpleNamespace(stream=stream),
                    config=TransportConfig(max_stream_bytes=8),
                )
                entered = asyncio.Event()

                async def consume():
                    async with transport.stream("POST", "/stream") as response:
                        async for chunk in response.aiter_bytes():
                            self.assertEqual(chunk, b"first")
                            entered.set()

                task = asyncio.create_task(consume())
                await asyncio.wait_for(entered.wait(), 1)
                if not oversized:
                    task.cancel()
                with self.assertRaises(ProviderError if oversized else asyncio.CancelledError):
                    await asyncio.wait_for(task, 1)
                self.assertTrue(released.is_set())

    async def test_provider_specific_facades_expose_the_same_injected_transport(self):
        for provider_type, origin, cls in (
            ("chatgpt", "https://chatgpt.com", AsyncChatGPTProvider),
            ("claude", "https://claude.ai", AsyncClaudeProvider),
            ("gemini", "https://gemini.google.com", AsyncGeminiProvider),
            ("grok", "https://grok.com", AsyncGrokProvider),
        ):
            with self.subTest(provider=provider_type):
                transport, client = self.transport(origin)
                provider = cls(Account.from_cookies(provider_type, {}), transport=transport)
                provider.prepare = AsyncMock()
                provider._close_provider = AsyncMock()
                async with provider:
                    self.assertIs(provider.transport, transport)
                    await provider.transport.request("GET", "/fixture")
                client.request.assert_awaited_once()

    async def test_provider_transport_injection_rejects_a_foreign_origin(self):
        transport, client = self.transport("https://other.test")
        for provider_id, cls in (
            ("chatgpt", AsyncChatGPTProvider),
            ("claude", AsyncClaudeProvider),
            ("gemini", AsyncGeminiProvider),
            ("grok", AsyncGrokProvider),
        ):
            with self.subTest(provider=provider_id), self.assertRaises(ValueError):
                cls(Account.from_cookies(provider_id, {}), transport=transport)
        client.request.assert_not_awaited()

    async def test_threaded_stream_is_incremental_and_early_exit_joins_worker(self):
        closed = threading.Event()
        read = 0

        class Bytes(httpx.SyncByteStream):
            def __iter__(self):
                nonlocal read
                while not closed.is_set():
                    read += 1
                    yield b"part"

            def close(self):
                closed.set()

        client = httpx.Client(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=Bytes()))
        )
        self.addCleanup(client.close)
        native = HTTPTransport("https://chatgpt.com", client_getter=lambda: client)

        @contextmanager
        def scope():
            yield native

        transport = ThreadedTransport("https://chatgpt.com", scope)
        async with transport.stream("GET", "/stream") as response:
            iterator = response.aiter_bytes()
            self.assertEqual(await anext(iterator), b"part")
        self.assertTrue(closed.is_set())
        self.assertLess(read, 40)

    async def test_threaded_cancellation_closes_a_blocking_read_and_joins_it(self):
        entered, released, finished = threading.Event(), threading.Event(), threading.Event()

        class Bytes(httpx.SyncByteStream):
            def __iter__(self):
                try:
                    entered.set()
                    if not released.wait(2):
                        raise TimeoutError("Stream did not close on cancellation")
                    yield b"late"
                finally:
                    finished.set()

            def close(self):
                released.set()

        with httpx.Client(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=Bytes()))
        ) as client:

            @contextmanager
            def scope():
                yield HTTPTransport("https://chatgpt.com", client_getter=lambda: client)

            async def consume():
                async with ThreadedTransport("https://chatgpt.com", scope).stream(
                    "GET", "/"
                ) as response:
                    return [chunk async for chunk in response.aiter_bytes()]

            task = asyncio.create_task(consume())
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
        self.assertTrue(released.is_set())
        self.assertTrue(finished.is_set())


class BlockingTransportTests(unittest.TestCase):
    def test_chatgpt_native_and_public_transport_share_headers_without_refreshing_with_old_token(
        self,
    ):
        requests = []
        native = ChatGPTClient(SimpleNamespace(cdp_url="unused", save=Mock()), authenticate=False)
        self.addCleanup(native.close)
        native.http.close()
        native.http = httpx.Client(
            transport=httpx.MockTransport(
                lambda request: (
                    requests.append(request) or httpx.Response(200, json={"accessToken": "new"})
                )
            )
        )
        native.headers = {"authorization": "Bearer old"}
        native.refresh_session()
        native.transport.send("GET", "/backend-api/models")
        self.assertNotIn("authorization", requests[0].headers)
        self.assertEqual(requests[1].headers["authorization"], "Bearer new")

    def test_sync_facade_uses_one_portal_for_raw_requests_and_stream_cleanup(self):
        threads = set()
        closed = threading.Event()
        exits = []

        async def request(*args, **options):
            threads.add(threading.get_ident())
            return httpx.Response(200, json={"ok": True})

        async def chunks():
            threads.add(threading.get_ident())
            yield b"stream"

        @asynccontextmanager
        async def stream(*args, **options):
            try:
                yield SimpleNamespace(status_code=200, headers={}, aiter_content=chunks)
            finally:
                exits.append(True)
                closed.set()

        transport = AsyncHTTPTransport(
            "https://claude.ai",
            client_getter=lambda: SimpleNamespace(
                request=request,
                stream=stream,
            ),
        )
        account = Account.from_cookies("claude", {})

        class CommonAsyncProvider(AsyncProvider):
            pass

        class CommonProvider(Provider):
            async_provider_type = CommonAsyncProvider

        with CommonProvider(account, transport=transport) as provider:
            self.assertEqual(provider.transport.request("GET", "/fixture"), {"ok": True})
            with provider.transport.stream("GET", "/stream") as response:
                self.assertEqual(list(response.iter_bytes()), [b"stream"])
                response.close()
            with self.assertRaises(ValueError):
                with provider.transport.stream("GET", "/stream"):
                    raise ValueError("caller failure")
        self.assertEqual(len(threads), 1)
        self.assertNotIn(threading.get_ident(), threads)
        self.assertTrue(closed.is_set())
        self.assertEqual(len(exits), 2)

    def test_sync_http_request_checks_response_limits_and_disables_redirects(self):
        requests = []
        with httpx.Client(
            transport=httpx.MockTransport(
                lambda request: (
                    requests.append(request) or httpx.Response(200, content=b"large body")
                )
            )
        ) as client:
            transport = HTTPTransport(
                "https://claude.ai",
                client_getter=lambda: client,
                config=TransportConfig(max_response_bytes=3),
            )
            with self.assertRaisesRegex(ProviderError, "size limit"):
                transport.send("POST", "/fixture")
            self.assertEqual(len(requests), 1)
