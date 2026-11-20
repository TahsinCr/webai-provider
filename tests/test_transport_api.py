"""Public transport extension, stream lifetime and account boundary contracts."""

import asyncio
import socket
import tempfile
import threading
import unittest
from contextlib import asynccontextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from tests.fakes import FakeHTTPClient, FakeHTTPResponse
from webaiprovider import (
    Account,
    AsyncProvider,
    Provider,
    ProviderType,
)
from webaiprovider.core.timing import SendTiming
from webaiprovider.errors import ProviderError, RateLimited
from webaiprovider.providers.chatgpt import AsyncChatGPTProvider, BrowserSession
from webaiprovider.providers.chatgpt.client import ChatGPTClient
from webaiprovider.providers.claude import AsyncClaudeProvider
from webaiprovider.providers.gemini import AsyncGeminiProvider
from webaiprovider.providers.grok import AsyncGrokProvider
from webaiprovider.resources.message.protocol import (
    ProviderRequest,
    ProviderStreamEvent,
    TextDelta,
)
from webaiprovider.transport import (
    AsyncHTTPTransport,
    HTTPTransport,
    TransportConfig,
)
from webaiprovider.transport.async_http import _check_ipv6_connectivity, _open_curl_stream


class AsyncTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_stream_timing_reports_first_byte_and_connection_reuse(self):
        from curl_cffi import CurlInfo

        response = SimpleNamespace(
            infos={
                CurlInfo.PRETRANSFER_TIME_T: 200,
                CurlInfo.STARTTRANSFER_TIME_T: 65_200,
                CurlInfo.NUM_CONNECTS: 0,
            },
            aclose=AsyncMock(),
        )
        client = SimpleNamespace(
            curl_infos=[CurlInfo.HTTP_VERSION], request=AsyncMock(return_value=response)
        )
        timing = SendTiming()
        async with _open_curl_stream(client, "POST", "https://example.com", {}, timing=timing):
            pass
        durations, counts = timing.snapshot()
        self.assertEqual(durations["transport_response_wait_ms"], 65.0)
        self.assertEqual(durations["transport_first_byte_ms"], 65.2)
        self.assertEqual(counts, {"transport_connection_reused": 1})
        self.assertIn(CurlInfo.HTTP_VERSION, client.curl_infos)
        response.aclose.assert_awaited_once()

    async def test_cancelled_request_releases_owned_client_reference(self):
        client = SimpleNamespace(close=AsyncMock())
        transport = AsyncHTTPTransport("https://example.com", client_factory=lambda **_: client)
        started = asyncio.Event()

        async def pending(*args, **kwargs):
            started.set()
            await asyncio.Event().wait()

        with patch.object(transport, "_send", side_effect=pending):
            task = asyncio.create_task(transport.send("GET", "/"))
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertFalse(transport._owned_refs)
        await transport.close()
        client.close.assert_awaited_once()

    async def test_cancelled_stream_open_releases_owned_client_reference(self):
        client = SimpleNamespace(close=AsyncMock())
        transport = AsyncHTTPTransport("https://example.com", client_factory=lambda **_: client)
        started = asyncio.Event()

        @asynccontextmanager
        async def pending(*args, **kwargs):
            started.set()
            await asyncio.Event().wait()
            yield

        async def open_stream():
            async with transport.stream("GET", "/"):
                pass

        with patch.object(transport, "_open_stream", side_effect=pending):
            task = asyncio.create_task(open_stream())
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertFalse(transport._owned_refs)
        await transport.close()
        client.close.assert_awaited_once()

    async def test_old_timeout_cannot_retire_a_new_client(self):
        first = SimpleNamespace(close=AsyncMock())
        second = SimpleNamespace(close=AsyncMock())
        transport = AsyncHTTPTransport("https://example.com")
        transport.client = second
        await transport._reset_owned_client(first)
        self.assertIs(transport.client, second)
        second.close.assert_not_awaited()
        await transport.close()

    async def test_custom_implementation_uses_public_events_and_the_shared_operation_pipeline(self):
        class CustomProvider(AsyncProvider):
            provider_type = ProviderType.GROK

            async def _stream_provider_events(self, request: ProviderRequest):
                payload = await self.transport.request(
                    "POST", "/fixture", json={"text": request.prompt}
                )
                event: ProviderStreamEvent = TextDelta(payload["answer"])
                yield event

        client = SimpleNamespace(
            request=AsyncMock(return_value=FakeHTTPResponse(200, json={"answer": "reply"})),
            close=AsyncMock(),
        )
        transport = AsyncHTTPTransport("https://grok.com", client_factory=lambda **_: client)
        async with CustomProvider(
            Account.from_cookies("grok", {}), transport=transport
        ) as provider:
            chat = await provider.chats.create()
            operation = chat.responses.create("question")
            response = await operation.wait()
            self.assertEqual(response.text, "reply")
        client.request.assert_awaited_once()
        self.assertEqual(client.request.call_args.kwargs["json"], {"text": "question"})
        client.close.assert_awaited_once()

    def test_http_transport_defaults_to_curl_session_and_validates_config(self):
        from curl_cffi.requests import Session as CurlSession

        import webaiprovider.transport as transport_pkg

        self.assertFalse(hasattr(transport_pkg, "AsyncRequestTransport"))
        self.assertEqual(TransportConfig().max_response_bytes, 64 * 1024 * 1024)
        with self.assertRaises(ValueError):
            TransportConfig(timeout=(0.0, 5.0))
        with self.assertRaises(ValueError):
            TransportConfig(timeout=(True, 5.0))
        with HTTPTransport("https://claude.ai", config=TransportConfig(timeout=(4.0, 19.0))) as tr:
            session = tr._connect()
            self.assertIsInstance(session, CurlSession)
            self.assertFalse(session.trust_env)
            self.assertEqual(session.timeout, (4.0, 19.0))

    async def test_curl_dns_pinning_ssrf_block_and_idempotent_get_retry(self):
        import socket

        from curl_cffi import CurlOpt
        from curl_cffi.requests import AsyncSession
        from curl_cffi.requests.exceptions import Timeout as CurlTimeout

        import webaiprovider.transport.http as transport_http
        from webaiprovider.core.cache import BoundedCache

        session = AsyncSession()
        responses = [
            CurlTimeout("curl: (28) Resolving timed out"),
            FakeHTTPResponse(200, json={"ok": True}),
        ]

        async def fake_request(method, url, **options):
            outcome = responses.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        session.request = AsyncMock(side_effect=fake_request)
        try:
            transport = AsyncHTTPTransport("https://claude.ai", client_getter=lambda: session)
            fresh_cache = BoundedCache(max_entries=16, max_bytes=4096, ttl=60)
            with (
                patch.object(transport_http, "_DNS_CACHE", fresh_cache),
                patch(
                    "webaiprovider.transport.async_http._check_ipv6_connectivity",
                    return_value=False,
                ),
                patch("asyncio.get_running_loop") as loop_getter,
            ):
                loop_getter.return_value.getaddrinfo = AsyncMock(
                    return_value=[
                        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))
                    ]
                )
                self.assertEqual(await transport.request("GET", "/fixture"), {"ok": True})
                self.assertEqual(session.request.await_count, 2)
                self.assertEqual(session.curl_options[CurlOpt.IPRESOLVE], 1)
                self.assertIn("claude.ai:443:93.184.216.34", session.curl_options[CurlOpt.RESOLVE])

                responses.append(CurlTimeout("curl: (28) Operation timed out"))
                with self.assertRaises(CurlTimeout):
                    await transport.send("POST", "/fixture")
                self.assertEqual(session.request.await_count, 3)

                fresh_cache.remove(("claude.ai", 443, False))
                loop_getter.return_value.getaddrinfo = AsyncMock(
                    return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]
                )
                with self.assertRaisesRegex(ValueError, "Prohibited destination network address"):
                    await transport.send("GET", "/fixture")

                async def stalled_getaddrinfo(*_args, **_kwargs):
                    await asyncio.sleep(1.0)

                fresh_cache.remove(("claude.ai", 443, False))
                loop_getter.return_value.getaddrinfo = stalled_getaddrinfo
                short_transport = AsyncHTTPTransport(
                    "https://claude.ai",
                    client_getter=lambda: session,
                    config=TransportConfig(timeout=(0.05, 1.0)),
                )
                with self.assertRaisesRegex(CurlTimeout, "Resolving timed out"):
                    await short_transport.send("POST", "/fixture")

                fresh_cache.remove(("claude.ai", 443, False))
                loop_getter.return_value.getaddrinfo = AsyncMock(
                    side_effect=socket.gaierror("offline")
                )
                with self.assertRaisesRegex(ProviderError, "Provider host resolution failed"):
                    await short_transport.send("GET", "/fixture")
        finally:
            await session.close()

    async def test_stream_mid_body_timeout_does_not_retry_and_concurrent_retry_defers_close(self):
        from curl_cffi.requests.exceptions import Timeout as CurlTimeout

        stream_calls = 0

        @asynccontextmanager
        async def failing_body_stream(*_args, **_kwargs):
            nonlocal stream_calls
            stream_calls += 1

            async def chunks():
                yield b"head"
                raise CurlTimeout("mid-stream timeout")

            yield SimpleNamespace(status_code=200, headers={}, aiter_content=chunks)

        stream_transport = AsyncHTTPTransport(
            "https://claude.ai",
            client_getter=lambda: SimpleNamespace(stream=failing_body_stream),
        )
        received = []
        with self.assertRaises(CurlTimeout):
            async with stream_transport.stream("GET", "/stream") as response:
                async for chunk in response.aiter_bytes():
                    received.append(chunk)
        self.assertEqual(received, [b"head"])
        self.assertEqual(stream_calls, 1)

        sibling_entered = asyncio.Event()
        release_sibling = asyncio.Event()
        clients = []

        def make_client(timeout=None):
            client_index = len(clients)
            closed_flag = False

            async def close():
                nonlocal closed_flag
                closed_flag = True

            async def request(method, url, **options):
                if url.endswith("/slow") and client_index == 0:
                    sibling_entered.set()
                    await release_sibling.wait()
                    if closed_flag:
                        raise RuntimeError("sibling client closed while in flight")
                    return FakeHTTPResponse(200, json={"sibling": True})
                if client_index == 0:
                    raise CurlTimeout("first client timed out")
                return FakeHTTPResponse(200, json={"retried": True})

            client = SimpleNamespace(request=request, close=close, is_closed=lambda: closed_flag)
            clients.append(client)
            return client

        shared_transport = AsyncHTTPTransport("https://claude.ai", client_factory=make_client)
        try:
            sibling_task = asyncio.create_task(shared_transport.request("GET", "/slow"))
            await asyncio.wait_for(sibling_entered.wait(), 1)
            retried = await shared_transport.request("GET", "/fast")
            self.assertEqual(retried, {"retried": True})
            self.assertFalse(clients[0].is_closed())
            release_sibling.set()
            self.assertEqual(await sibling_task, {"sibling": True})
            self.assertTrue(clients[0].is_closed())
            self.assertFalse(clients[1].is_closed())
        finally:
            await shared_transport.close()

    def transport(self, origin="https://claude.ai", **options):
        client = SimpleNamespace(
            request=AsyncMock(return_value=FakeHTTPResponse(200, json={"ok": True})),
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
        client.request.return_value = FakeHTTPResponse(503)
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
        first_client.request.return_value = FakeHTTPResponse(429, headers={"retry-after": "60"})
        for _ in range(2):
            with self.assertRaises(RateLimited):
                await first.request("GET", "/api/test")
        first_client.request.assert_awaited_once()
        self.assertEqual(await second.request("GET", "/api/test"), {"ok": True})

    async def test_cancelled_and_oversized_streams_close_the_response(self):
        for oversized in (False, True):
            with self.subTest(oversized=oversized):
                released = asyncio.Event()

                async def chunks(oversized=oversized):
                    yield b"first"
                    if oversized:
                        yield b"too large"
                    else:
                        await asyncio.Event().wait()

                @asynccontextmanager
                async def stream(*args, released=released, **options):
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

                async def consume(transport=transport, entered=entered):
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

    async def test_send_limits_unknown_length_body_and_closes_stream_without_retry(self):
        for headers, chunks, expected_reads in (
            ({"content-length": "9"}, [b"1234", b"56"], 0),
            ({}, [b"1234", b"56"], 2),
        ):
            with self.subTest(headers=headers):
                reads = 0
                opens = 0
                closed = asyncio.Event()

                async def body():
                    nonlocal reads
                    for chunk in chunks:
                        reads += 1
                        yield chunk

                @asynccontextmanager
                async def stream(*_args, **_options):
                    nonlocal opens
                    opens += 1
                    try:
                        yield SimpleNamespace(
                            status_code=200,
                            headers=headers,
                            aiter_content=body,
                        )
                    finally:
                        closed.set()

                transport = AsyncHTTPTransport(
                    "https://claude.ai",
                    client_getter=lambda: SimpleNamespace(stream=stream),
                    config=TransportConfig(max_response_bytes=5),
                )
                with self.assertRaisesRegex(ProviderError, "size limit"):
                    await transport.send("POST", "/fixture")
                self.assertEqual(reads, expected_reads)
                self.assertEqual(opens, 1)
                self.assertTrue(closed.is_set())

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
                provider._prepare_session = AsyncMock()
                provider._close_session = AsyncMock()
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


class BlockingTransportTests(unittest.TestCase):
    def test_blocking_stream_close_cancels_a_pending_read(self):
        started = threading.Event()
        received = threading.Event()
        chunks = []
        exits = []
        errors = []

        async def hung_chunks():
            yield b"first"
            started.set()
            await asyncio.Event().wait()
            yield b"never"

        @asynccontextmanager
        async def open_stream(*_args, **_kwargs):
            try:
                yield FakeHTTPResponse(200, stream=hung_chunks())
            finally:
                exits.append(True)

        transport = AsyncHTTPTransport(
            "https://chatgpt.com", client_getter=lambda: SimpleNamespace(stream=open_stream)
        )
        provider = Provider(Account.from_cookies("chatgpt", {}), transport=transport)
        provider._provider._prepare_session = AsyncMock()
        provider._provider._close_session = AsyncMock()
        provider.open()
        self.addCleanup(provider.close)
        holder = []

        def read():
            try:
                with provider.transport.stream("POST", "/fixture") as stream:
                    holder.append(stream)
                    for chunk in stream.iter_bytes():
                        chunks.append(chunk)
                        received.set()
            except BaseException as failure:
                errors.append(failure)

        reader = threading.Thread(target=read, daemon=True)
        reader.start()
        self.assertTrue(started.wait(timeout=2))
        self.assertTrue(received.wait(timeout=2))
        closer = threading.Thread(target=holder[0].close, daemon=True)
        closer.start()
        closer.join(timeout=2)
        reader.join(timeout=2)
        self.assertFalse(closer.is_alive())
        self.assertFalse(reader.is_alive())
        self.assertEqual(chunks, [b"first"])
        self.assertEqual(exits, [True])
        self.assertEqual(errors, [])

    def test_default_buffered_curl_transport_runs_in_caller_and_bounds_body(self):
        from curl_cffi.requests import Session
        from curl_cffi.requests.exceptions import Timeout

        release = threading.Event()
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append(self.path)
                self.send_response(200)
                if self.path == "/stall":
                    self.send_header("Content-Length", "1")
                self.end_headers()
                if self.path == "/stall":
                    release.wait(timeout=2)
                    return
                self.wfile.write(b"ok" if self.path == "/ok" else b"x" * 64)

            def log_message(self, *_args):
                pass

        with HTTPServer(("127.0.0.1", 0), Handler) as server:
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            caller = threading.get_ident()
            request = Session.request

            def request_on_caller(session, *args, **kwargs):
                collect = kwargs["content_callback"]

                def collect_on_caller(chunk):
                    self.assertEqual(threading.get_ident(), caller)
                    return collect(chunk)

                kwargs["content_callback"] = collect_on_caller
                return request(session, *args, **kwargs)

            try:
                with (
                    patch("webaiprovider.transport.http._prepare_curl_session_sync"),
                    patch.object(Session, "request", request_on_caller),
                    patch.object(
                        Session, "stream", side_effect=AssertionError("Stream worker used")
                    ),
                    HTTPTransport(
                        f"http://127.0.0.1:{server.server_port}",
                        config=TransportConfig(max_response_bytes=16),
                    ) as transport,
                ):
                    self.assertEqual(transport.send("GET", "/ok").content, b"ok")
                    with self.assertRaisesRegex(ProviderError, "size limit"):
                        transport.send("GET", "/large")
                    with self.assertRaises(Timeout):
                        transport.send("GET", "/stall", timeout=0.1)
                self.assertEqual(requests, ["/ok", "/large", "/stall"])
            finally:
                release.set()
                server.shutdown()
                worker.join(timeout=2)

    def test_default_curl_stream_yields_on_caller_before_transfer_finishes(self):
        from curl_cffi.requests import Session

        from webaiprovider.errors import AccountPaused
        from webaiprovider.transport.stream import _CurlStream

        release = threading.Event()
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append(self.path)
                if self.path == "/redirect":
                    self.send_response(302)
                    self.send_header("Location", "/forbidden")
                    self.end_headers()
                    return
                if self.path == "/empty":
                    self.send_response(204)
                    self.end_headers()
                    return
                self.send_response(200)
                if self.path == "/stream":
                    self.send_header("Set-Cookie", "fixture=updated; Path=/")
                self.end_headers()
                if self.path == "/stream":
                    self.wfile.write(b"first")
                    release.wait(timeout=3)
                    self.wfile.write(b"after")
                else:
                    self.wfile.write(b"x" * (70_000 if self.path == "/overflow" else 50_000))

            def log_message(self, *_args):
                pass

        caller = threading.get_ident()
        original_receive = _CurlStream.receive

        def receive(stream, chunk):
            self.assertEqual(threading.get_ident(), caller)
            return original_receive(stream, chunk)

        with HTTPServer(("127.0.0.1", 0), Handler) as server:
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                with (
                    patch("webaiprovider.transport.http._prepare_curl_session_sync"),
                    patch.object(_CurlStream, "receive", receive),
                    patch.object(
                        Session, "stream", side_effect=AssertionError("Stream worker used")
                    ),
                    HTTPTransport(
                        f"http://127.0.0.1:{server.server_port}",
                        config=TransportConfig(max_stream_bytes=60_000),
                    ) as transport,
                ):
                    with transport.stream("GET", "/stream") as response:
                        chunks = response.iter_bytes()
                        self.assertEqual(next(chunks), b"first")
                        self.assertEqual(transport.client.cookies.get("fixture"), "updated")
                        release.set()
                        self.assertEqual(b"".join(chunks), b"after")
                    with transport.stream("GET", "/large") as response:
                        self.assertEqual(b"".join(response.iter_bytes()), b"x" * 50_000)
                    with transport.stream("GET", "/large") as response:
                        response.close()
                        self.assertEqual(list(response.iter_bytes()), [])
                    with transport.stream("GET", "/empty") as response:
                        self.assertEqual(response.status_code, 204)
                        self.assertEqual(list(response.iter_bytes()), [])
                    with transport.stream("GET", "/overflow") as response:
                        with self.assertRaisesRegex(ProviderError, "size limit"):
                            list(response.iter_bytes())
                    with self.assertRaises(AccountPaused), transport.stream("GET", "/redirect"):
                        self.fail("Redirect was exposed to the consumer")
                self.assertEqual(
                    requests, ["/stream", "/large", "/large", "/empty", "/overflow", "/redirect"]
                )
            finally:
                release.set()
                server.shutdown()
                worker.join(timeout=2)

    def test_all_blocking_provider_facades_use_the_injected_async_transport(self):
        for provider_type, origin in (
            ("chatgpt", "https://chatgpt.com"),
            ("claude", "https://claude.ai"),
            ("gemini", "https://gemini.google.com"),
            ("grok", "https://grok.com"),
        ):
            with self.subTest(provider=provider_type):
                request = AsyncMock(return_value=FakeHTTPResponse(200, json={"ok": True}))
                transport = AsyncHTTPTransport(
                    origin, client_getter=lambda: SimpleNamespace(request=request)
                )
                provider = Provider(Account.from_cookies(provider_type, {}), transport=transport)
                provider._provider._prepare_session = AsyncMock()
                provider._provider._close_session = AsyncMock()
                with provider:
                    self.assertEqual(provider.transport.request("GET", "/fixture"), {"ok": True})
                request.assert_awaited_once()

    def test_chatgpt_native_and_public_transport_share_headers_without_refreshing_with_old_token(
        self,
    ):
        from tests.fakes import FakeAsyncHTTPClient

        async def exercise():
            requests = []
            http = FakeAsyncHTTPClient(
                lambda request: (
                    requests.append(request) or FakeHTTPResponse(200, json={"accessToken": "new"})
                )
            )
            native = ChatGPTClient(
                SimpleNamespace(cdp_url="unused", save=Mock()),
                authenticate=False,
                transport=AsyncHTTPTransport(
                    "https://chatgpt.com",
                    client_getter=lambda: http,
                    headers=lambda method, path: native._transport_headers(method, path),
                ),
            )
            try:
                native.headers = {"authorization": "Bearer old"}
                await native.refresh_session()
                await native.transport.send("GET", "/backend-api/models")
                self.assertNotIn("authorization", requests[0].headers)
                self.assertEqual(requests[1].headers["authorization"], "Bearer new")
            finally:
                await native.close()
                await http.aclose()

        asyncio.run(exercise())

    def test_sync_raw_requests_and_stream_cleanup_run_on_the_caller(self):
        threads = set()
        closed = threading.Event()
        exits = []

        async def chunks(data=b"stream"):
            threads.add(threading.get_ident())
            yield data

        @asynccontextmanager
        async def stream(*args, **options):
            fixture = args[1].endswith("/fixture")
            try:
                yield SimpleNamespace(
                    status_code=200,
                    headers={"content-type": "application/json"} if fixture else {},
                    aiter_content=lambda: chunks(b'{"ok": true}' if fixture else b"stream"),
                    encoding="utf-8",
                )
            finally:
                exits.append(True)
                closed.set()

        transport = AsyncHTTPTransport(
            "https://claude.ai",
            client_getter=lambda: SimpleNamespace(stream=stream),
        )
        provider = Provider(Account.from_cookies("claude", {}), transport=transport)
        provider._provider._prepare_session = AsyncMock()
        provider._provider._close_session = AsyncMock()
        with provider:
            self.assertEqual(provider.transport.request("GET", "/fixture"), {"ok": True})
            with provider.transport.stream("GET", "/stream") as response:
                self.assertEqual(list(response.iter_bytes()), [b"stream"])
                response.close()
            with (
                self.assertRaises(ValueError),
                provider.transport.stream("GET", "/stream"),
            ):
                raise ValueError("caller failure")
            provider.transport.record_rate_limit(2.0)
            with self.assertRaises(RateLimited):
                provider.transport.request("GET", "/fixture")
        self.assertEqual(threads, {threading.get_ident()})
        self.assertTrue(closed.is_set())
        self.assertEqual(len(exits), 3)

    def test_sync_http_request_checks_response_limits_and_disables_redirects(self):
        requests = []
        with FakeHTTPClient(
            lambda request: requests.append(request) or FakeHTTPResponse(200, content=b"large body")
        ) as client:
            transport = HTTPTransport(
                "https://claude.ai",
                client_getter=lambda: client,
                config=TransportConfig(max_response_bytes=3),
            )
            with self.assertRaisesRegex(ProviderError, "size limit"):
                transport.send("POST", "/fixture")
            self.assertEqual(len(requests), 1)

    def test_sync_http_transport_pins_dns_blocks_ssrf_and_retries_idempotent_get(self):
        import socket

        from curl_cffi import CurlOpt
        from curl_cffi.requests import Session as CurlSession
        from curl_cffi.requests.exceptions import Timeout as CurlTimeout

        import webaiprovider.transport.http as transport_http
        from webaiprovider.core.cache import BoundedCache

        outcomes = [
            CurlTimeout("curl: (28) Resolving timed out"),
            FakeHTTPResponse(200, json={"ok": True}),
        ]
        created = []

        def fake_session(**_kwargs):
            sess = CurlSession()
            sess.request = Mock(
                side_effect=lambda *_a, **_kw: (
                    (_ for _ in ()).throw(res)
                    if isinstance((res := outcomes.pop(0)), BaseException)
                    else res
                )
            )
            created.append(sess)
            return sess

        fresh_cache = BoundedCache(max_entries=16, max_bytes=4096, ttl=60)
        with (
            patch.object(transport_http, "_DNS_CACHE", fresh_cache),
            patch.object(transport_http, "_has_ipv6_connectivity", return_value=False),
            patch.object(
                transport_http.socket,
                "getaddrinfo",
                return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))],
            ) as getaddr,
        ):
            transport = HTTPTransport("https://api.example", client_factory=fake_session)
            result = transport.request("GET", "/v1")
            self.assertEqual(result, {"ok": True})
            self.assertEqual(len(created), 2)
            self.assertEqual(created[1].curl_options[CurlOpt.IPRESOLVE], 1)
            self.assertEqual(
                created[1].curl_options[CurlOpt.RESOLVE],
                ["api.example:443:93.184.216.34"],
            )
            self.assertEqual(getaddr.call_count, 2)
            transport.close()

            with (
                patch.object(
                    transport_http.socket,
                    "getaddrinfo",
                    return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 443))],
                ),
                self.assertRaises(ValueError),
            ):
                HTTPTransport("https://rebind.example", client_factory=fake_session).request(
                    "GET", "/v1"
                )

            with self.assertRaises(ValueError):
                HTTPTransport("https://169.254.169.254", client_factory=fake_session).request(
                    "GET", "/latest"
                )


class TransportOptimizationTests(unittest.IsolatedAsyncioTestCase):
    async def test_curl_resolve_cache_reuses_entry_and_refreshes_after_invalidation(self):
        import socket

        from curl_cffi import CurlOpt
        from curl_cffi.requests import AsyncSession

        import webaiprovider.transport.http as transport_http

        session = AsyncSession()
        session.request = AsyncMock(return_value=FakeHTTPResponse(200, json={"ok": True}))
        resolved_ips = iter(["93.184.216.34", "93.184.216.35"])

        async def fake_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
            ip = next(resolved_ips)
            return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, port))]

        transport_http._DNS_CACHE.clear()
        try:
            transport = AsyncHTTPTransport("https://claude.ai", client_getter=lambda: session)
            with patch.object(
                asyncio.get_running_loop(), "getaddrinfo", side_effect=fake_getaddrinfo
            ) as getaddr:
                await transport.send("GET", "/one")
                await transport.send("GET", "/two")
                resolve_one = session.curl_options[CurlOpt.RESOLVE]
                self.assertEqual(resolve_one, ["claude.ai:443:93.184.216.34"])

                with patch.object(
                    transport._dns, "resolve_entry", wraps=transport._dns.resolve_entry
                ) as build_entry:
                    await transport.send("GET", "/two-again")
                    self.assertIs(session.curl_options[CurlOpt.RESOLVE], resolve_one)
                    self.assertEqual(build_entry.call_count, 1)

                transport._invalidate_curl_dns()
                await transport.send("GET", "/three")
                resolve_three = session.curl_options[CurlOpt.RESOLVE]
                self.assertEqual(resolve_three, ["claude.ai:443:93.184.216.35"])
                self.assertIsNot(resolve_three, resolve_one)
                self.assertEqual(getaddr.await_count, 2)
        finally:
            transport_http._DNS_CACHE.clear()
            await session.close()

    async def test_check_ipv6_connectivity_offloads_uncached_probe_to_worker_thread(self):
        import socket
        import time

        import webaiprovider.transport.http as transport_http

        previous_cache = transport_http._ipv6_connectivity_cache
        socket_type = socket.socket
        caller_thread = threading.get_ident()
        probe_threads = []

        def fake_socket(*_args, **_kwargs):
            probe_threads.append(threading.get_ident())
            raise OSError("no ipv6")

        transport_http._ipv6_connectivity_cache = None
        try:
            with patch.object(transport_http, "_create_ipv6_probe_socket", side_effect=fake_socket):
                self.assertFalse(await _check_ipv6_connectivity())
            self.assertIs(socket.socket, socket_type)
            self.assertEqual(len(probe_threads), 1)
            self.assertNotEqual(probe_threads[0], caller_thread)

            transport_http._ipv6_connectivity_cache = (time.monotonic(), True)
            with patch(
                "webaiprovider.transport.http._create_ipv6_probe_socket",
                side_effect=AssertionError("unexpected"),
            ):
                self.assertTrue(await _check_ipv6_connectivity())

        finally:
            transport_http._ipv6_connectivity_cache = previous_cache

    def test_chatgpt_browser_session_import_cookies_pins_dns_and_invalidates_on_timeout(self):
        from curl_cffi import CurlOpt
        from curl_cffi.requests.exceptions import Timeout as CurlTimeout

        import webaiprovider.transport.http as transport_http

        transport_http._DNS_CACHE.clear()
        instances = []

        def build_session_cls(raise_timeout):
            class FakeSession:
                def __init__(self, **_kwargs):
                    self.curl_options = {}
                    self.cookies = SimpleNamespace(jar=[])
                    instances.append(self)

                def __enter__(self):
                    return self

                def __exit__(self, *_args):
                    return False

                def get(self, *_args, **_kwargs):
                    if raise_timeout:
                        raise CurlTimeout("timed out")
                    return SimpleNamespace(status_code=200, json=lambda: {"accessToken": "tok-1"})

            return FakeSession

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(transport_http, "_has_ipv6_connectivity", return_value=False),
            patch.object(
                transport_http.socket,
                "getaddrinfo",
                return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))],
            ),
        ):
            root = Path(directory)
            session = BrowserSession(root / "session.json")
            with patch("curl_cffi.requests.Session", build_session_cls(False)):
                session.import_cookies({"oai-did": "did-1"})
            ok_client = instances[-1]
            self.assertEqual(ok_client.curl_options.get(CurlOpt.IPRESOLVE), 1)
            self.assertEqual(
                ok_client.curl_options.get(CurlOpt.RESOLVE),
                ["chatgpt.com:443:93.184.216.34"],
            )
            self.assertIsNotNone(transport_http._DNS_CACHE.get(("chatgpt.com", 443, False)))

            with (
                patch("curl_cffi.requests.Session", build_session_cls(True)),
                self.assertRaises(CurlTimeout),
            ):
                session.import_cookies({"oai-did": "did-1"})
            timeout_client = instances[-1]
            self.assertEqual(timeout_client.curl_options.get(CurlOpt.RESOLVE), [])
            self.assertIsNone(transport_http._DNS_CACHE.get(("chatgpt.com", 443, False)))

    def test_dual_stack_resolution_prefers_ipv4_before_ipv6(self):
        import webaiprovider.transport.http as transport_http

        transport_http._DNS_CACHE.clear()
        try:
            records = [
                (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("2606:4700:4700::1111", 443, 0, 0)),
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
                (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("2606:4700:4700::1001", 443, 0, 0)),
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.35", 443)),
            ]
            with patch.object(transport_http.socket, "getaddrinfo", return_value=records):
                ips = transport_http._resolve_host_ips_sync("dual.example", 443, use_ipv6=True)
            self.assertEqual(
                ips,
                (
                    "93.184.216.34",
                    "93.184.216.35",
                    "2606:4700:4700::1111",
                    "2606:4700:4700::1001",
                ),
            )
        finally:
            transport_http._DNS_CACHE.clear()
