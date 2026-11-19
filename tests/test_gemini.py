"""Gemini provider session, streaming, history, and notebook tests."""

import asyncio
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from tests.fixtures import notebook
from tests.provider_fakes import provider_account
from webaiprovider.errors import AccountPaused, ProviderError
from webaiprovider.providers.gemini import AsyncGeminiProvider
from webaiprovider.providers.gemini.catalogs import GeminiModels, parse_model_catalog
from webaiprovider.providers.gemini.generation import generation_request, upload_files
from webaiprovider.providers.gemini.notebooks import Notebooks, project_from_native
from webaiprovider.providers.gemini.session import GeminiSession
from webaiprovider.resources.message.protocol import ProviderRequest, TextDelta, TextReplacement
from webaiprovider.resources.message.types import MessageAction
from webaiprovider.resources.model.model import EffortOption, Model
from webaiprovider.transport import AsyncHTTPTransport


# Session, catalog, streaming, and optimization contracts
async def delayed_response(_request):
    await asyncio.sleep(0.02)
    yield TextReplacement("answer")


def _enable_streaming_requests(client):
    if callable(getattr(client, "request", None)):
        return client

    async def request(method, url, **options):
        options.pop("stream", None)
        response = await getattr(client, method.lower())(url, **options)
        content = getattr(response, "content", b"")
        if not content:
            content = getattr(response, "text", "").encode()
        response.headers = getattr(response, "headers", {})

        async def aiter_content():
            if content:
                yield content

        response.aiter_content = aiter_content
        response.astream_task = None
        response.aclose = AsyncMock()
        return response

    client.request = request
    return client


class NativeGeminiTests(unittest.IsolatedAsyncioTestCase):
    async def test_saved_parameters_do_not_bypass_remote_session_validation(self):
        saved = {
            "__Secure-1PSID": "cookie",
            "token": "stale-token",
            "build": "stale-build",
            "sid": "stale-sid",
            "rpc_path_prefix": "/_/BardChatUi/",
        }
        http = SimpleNamespace(
            get=AsyncMock(return_value=SimpleNamespace(status_code=401, content=b"")),
            close=AsyncMock(),
        )
        _enable_streaming_requests(http)
        factory = Mock(return_value=http)
        session = GeminiSession(Path("unused"), http_factory=factory)
        with (
            patch("webaiprovider.providers.gemini.session.read_session_async", return_value=saved),
            self.assertRaises(AccountPaused),
        ):
            await session.open()

        http.get.assert_awaited_once()
        http.close.assert_awaited_once()
        self.assertFalse(session.verified)
        self.assertEqual(factory.call_args.kwargs["cookies"], {"__Secure-1PSID": "cookie"})

    async def test_logout_waits_for_inflight_open_and_removes_rotated_cookie_write(self):
        with tempfile.TemporaryDirectory() as folder:
            credential = Path(folder) / "session.json"
            credential.write_text('{"__Secure-1PSID":"initial"}', encoding="utf-8")
            started, release = asyncio.Event(), asyncio.Event()

            async def open_session():
                started.set()
                await release.wait()
                credential.write_text('{"__Secure-1PSID":"rotated"}', encoding="utf-8")

            client = SimpleNamespace(
                open=open_session,
                close=AsyncMock(),
                models=(),
                discover_models=AsyncMock(return_value=(Model("one", "One"),)),
            )
            provider = AsyncGeminiProvider(
                provider_account("gemini", credential), client_factory=lambda _path: client
            )
            opening = asyncio.create_task(provider._prepare_session())
            await started.wait()
            logout = asyncio.create_task(provider.forget_session())
            await asyncio.sleep(0)
            self.assertFalse(logout.done())
            release.set()
            await asyncio.gather(opening, logout)

            self.assertFalse(credential.exists())
            self.assertIsNone(provider._client)
            client.close.assert_awaited_once_with()
            with self.assertRaises(AccountPaused):
                await provider._prepare_session()

    async def test_background_prepare_and_first_request_share_session_open(self):
        provider = AsyncGeminiProvider(
            provider_account("gemini", Path("saved")), client_factory=lambda _path: client
        )
        first = Model("first", "First")

        async def open_session():
            await asyncio.sleep(0.02)

        client = SimpleNamespace(
            open=AsyncMock(side_effect=open_session),
            close=AsyncMock(),
            models=(),
            discover_models=AsyncMock(return_value=(first,)),
        )
        with patch("webaiprovider.providers.gemini.provider.session_exists", return_value=True):
            await asyncio.gather(provider._prepare_session(), provider._load_catalog())
        client.open.assert_awaited_once_with()
        client.discover_models.assert_awaited_once_with()
        await provider.close()

    async def test_web_activity_is_synced_immediately_before_generation(self):
        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused"), "test"))
        events = []
        client = SimpleNamespace(
            models=(Model("flash", "Flash"),),
            sync_activity=AsyncMock(side_effect=lambda: events.append("activity")),
        )
        provider._connect = AsyncMock(return_value=client)

        async def generate(*_args, **_kwargs):
            events.append("generation")
            yield {
                "cid": "remote",
                "metadata": ["remote", "rid", "rcid"],
                "context_only": False,
                "files": [],
                "sources": (),
                "text": "answer",
                "thoughts": "",
            }

        request = ProviderRequest("question")
        with patch("webaiprovider.providers.gemini.generation.generate", generate):
            _ = [event async for event in provider._stream(request)]

        self.assertEqual(events, ["activity", "generation"])

    async def test_silent_generation_time_is_not_mistaken_for_network_idle(self):
        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused"), "test"))
        provider.stream_idle_timeout = 0.005
        provider.stream_total_timeout = 0.5

        provider._stream = delayed_response
        events = [
            event async for event in provider._stream_provider_events(ProviderRequest("question"))
        ]

        self.assertEqual(events, [TextReplacement("answer")])

    async def test_stream_still_enforces_the_total_request_deadline(self):
        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused"), "test"))
        provider.stream_total_timeout = 0.005

        provider._stream = delayed_response
        with self.assertRaisesRegex(ProviderError, "10-minute time limit"):
            _ = [
                event
                async for event in provider._stream_provider_events(ProviderRequest("question"))
            ]

    async def test_activity_sync_uses_the_web_preference_rpc(self):
        session = GeminiSession(Path("unused"))
        session.batch = AsyncMock()

        await session.sync_activity()

        payloads = session.batch.await_args.args[0]
        self.assertEqual(
            [(item.rpcid, item.payload) for item in payloads],
            [
                ("ESY5D", '[[["bard_activity_enabled"]]]'),
            ],
        )

    async def test_catalog_is_cached_but_expiry_and_force_refresh_entitlements(self):
        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused")))
        first, second = Model("first", "First"), Model("second", "Second")
        session = SimpleNamespace(
            models=(first,), discover_models=AsyncMock(return_value=(second,))
        )
        provider._connect = AsyncMock(return_value=session)
        provider._catalog_at = time.monotonic()
        self.assertEqual((await provider._load_catalog()).models, (first,))
        await asyncio.gather(provider._load_catalog(), provider._load_catalog())
        session.discover_models.assert_not_awaited()
        self.assertEqual((await provider._load_catalog(force=True)).models, (second,))
        provider._catalog_at -= 301
        await asyncio.gather(provider._load_catalog(), provider._load_catalog())
        self.assertEqual(session.discover_models.await_count, 2)

    async def test_delete_clears_cached_continuation_only_after_remote_confirmation(self):
        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused")))
        provider._client = SimpleNamespace()
        provider._chat_metadata.put("conversation", ["conversation", "rid", "rcid"])
        provider.rpc.call = AsyncMock(side_effect=[[], []])

        await provider._delete_chat_data("conversation")

        self.assertIsNone(provider._chat_metadata.get("conversation"))

    async def test_rotated_credentials_are_serialized_and_saved_on_close(self):
        session = GeminiSession("unused")
        current = {"__Secure-1PSID": "first"}
        session.verified = True
        session.http = SimpleNamespace(
            cookies=SimpleNamespace(get_dict=lambda: dict(current)), close=AsyncMock()
        )
        started, release = threading.Event(), threading.Event()
        writes = []

        def save(_path, value):
            writes.append(value)
            if len(writes) == 1:
                started.set()
                if not release.wait(2):
                    raise TimeoutError("save not released")

        with patch("webaiprovider.accounts.credentials.save_session", side_effect=save):
            first = asyncio.create_task(session.persist())
            self.assertTrue(await asyncio.to_thread(started.wait, 2))
            current["__Secure-1PSID"] = "rotated"
            second = asyncio.create_task(session.persist())
            await asyncio.sleep(0.01)
            self.assertEqual(len(writes), 1)
            release.set()
            await asyncio.gather(first, second)
            await session.persist()
            self.assertEqual(len(writes), 2)
            current["__Secure-1PSID"] = "last"
            await session.close()
        self.assertEqual([v["__Secure-1PSID"] for v in writes], ["first", "rotated", "last"])
        self.assertIsNone(session.http)

    def test_model_selection_notebook_temporary_and_effort_are_native(self):
        model = Model(
            "dynamic",
            "Dynamic",
            (EffortOption("extended", "Remote title"),),
            default_effort="extended",
            metadata={
                "model_id": "remote",
                "capacity": 2,
                "capacity_slot": 12,
                "number": 3,
                "effort_values": {"extended": 2},
            },
        )
        session = SimpleNamespace(
            models=(model,), language="en", token="test", session_id="session"
        )
        request = ProviderRequest(
            "hello",
            model="dynamic",
            effort="extended",
            temporary=True,
        )
        headers, data = generation_request(session, request, ["cid"], [], ("notebooks/test", [2]))
        body = json.loads(json.loads(data["f.req"])[1])
        self.assertEqual(
            (body[19], body[40][13], body[45], body[79], body[80]), ("notebooks/test", [2], 1, 3, 2)
        )
        self.assertEqual(body[6], [1])
        self.assertEqual(body[17], [[0]])
        selector = json.loads(headers["x-goog-ext-525001261-jspb"])
        self.assertEqual(selector[11], 2)
        self.assertEqual(selector[7], 1)
        with self.assertRaises(ValueError):
            generation_request(
                session,
                ProviderRequest("hello", model="unavailable"),
                [],
                [],
            )

    async def test_model_policy_controls_available_efforts_defaults_and_wire_values(self):
        row = [None] * 18
        row[0], row[1], row[11], row[17] = "remote", "flash", "Flash", 1
        legacy = [None] * 18
        legacy[15], legacy[16], legacy[17] = [row], [], []
        options = [
            {"thinkingLevel": "THINKING_LEVEL_STANDARD", "title": "Fast"},
            {"thinkingLevel": "THINKING_LEVEL_MEDIUM", "title": "Balanced"},
            {"thinkingLevel": "THINKING_LEVEL_DEEP_THINK"},
            {"thinkingLevel": "THINKING_LEVEL_EXTENDED", "title": "Thorough", "isDisabled": True},
        ]
        policy = {
            "modeConfig": {"id": "", "modeCategory": "FAST", "title": "Provider model"},
            "hosts": ["MODE_PICKER_HOST_CHAT"],
            "implicitThinkingLevel": "THINKING_LEVEL_MEDIUM",
            "thinkingLevelConfigs": options,
        }
        status = {
            "bardModeConfigs": [{"id": "remote", "modeCategory": "FAST"}],
            "geminiModePolicy": {"configs": [policy]},
        }
        session = GeminiSession(Path("unused"))
        session.rpc_path_prefix = "/_/BardChatUi/"
        mock_status_response = SimpleNamespace(
            status_code=200,
            text=json.dumps([{"data": {"viewer": {"rpcs": {"getUserStatus": status}}}}]),
            content=b"",
        )
        session.http = _enable_streaming_requests(
            SimpleNamespace(
                get=AsyncMock(return_value=mock_status_response),
                post=AsyncMock(return_value=mock_status_response),
            )
        )
        session.batch = AsyncMock(
            return_value=SimpleNamespace(
                text=json.dumps([["wrb.fr", "otAQ7b", json.dumps(legacy)]]) + "\n",
            )
        )
        session.models = await session.discover_models()
        (model,) = session.models
        self.assertEqual(
            [(e.id, e.title) for e in model.efforts],
            [("standard", "standard"), ("medium", "medium"), ("deep_think", "deep_think")],
        )
        self.assertEqual(model.default_effort, "medium")
        self.assertEqual(model.title, "Provider model")
        session.token, session.session_id = "token", "session"
        request = ProviderRequest("hello", model=model.id)
        headers, data = generation_request(session, request, [], [])
        self.assertEqual(json.loads(json.loads(data["f.req"])[1])[80], 4)
        self.assertEqual(json.loads(headers["x-goog-ext-525001261-jspb"])[-2], 4)
        self.assertEqual(json.loads(headers["x-goog-ext-525001261-jspb"])[7], 0)
        from dataclasses import replace

        with self.assertRaisesRegex(ValueError, "effort"):
            generation_request(session, replace(request, effort="extended"), [], [])

        # A refreshed server policy changes the choices without local model-name tables.
        options[1]["isDisabled"] = True
        options[2]["isDisabled"] = True
        policy["implicitThinkingLevel"] = "THINKING_LEVEL_STANDARD"
        session.http.get.return_value.text = json.dumps(
            [
                {"data": {"viewer": {"rpcs": {"getUserStatus": status}}}},
            ]
        )
        (refreshed,) = await session.discover_models()
        self.assertEqual(refreshed.efforts, (EffortOption("standard", "standard"),))
        self.assertEqual(refreshed.default_effort, "standard")
        policy["modeConfig"]["isDisabled"] = True
        session.http.get.return_value.text = json.dumps(
            [
                {"data": {"viewer": {"rpcs": {"getUserStatus": status}}}},
            ]
        )
        with self.assertRaisesRegex(ProviderError, "available models"):
            await session.discover_models()

    def test_parse_model_catalog_normalizes_hydrogen_version_prefix(self):
        row = [None] * 18
        row[0] = "56fdd199312815e2"
        row[1] = "4 Hydrogen"
        row[2] = "4 Hydrogen"
        row[11] = "4 Hydrogen"
        row[17] = 20
        legacy = [None] * 18
        legacy[15], legacy[16], legacy[17] = [row], [], []
        status = {
            "bardModeConfigs": [{"id": "56fdd199312815e2", "modeCategory": "MODE_CATEGORY_FAST"}],
            "geminiModePolicy": {
                "configs": [
                    {
                        "modeConfig": {
                            "id": "56fdd199312815e2",
                            "modeCategory": "MODE_CATEGORY_FAST",
                            "title": "4 Hydrogen",
                        },
                        "hosts": ["MODE_PICKER_HOST_CHAT"],
                        "implicitThinkingLevel": "THINKING_LEVEL_STANDARD",
                        "thinkingLevelConfigs": [
                            {"thinkingLevel": "THINKING_LEVEL_STANDARD"},
                            {"thinkingLevel": "THINKING_LEVEL_MEDIUM"},
                            {"thinkingLevel": "THINKING_LEVEL_EXTENDED"},
                        ],
                    }
                ]
            },
        }
        rpc_text = json.dumps([["wrb.fr", "otAQ7b", json.dumps(legacy)]]) + "\n"
        (model,) = parse_model_catalog(status, rpc_text)
        self.assertEqual(model.id, GeminiModels.HYDROGEN.id)
        self.assertEqual(model.id, "gemini-hydrogen")
        self.assertEqual(model.metadata["model_id"], "56fdd199312815e2")
        self.assertIsNone(model.metadata["selector_id"])
        self.assertIsNone(model.metadata["number"])
        self.assertEqual(
            [choice.id for choice in model.efforts],
            ["standard", "medium", "extended"],
        )

    async def test_uploads_use_local_paths_serially_and_close_mime(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / name for name in ("one.txt", "two.txt")]
            for path in paths:
                path.write_text("fixture")
            http = _enable_streaming_requests(
                SimpleNamespace(
                    post=AsyncMock(
                        return_value=SimpleNamespace(
                            status_code=200, text="uploaded", headers={}, content=b"uploaded"
                        )
                    )
                )
            )
            session = SimpleNamespace(http=http, push_id="push")
            session.upload_transport = AsyncHTTPTransport(
                "https://content-push.googleapis.com", client_getter=lambda: session.http
            )
            with patch(
                "pathlib.Path.read_bytes", side_effect=AssertionError("must stream from disk")
            ):
                files = await upload_files(session, paths)
            self.assertEqual([item[1] for item in files], ["one.txt", "two.txt"])
            self.assertEqual(http.post.await_count, 2)

            byte_files = await upload_files(
                session,
                [{"bytes": b"inline data", "name": "inline.txt", "mime_type": "text/plain"}],
            )
            self.assertEqual(byte_files, [[["uploaded"], "inline.txt"]])
            self.assertEqual(http.post.await_count, 3)

            http.post.reset_mock()
            with self.assertRaisesRegex(ValueError, "empty or exceeds"):
                await upload_files(
                    session,
                    [{"bytes": b"", "name": "empty.txt", "mime_type": "text/plain"}],
                )
            with self.assertRaisesRegex(ValueError, "Too many Gemini attachments"):
                await upload_files(
                    session,
                    [
                        {"bytes": b"x", "name": f"{i}.txt", "mime_type": "text/plain"}
                        for i in range(11)
                    ],
                )
            http.post.assert_not_awaited()

    async def test_rpc_is_account_owned_and_never_retries_writes(self):
        from webaiprovider.providers.gemini.rpc import GeminiRPC

        client = SimpleNamespace(
            batch=AsyncMock(return_value=SimpleNamespace(text='[["wrb.fr","rpc","[]"]]\n'))
        )
        rpc = GeminiRPC(AsyncMock(return_value=client))
        self.assertEqual(await rpc.call("rpc", ["test"]), [[]])
        client.batch.assert_awaited_once()
        client.batch.return_value.text = "invalid"
        with self.assertRaises(ProviderError):
            await rpc.call("rpc", [])

    async def test_large_rpc_json_is_decoded_off_the_provider_loop(self):
        from webaiprovider.providers.gemini.rpc import GeminiRPC

        response = SimpleNamespace(
            content=b" " * (256 * 1024),
            text='[["wrb.fr","rpc","[]"]]\n',
        )
        client = SimpleNamespace(batch=AsyncMock(return_value=response))
        rpc = GeminiRPC(AsyncMock(return_value=client))
        original = rpc._decode
        threads = []

        def decode(*args):
            threads.append(threading.get_ident())
            return original(*args)

        with patch.object(rpc, "_decode", side_effect=decode):
            self.assertEqual(await rpc.call("rpc", []), [[]])
        self.assertNotEqual(threads, [threading.get_ident()])

    async def test_close_cancels_and_joins_refresh_then_closes_http(self):
        session = GeminiSession(Path("unused"))
        session.http = SimpleNamespace(close=AsyncMock())
        http = session.http
        task = session.refresh_task = asyncio.create_task(asyncio.sleep(3600))
        await session.close()
        self.assertTrue(task.cancelled())
        http.close.assert_awaited_once()
        await session.close()

    async def test_cookie_refresh_retries_transient_network_error_with_backoff(self):
        session = GeminiSession(Path("unused"))
        response = SimpleNamespace(status_code=200, headers={}, content=b"")
        requests = 0

        async def post(*_args, **_kwargs):
            nonlocal requests
            requests += 1
            if requests == 1:
                raise OSError("temporary network outage")
            return response

        session.http = _enable_streaming_requests(SimpleNamespace(post=post))
        session.persist = AsyncMock()
        delays = []

        async def sleep(delay):
            delays.append(delay)
            if len(delays) == 3:
                raise asyncio.CancelledError

        with patch("webaiprovider.providers.gemini.session.asyncio.sleep", side_effect=sleep):
            task = asyncio.create_task(session.refresh())
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.assertEqual(requests, 2)
        self.assertEqual(delays, [600, 15, 600])
        session.persist.assert_awaited_once_with()

    async def test_cookie_refresh_stops_on_expired_session(self):
        session = GeminiSession(Path("unused"))
        session.http = _enable_streaming_requests(
            SimpleNamespace(
                post=AsyncMock(
                    return_value=SimpleNamespace(status_code=401, headers={}, content=b"")
                )
            )
        )

        with patch("webaiprovider.providers.gemini.session.asyncio.sleep", new=AsyncMock()):
            await asyncio.wait_for(session.refresh(), 1)

        session.http.post.assert_awaited_once()

    async def test_history_preserves_thoughts_and_stable_ids(self):
        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused"), "test"))
        candidate = [None] * 38
        candidate[0], candidate[1], candidate[37] = "rcid", ["Answer"], [["Thought"]]
        turn = [["cid", "rid"], None, [["Question"]], [[candidate]]]
        provider.rpc.call = AsyncMock(return_value=[[[turn]]])
        history = await provider._get_chat_data("cid")
        self.assertEqual([m.channel for m in history.messages], [None, "analysis", None])
        self.assertEqual(history.messages[-1].id, "rid:model")


class StreamTests(unittest.IsolatedAsyncioTestCase):
    def session(self, response):
        if response is not None:
            response.astream_task = asyncio.create_task(asyncio.Event().wait())
            response.astream_task.add_done_callback(lambda _: setattr(response, "closed", True))

        session = SimpleNamespace(
            http=SimpleNamespace(request=AsyncMock(return_value=response)),
            models=(
                Model(
                    "flash",
                    "Flash",
                    (EffortOption("standard", "Low"),),
                    "standard",
                    {
                        "model_id": "remote",
                        "capacity": 2,
                        "capacity_slot": 12,
                        "number": 1,
                        "effort_values": {"standard": 1},
                    },
                ),
            ),
            language="en",
            token="test",
            session_id="test",
            params=dict,
            persist=AsyncMock(),
        )

        session.transport = AsyncHTTPTransport(
            "https://gemini.google.com", client_getter=lambda: session.http
        )
        return session

    def response(self, payloads):
        async def content():
            for payload in payloads:
                yield payload

        return SimpleNamespace(
            status_code=200,
            headers={},
            aiter_content=content,
            aclose=AsyncMock(),
            closed=False,
            content=b"",
        )

    async def test_generation_rejection_is_typed_and_does_not_replay_the_request(self):
        from webaiprovider.providers.gemini.errors import GeminiGenerationRejected
        from webaiprovider.providers.gemini.generation import generate

        frame = [None] * 6
        frame[5] = [None, None, [[None, [1]]]]
        payload = json.dumps([frame])
        size = len(payload) + 1
        response = self.response([f")]}}'\n\n{size}\n{payload}".encode()])
        session = self.session(response)
        with self.assertRaises(GeminiGenerationRejected):
            _ = [chunk async for chunk in generate(session, ProviderRequest("test"), [])]
        session.http.request.assert_awaited_once()

    async def test_completed_stream_preserves_text_thoughts_and_cid(self):
        from webaiprovider.core.timing import SendTiming, use_send_timing
        from webaiprovider.providers.gemini.generation import generate

        candidate = [None] * 38
        candidate[0], candidate[1], candidate[8] = "rcid", ["answer"], [2]
        activity = [None] * 8
        activity[1] = activity[5] = "Searching the web"
        activity[7] = [["favicon", "https://example.com", "Example"]]
        candidate[37] = [["thought"], [activity]]
        body = [None, ["c_new", "rid"], None, None, [candidate]]
        acknowledged = [None, ["c_new", "rid"]]

        def encode_frame(payload):
            encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            units = len(("\n" + encoded).encode("utf-16-le")) // 2
            return f"{units}\n{encoded}"

        wire = (
            ")]}'\n\n"
            + encode_frame([["wrb.fr", None, json.dumps(acknowledged)]])
            + encode_frame([["wrb.fr", None, json.dumps(body)]])
            + "\n"
        ).encode()
        response = self.response([wire[:20], wire[20:]])
        session = self.session(response)
        timing = SendTiming("gemini")
        with use_send_timing(timing):
            result = [c async for c in generate(session, ProviderRequest("question"), [])]
        self.assertEqual(
            (result[1]["cid"], result[1]["text"], result[1]["thoughts"]),
            ("c_new", "answer", "thought\nSearching the web"),
        )
        self.assertEqual(
            result[1]["sources"], ({"title": "Example", "url": "https://example.com"},)
        )
        self.assertTrue(result[1]["response_complete"])
        self.assertEqual(session.http.request.await_args.kwargs["params"], {})
        self.assertTrue(result[0]["context_only"])
        self.assertEqual(result[0]["cid"], "c_new")
        self.assertTrue(response.closed)
        session.persist.assert_awaited_once()
        durations, _counts = timing.snapshot()
        self.assertTrue(
            {
                "sse_connect_ms",
                "sse_first_event_ms",
                "sse_first_assistant_text_ms",
                "sse_last_event_ms",
            }.issubset(durations)
        )
        self.assertGreaterEqual(durations["sse_last_event_ms"], durations["sse_first_event_ms"])

    async def test_cancellation_aborts_an_actual_stalled_http_transfer(self):
        from curl_cffi.requests import AsyncSession

        from webaiprovider.providers.gemini.generation import generate

        accepted = asyncio.Event()
        requests = []

        async def serve(reader, writer):
            try:
                requests.append(await reader.readuntil(b"\r\n\r\n"))
                if requests[-1].startswith(b"GET "):
                    writer.write(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK"
                    )
                    await writer.drain()
                    return
                length = next(
                    int(line.split(b":", 1)[1])
                    for line in requests[-1].splitlines()
                    if line.lower().startswith(b"content-length:")
                )
                await reader.readexactly(length)
                writer.write(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\n[]\n\r\n")
                await writer.drain()
                accepted.set()
                await reader.read()
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        session = self.session(None)
        session.http = AsyncSession(trust_env=False)
        session.transport = AsyncHTTPTransport(
            f"http://127.0.0.1:{port}", client_getter=lambda: session.http
        )
        try:
            with patch(
                "webaiprovider.providers.gemini.generation.BASE", f"http://127.0.0.1:{port}/"
            ):
                stream = generate(session, ProviderRequest("question"), [])
                pending = asyncio.create_task(anext(stream))
                await asyncio.wait_for(accepted.wait(), 2)
                pending.cancel()
                done, _ = await asyncio.wait({pending}, timeout=2)
                self.assertIn(pending, done, "Cancellation must release a stalled transfer")
                with self.assertRaises(asyncio.CancelledError):
                    await pending
                # The pool may retain idle sockets, but cancellation must free
                # the transfer and allow a separate request to finish normally.
                result = await session.http.get(f"http://127.0.0.1:{port}/health", timeout=2)
                self.assertEqual(result.text, "OK")
                self.assertEqual(sum(r.startswith(b"POST ") for r in requests), 1)
        finally:
            await session.http.close()
            server.close()
            await server.wait_closed()

    async def test_incomplete_stream_is_not_success_or_retried(self):
        from webaiprovider.providers.gemini.generation import generate

        response = self.response([b""])
        session = self.session(response)
        with self.assertRaises(ProviderError):
            _ = [c async for c in generate(session, ProviderRequest("question"), [])]
        self.assertTrue(response.closed)
        session.persist.assert_not_awaited()

    async def test_cancellation_closes_stream_without_resending(self):
        from webaiprovider.providers.gemini.generation import generate

        entered = asyncio.Event()

        async def content():
            entered.set()
            await asyncio.Future()
            yield b""

        response = self.response([])
        response.aiter_content = content

        async def consume():
            return [
                c async for c in generate(self.session(response), ProviderRequest("question"), [])
            ]

        task = asyncio.create_task(consume())
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(response.closed)

    def test_flash_model_omits_selector_id_and_number_for_fast_routing(self):
        flash = Model(
            "gemini-flash",
            "Gemini Flash",
            (),
            default_effort=None,
            metadata={
                "model_id": "56fdd199312815e2",
                "selector_id": None,
                "capacity": 2,
                "capacity_slot": 12,
                "number": None,
                "effort_values": {},
            },
        )
        session = SimpleNamespace(
            models=(flash,), language="en", token="test", session_id="session"
        )
        request = ProviderRequest("hello", model="gemini-flash")
        headers, data = generation_request(session, request, ["cid", "", ""], [])
        body = json.loads(json.loads(data["f.req"])[1])
        selector = json.loads(headers["x-goog-ext-525001261-jspb"])
        self.assertIsNone(body[79])
        self.assertIsNone(selector[4])
        self.assertIsNone(selector[-3])

    async def test_sync_activity_is_throttled_unless_forced(self):
        session = GeminiSession(Path("unused"))
        session.batch = AsyncMock()
        with patch(
            "webaiprovider.providers.gemini.session.time.monotonic", side_effect=[1.0, 1.0, 301.0]
        ):
            await session.sync_activity()
            await session.sync_activity()
            self.assertEqual(session.batch.await_count, 1)
            await session.sync_activity(force=True)
        self.assertEqual(session.batch.await_count, 2)

    async def test_chat_metadata_caches_turn_ids_for_consecutive_stream_calls(self):
        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused"), "test"))
        client = SimpleNamespace(
            models=(Model("gemini-flash", "Flash"),),
            sync_activity=AsyncMock(),
        )
        provider._connect = AsyncMock(return_value=client)
        provider.rpc = SimpleNamespace(call=AsyncMock())

        async def fake_generate(*_args, **_kwargs):
            yield {
                "cid": "conv_1",
                "metadata": ["conv_1", "rid_1", "rcid_1"],
                "text": "first answer",
                "thoughts": "",
                "files": [],
                "sources": (),
                "response_complete": True,
            }

        with patch("webaiprovider.providers.gemini.generation.generate", fake_generate):
            _ = [
                e
                async for e in provider._stream_provider_events(
                    ProviderRequest("first message", model="gemini-flash")
                )
            ]
            self.assertEqual(provider.rpc.call.await_count, 0)
            self.assertEqual(provider._chat_metadata.get("conv_1"), ["conv_1", "rid_1", "rcid_1"])

            _ = [
                e
                async for e in provider._stream_provider_events(
                    ProviderRequest(
                        "second message",
                        model="gemini-flash",
                        conversation_id="conv_1",
                    )
                )
            ]
            self.assertEqual(provider.rpc.call.await_count, 0)

    async def test_chat_metadata_cache_expires_after_verification_ttl(self):
        from webaiprovider.providers.gemini.provider import _CHAT_METADATA_TTL_SECONDS

        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused"), "test"))
        metadata = ["conversation", "request", "response"]
        with patch("webaiprovider.providers.gemini.provider.time.monotonic", return_value=100.0):
            provider._remember_chat_metadata("conversation", metadata)

        with patch(
            "webaiprovider.providers.gemini.provider.time.monotonic",
            return_value=100.0 + _CHAT_METADATA_TTL_SECONDS - 1,
        ):
            self.assertEqual(provider._take_chat_metadata("conversation"), metadata)

        with patch("webaiprovider.providers.gemini.provider.time.monotonic", return_value=100.0):
            provider._remember_chat_metadata("conversation", metadata)
        with patch(
            "webaiprovider.providers.gemini.provider.time.monotonic",
            return_value=100.0 + _CHAT_METADATA_TTL_SECONDS + 1,
        ):
            self.assertIsNone(provider._take_chat_metadata("conversation"))
        self.assertIsNone(provider._chat_metadata.get("conversation"))

    async def test_flash_variants_distinguish_default_fast_and_specialized_models(self):
        fast_row = [None] * 18
        fast_row[0], fast_row[1], fast_row[11], fast_row[17] = (
            "remote-fast",
            "flash-3.8",
            "Flash 3.8",
            1,
        )
        lite_row = [None] * 18
        lite_row[0], lite_row[1], lite_row[11], lite_row[17] = (
            "remote-lite",
            "flash",
            "Flash",
            42,
        )
        legacy = [None] * 18
        legacy[15], legacy[16], legacy[17] = [fast_row, lite_row], [], []
        policies = [
            {
                "modeConfig": {"id": "remote-fast", "modeCategory": "FAST", "title": "Flash"},
                "hosts": ["MODE_PICKER_HOST_CHAT"],
            },
            {
                "modeConfig": {
                    "id": "remote-lite",
                    "modeCategory": "FAST",
                    "title": "Flash Lite",
                },
                "hosts": ["MODE_PICKER_HOST_CHAT"],
            },
        ]
        status = {
            "bardModeConfigs": [
                {"id": "remote-fast", "modeCategory": "FAST"},
                {"id": "remote-lite", "modeCategory": "FAST"},
            ],
            "geminiModePolicy": {"configs": policies},
        }
        session = GeminiSession(Path("unused"))
        session.rpc_path_prefix = "/_/BardChatUi/"
        mock_status_response = SimpleNamespace(
            status_code=200,
            text=json.dumps([{"data": {"viewer": {"rpcs": {"getUserStatus": status}}}}]),
            content=b"",
        )
        session.http = _enable_streaming_requests(
            SimpleNamespace(
                get=AsyncMock(return_value=mock_status_response),
                post=AsyncMock(return_value=mock_status_response),
            )
        )
        session.batch = AsyncMock(
            return_value=SimpleNamespace(
                text=json.dumps([["wrb.fr", "otAQ7b", json.dumps(legacy)]]) + "\n",
            )
        )
        models = await session.discover_models()
        self.assertEqual(len(models), 2)
        fast_model = next(m for m in models if m.metadata["model_id"] == "remote-fast")
        lite_model = next(m for m in models if m.metadata["model_id"] == "remote-lite")
        self.assertIsNone(fast_model.metadata["selector_id"])
        self.assertIsNone(fast_model.metadata["number"])
        self.assertEqual(lite_model.metadata["selector_id"], "remote-lite")
        self.assertEqual(lite_model.metadata["number"], 42)

    async def test_retry_earlier_turn_uses_edit_controls_and_preserves_prompt_and_files(self):
        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused"), "test"))
        client = SimpleNamespace(
            models=(Model("gemini-flash", "Flash"),),
            sync_activity=AsyncMock(),
        )
        provider._connect = AsyncMock(return_value=client)
        turns = [
            [
                [None, f"r_{number}"],
                None,
                [[f"Question {number}", 0, None, [{"name": f"file{number}.txt"}]]],
                [[[f"rc_{number}", [f"Answer {number}"]]]],
            ]
            for number in (3, 2, 1)
        ]
        provider.rpc = SimpleNamespace(call=AsyncMock(return_value=[[turns]]))

        captured = []

        async def fake_generate(_client, request, metadata, _context, files=None, **_kwargs):
            captured.append((request, metadata, files))
            yield {
                "cid": "conv_1",
                "metadata": ["conv_1", "r_new", "rc_new"],
                "text": "new answer",
                "thoughts": "",
                "files": [],
                "sources": (),
                "response_complete": True,
            }

        async def retry(message_id):
            with patch("webaiprovider.providers.gemini.generation.generate", fake_generate):
                return [
                    event
                    async for event in provider._stream_provider_events(
                        ProviderRequest(
                            "",
                            conversation_id="conv_1",
                            action=MessageAction.RETRY,
                            message_id=message_id,
                            model="gemini-flash",
                        )
                    )
                ]

        cases = (
            (
                "r_3:model",
                MessageAction.RETRY,
                "Question 3",
                ["conv_1", "r_2", "rc_2"],
                "file3.txt",
            ),
            ("r_2:model", MessageAction.EDIT, "Question 2", ["conv_1", "r_1", "rc_1"], "file2.txt"),
            ("r_1:model", MessageAction.EDIT, "Question 1", ["conv_1", "", ""], "file1.txt"),
        )
        for message_id, action, prompt, parent, filename in cases:
            with self.subTest(message_id=message_id):
                await retry(message_id)
                request, metadata, files = captured[-1]
                self.assertEqual(request.action, action)
                self.assertEqual(request.prompt, prompt)
                self.assertEqual(metadata[:3], parent)
                self.assertEqual(files, [{"name": filename}])

    async def test_rename_conversation_uses_muazcd_batch_and_confirms_title(self):
        session = GeminiSession(Path("unused"))
        session.http = SimpleNamespace(cookies=SimpleNamespace(get_dict=dict))
        session.token = "tok-1"
        session.sid = "sid-1"
        session.persist = AsyncMock()
        response_payload = json.dumps(
            [
                [
                    "wrb.fr",
                    "MUAZcd",
                    json.dumps([None, ["conv_1", "Renamed", "2026-11-14T00:00:00Z"]]),
                ]
            ]
        )
        session.request = AsyncMock(
            return_value=SimpleNamespace(
                status_code=200, text=")]}'\n" + response_payload, headers={}
            )
        )

        await session.rename_conversation("conv_1", " Renamed ")

        session.persist.assert_awaited_once_with(force=False)
        session.request.assert_awaited_once()
        _, call_url = session.request.await_args.args[:2]
        kwargs = session.request.await_args.kwargs
        self.assertTrue(call_url.endswith("/_/BardChatUi/data/batchexecute"))
        self.assertEqual(kwargs["params"]["rpcids"], "MUAZcd")
        self.assertEqual(kwargs["params"]["source-path"], "/app/conv_1")
        f_req = json.loads(kwargs["data"]["f.req"])
        self.assertEqual(f_req[0][0][0], "MUAZcd")
        self.assertEqual(json.loads(f_req[0][0][1]), [None, [["title"]], ["conv_1", "Renamed"]])

        session.request.return_value = SimpleNamespace(
            status_code=200,
            text=json.dumps([["wrb.fr", "MUAZcd", json.dumps([None, ["conv_1", "Wrong"]])]]),
            headers={},
        )
        with self.assertRaises(ProviderError):
            await session.rename_conversation("conv_1", "Renamed")

    async def test_discover_models_refreshes_xsrf_token_on_400(self):
        session = GeminiSession(Path("unused"))
        session.http = object()
        session.rpc_path_prefix = "/_/BardChatUi/"
        session.token = "stale-token"
        status = {
            "bardModeConfigs": [{"id": "remote", "modeCategory": "FAST"}],
            "geminiModePolicy": {
                "configs": [
                    {
                        "modeConfig": {"id": "", "modeCategory": "FAST", "title": "Provider model"},
                        "hosts": ["MODE_PICKER_HOST_CHAT"],
                    }
                ]
            },
        }
        responses = [
            SimpleNamespace(
                status_code=400,
                json=lambda: [
                    {"error": {"details": [{"@type": "type.googleapis.com/fava.request.XsrfData"}]}}
                ],
                headers={},
            ),
            SimpleNamespace(status_code=200, text=" fresh-token\n", headers={}),
            SimpleNamespace(
                status_code=200,
                text=json.dumps([{"data": {"viewer": {"rpcs": {"getUserStatus": status}}}}]),
                headers={},
            ),
        ]
        legacy = [None] * 18
        row = [None] * 18
        row[0], row[1], row[11], row[17] = "remote", "flash", "Flash", 1
        legacy[15], legacy[16], legacy[17] = [row], [], []
        session.request = AsyncMock(side_effect=responses)
        session.batch = AsyncMock(
            return_value=SimpleNamespace(
                text=json.dumps([["wrb.fr", "otAQ7b", json.dumps(legacy)]]) + "\n"
            )
        )
        session.persist = AsyncMock()

        models = await session.discover_models()

        self.assertEqual(session.token, "fresh-token")
        session.persist.assert_awaited_once_with(force=False)
        self.assertEqual([m.title for m in models], ["Provider model"])
        self.assertEqual(session.request.await_count, 3)
        self.assertEqual(
            session.request.await_args_list[2].kwargs["headers"]["X-Framework-XSRF-Token"],
            "fresh-token",
        )


class GeminiOptimizationTests(unittest.IsolatedAsyncioTestCase):
    async def test_session_open_parses_reordered_and_partial_bootstrap_fields(self):
        html = (
            '<script>window.WIZ_global_data={"eptZe":"/_/BardChatUi/","FdrFJe":"sid-1",'
            '"SNlM0e":"tok-1","TuX5cc":"tr","cfb2h":"bld-1"};</script>'
        )
        http = _enable_streaming_requests(
            SimpleNamespace(
                get=AsyncMock(
                    return_value=SimpleNamespace(
                        status_code=200, headers={}, content=html.encode(), text=html
                    )
                ),
                cookies=SimpleNamespace(get_dict=lambda: {"__Secure-1PSID": "cookie-1"}),
                close=AsyncMock(),
            )
        )
        session = GeminiSession(Path("unused"), http_factory=lambda **_kwargs: http)
        with (
            patch(
                "webaiprovider.providers.gemini.session.read_session_async",
                return_value={"__Secure-1PSID": "cookie-1"},
            ),
            patch("webaiprovider.providers.gemini.session.save_session_async", AsyncMock()),
        ):
            await session.open()
            try:
                self.assertEqual(session.token, "tok-1")
                self.assertEqual(session.build, "bld-1")
                self.assertEqual(session.sid, "sid-1")
                self.assertEqual(session.language, "tr")
                self.assertEqual(session.push_id, "")
                self.assertEqual(session.rpc_path_prefix, "/_/BardChatUi/")
            finally:
                await session.close()

    async def test_session_batch_debounces_auxiliary_cookie_persistence_and_close_flushes(self):
        from webaiprovider.accounts.credentials import save_session
        from webaiprovider.providers.gemini import session as gemini_session_mod
        from webaiprovider.providers.gemini.rpc import RPCPayload

        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "gemini.json"
            save_session(path, {"__Secure-1PSID": "p1", "NID": "n1"})
            cookie_state = {"__Secure-1PSID": "p1", "NID": "n1"}
            http = _enable_streaming_requests(
                SimpleNamespace(
                    cookies=SimpleNamespace(get_dict=lambda: dict(cookie_state)),
                    post=AsyncMock(
                        return_value=SimpleNamespace(
                            status_code=200, headers={}, content=b"", text=""
                        )
                    ),
                    close=AsyncMock(),
                )
            )
            save_calls = []
            original_save = gemini_session_mod.save_session_async

            async def counting_save(target_path, record):
                save_calls.append(dict(record))
                await original_save(target_path, record)

            session = GeminiSession(path)
            session.http = http
            session.verified = True
            session.token = "tok-1"
            session.sid = "sid-1"
            try:
                with patch.object(
                    gemini_session_mod, "save_session_async", side_effect=counting_save
                ):
                    await session.persist(force=True)
                    self.assertEqual(len(save_calls), 1)
                    self.assertFalse(session._dirty)

                    cookie_state["NID"] = "n2"
                    await session.batch([RPCPayload("ESY5D", "[]")])
                    self.assertEqual(len(save_calls), 1)
                    self.assertTrue(session._dirty)

                    cookie_state["__Secure-1PSID"] = "p2"
                    await session.batch([RPCPayload("ESY5D", "[]")])
                    self.assertEqual(len(save_calls), 2)
                    self.assertFalse(session._dirty)

                    cookie_state["NID"] = "n3"
                    await session.batch([RPCPayload("ESY5D", "[]")])
                    self.assertEqual(len(save_calls), 2)
                    self.assertTrue(session._dirty)

                    await session.close()
                    self.assertEqual(len(save_calls), 3)
                    self.assertFalse(session._dirty)
                    self.assertEqual(save_calls[-1]["NID"], "n3")
            finally:
                await session.close()

    async def test_project_context_cache_reuses_and_invalidates_on_mutations_delete_and_close(
        self,
    ):
        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused")))
        detail = [None] * 13
        detail[12] = [1, None, None, None, 0]
        provider._connect = AsyncMock(return_value=SimpleNamespace(verified=True))
        provider.notebooks.get = AsyncMock(
            return_value=SimpleNamespace(metadata={"native": ["notebooks/abcdef", detail]})
        )
        provider.rpc.call = AsyncMock(return_value=[])
        provider._get_project_data = AsyncMock(return_value="confirmed")
        try:
            first = await provider._resolve_project_context("notebooks/abcdef", None)
            second = await provider._resolve_project_context("notebooks/abcdef", None)
            self.assertEqual(first, ("notebooks/abcdef", [1, None, None, None, 0]))
            self.assertEqual(second, ("notebooks/abcdef", [1, None, None, None, 0]))
            self.assertEqual(provider.notebooks.get.await_count, 1)

            await provider._project_after_write("notebooks/abcdef", lambda _p: True)
            self.assertIsNone(provider._project_contexts.get("notebooks/abcdef"))

            await provider._resolve_project_context("notebooks/abcdef", None)
            self.assertEqual(provider.notebooks.get.await_count, 2)

            await provider._delete_project_data("notebooks/abcdef")
            self.assertIsNone(provider._project_contexts.get("notebooks/abcdef"))

            await provider._resolve_project_context("notebooks/abcdef", None)
            self.assertEqual(provider.notebooks.get.await_count, 3)

            await provider.close()
            self.assertIsNone(provider._project_contexts.get("notebooks/abcdef"))
        finally:
            await provider.close()

    async def test_delete_chat_skips_redundant_readback_and_preserves_metadata_on_error(
        self,
    ):
        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused")))
        provider._connect = AsyncMock(return_value=SimpleNamespace(verified=True))
        provider._chat_metadata.put("cid-1", ["cid-1", "r1", "rc1"])

        try:
            provider.rpc.call = AsyncMock(return_value=None)
            await provider._delete_chat_data("cid-1")
            provider.rpc.call.assert_awaited_once_with(
                "GzXR5e", ["cid-1"], "/app", allow_empty=True
            )
            self.assertIsNone(provider._chat_metadata.get("cid-1"))
            self.assertFalse(provider._gate.locked())

            provider._chat_metadata.put("cid-2", ["cid-2", "r2", "rc2"])
            provider.rpc.call = AsyncMock(side_effect=ProviderError("delete failed"))
            with self.assertRaises(ProviderError):
                await provider._delete_chat_data("cid-2")
            self.assertEqual(provider._chat_metadata.get("cid-2"), ["cid-2", "r2", "rc2"])
        finally:
            await provider.close()

    async def test_upload_files_bounds_concurrency_to_two_and_preserves_order(self):
        active = 0
        max_active = 0

        class FakeMime:
            def __init__(self):
                self.filename = ""

            def addpart(self, *, filename, **_kwargs):
                self.filename = filename

            def close(self):
                return None

        async def fake_send(_method, _path, *, multipart, **_kwargs):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            try:
                if multipart.filename == "f0.txt":
                    await asyncio.sleep(0)
                return SimpleNamespace(text=f"uri-{multipart.filename}")
            finally:
                active -= 1

        session = SimpleNamespace(
            push_id="push-1",
            upload_transport=SimpleNamespace(send=fake_send),
        )
        items = [
            {"bytes": b"data", "name": f"f{i}.txt", "mime_type": "text/plain"} for i in range(4)
        ]
        with patch("curl_cffi.CurlMime", FakeMime):
            uploaded = await upload_files(session, items)

        self.assertEqual(max_active, 2)
        self.assertEqual(
            uploaded,
            [
                [["uri-f0.txt"], "f0.txt"],
                [["uri-f1.txt"], "f1.txt"],
                [["uri-f2.txt"], "f2.txt"],
                [["uri-f3.txt"], "f3.txt"],
            ],
        )

    async def test_notebook_editor_update_reuses_current_snapshot_and_skips_get(self):
        from webaiprovider.providers.gemini.projects import GeminiProjectEditor

        detail = [None] * 13
        detail[12] = [2, None, None, None, 1]
        current = SimpleNamespace(
            name="Old Name",
            instructions="Old inst",
            metadata={"native": ["notebooks/abcdef", detail], "description": ""},
        )
        rpc = SimpleNamespace(call=AsyncMock(return_value=True))
        notebooks = SimpleNamespace(get=AsyncMock())
        await GeminiProjectEditor(rpc, notebooks).update(
            "notebooks/abcdef", name="New Name", current=current
        )
        notebooks.get.assert_not_awaited()
        rpc.call.assert_awaited_once()

    async def test_notebook_editor_returns_project_data_for_update_and_remove_source(self):
        from webaiprovider.providers.gemini.projects import GeminiProjectEditor
        from webaiprovider.resources.message.attachment import Attachment
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        detail = [None] * 13
        detail[12] = [2, None, None, None, 1]
        source_id = "notebooks/abcdef/sources/src-1"
        current = NativeProject(
            "notebooks/abcdef",
            "Old Name",
            "Old inst",
            resources=(Attachment(source_id, "Note", "text/plain"),),
            metadata={"native": ["notebooks/abcdef", detail], "description": ""},
        )
        rpc = SimpleNamespace(call=AsyncMock(return_value=[]))
        notebooks = SimpleNamespace(get=AsyncMock())
        editor = GeminiProjectEditor(rpc, notebooks)

        updated = await editor.update("notebooks/abcdef", name="New Name", current=current)
        self.assertIsInstance(updated, NativeProject)
        self.assertEqual(updated.name, "New Name")
        self.assertEqual(updated.instructions, "Old inst")

        removed = await editor.remove_source(
            "notebooks/abcdef",
            current=updated,
            resource_id=source_id,
            resource=updated.resources[0],
        )
        self.assertIsInstance(removed, NativeProject)
        self.assertEqual(removed.resources, ())
        notebooks.get.assert_not_awaited()

    async def test_stream_normalizes_deduplicated_sources_and_files_once_at_metadata(self):
        from webaiprovider.providers.gemini import provider as gemini_provider_mod
        from webaiprovider.resources.message.protocol import MessageMetadata

        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused")))
        client = SimpleNamespace()
        request = ProviderRequest("Q", conversation_id="cid-1")
        provider._prepare_send_context = AsyncMock(
            return_value=(client, request, ["cid-1", "r0", "rc0"], None, [])
        )
        img1 = {
            "alias": "img-1",
            "name": "one.png",
            "url": "https://example.com/1.png",
            "media_type": "image",
        }
        img2 = {
            "alias": "img-2",
            "name": "two.png",
            "url": "https://example.com/2.png",
            "media_type": "image",
        }
        src1 = {"title": "First", "url": "https://example.com/s1"}
        chunks = [
            {
                "cid": "cid-1",
                "metadata": ["cid-1", "r1", "rc1"],
                "text": "part 1",
                "thoughts": "",
                "files": [img1],
                "sources": [src1],
            },
            {
                "cid": "cid-1",
                "metadata": ["cid-1", "r1", "rc1"],
                "text": "part 2",
                "thoughts": "",
                "files": [img1, img2],
                "sources": [src1],
                "response_complete": True,
            },
        ]

        async def fake_generate(*_args, **_kwargs):
            for chunk in chunks:
                yield chunk

        try:
            with (
                patch.object(gemini_provider_mod.generation, "generate", side_effect=fake_generate),
                patch.object(
                    gemini_provider_mod, "sources_from", wraps=gemini_provider_mod.sources_from
                ) as norm_sources,
                patch.object(
                    gemini_provider_mod, "files_from", wraps=gemini_provider_mod.files_from
                ) as norm_files,
            ):
                events = [e async for e in provider._stream(request)]
            meta = next(e for e in events if isinstance(e, MessageMetadata))
            self.assertEqual(norm_sources.call_count, 1)
            self.assertEqual(norm_files.call_count, 1)
            self.assertEqual(len(meta.sources), 1)
            self.assertEqual([f["name"] for f in meta.files], ["one.png", "two.png"])
        finally:
            await provider.close()

    async def test_session_flushes_psidcc_immediately_and_enforces_429_cooldown(self):
        from webaiprovider.accounts.credentials import save_session
        from webaiprovider.errors import RateLimited
        from webaiprovider.providers.gemini import session as gemini_session_mod
        from webaiprovider.providers.gemini.rpc import RPCPayload

        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "gemini.json"
            save_session(
                path,
                {
                    "__Secure-1PSID": "p1",
                    "__Secure-1PSIDCC": "cc1",
                    "SIDCC": "s1",
                },
            )
            cookie_state = {
                "__Secure-1PSID": "p1",
                "__Secure-1PSIDCC": "cc1",
                "SIDCC": "s1",
            }
            responses = iter(
                [
                    SimpleNamespace(status_code=200, headers={}, content=b"", text=""),
                    SimpleNamespace(
                        status_code=429,
                        headers={"retry-after": "30"},
                        content=b"",
                        text="",
                    ),
                ]
            )
            post = AsyncMock(side_effect=lambda *_args, **_kwargs: next(responses))
            http = _enable_streaming_requests(
                SimpleNamespace(
                    cookies=SimpleNamespace(get_dict=lambda: dict(cookie_state)),
                    post=post,
                    close=AsyncMock(),
                )
            )
            save_calls = []
            original_save = gemini_session_mod.save_session_async

            async def counting_save(target_path, record):
                save_calls.append(dict(record))
                await original_save(target_path, record)

            session = GeminiSession(path)
            session.http = http
            session.verified = True
            session.token = "tok-1"
            session.sid = "sid-1"
            try:
                with patch.object(
                    gemini_session_mod, "save_session_async", side_effect=counting_save
                ):
                    await session.persist(force=True)
                    cookie_state["__Secure-1PSIDCC"] = "cc2"
                    await session.batch([RPCPayload("ESY5D", "[]")])
                    self.assertEqual(len(save_calls), 2)
                    self.assertFalse(session._dirty)

                with self.assertRaises(RateLimited):
                    await session.batch([RPCPayload("ESY5D", "[]")])
                self.assertEqual(post.await_count, 2)
                with self.assertRaises(RateLimited):
                    await session.batch([RPCPayload("ESY5D", "[]")])
                self.assertEqual(post.await_count, 2)
            finally:
                await session.close()

    def test_rpc_malformed_json_raises_gemini_compatibility_error(self):
        from webaiprovider.providers.gemini.errors import GeminiCompatibilityError
        from webaiprovider.providers.gemini.rpc import GeminiRPC, rpc_parts

        with self.assertRaises(GeminiCompatibilityError):
            list(rpc_parts('[["wrb.fr","ESY5D","{bad-json"]]', "ESY5D"))
        with self.assertRaises(GeminiCompatibilityError):
            list(rpc_parts('[["wrb.fr", bad]', "ESY5D"))

        bad_response = SimpleNamespace(text='[["wrb.fr","CNgdBe","{not-json"]]')
        with self.assertRaises(GeminiCompatibilityError):
            GeminiRPC._decode(bad_response, "CNgdBe", False)

    async def test_get_chat_warms_metadata_and_prepare_send_runs_rpcs_concurrently(self):
        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused")))
        turn = [["c_warm", "r_warm"], None, [["hi"]], [[["rc_warm", ["hello"]]]]]
        client = SimpleNamespace(models=(Model("m1", "Model 1"),), sync_activity=None)
        started = {
            "context": asyncio.Event(),
            "history": asyncio.Event(),
            "activity": asyncio.Event(),
        }

        async def fake_context(_project_id, _timing):
            started["context"].set()
            await asyncio.wait_for(
                asyncio.gather(started["history"].wait(), started["activity"].wait()), 0.5
            )
            return ("notebooks/abcdef", [1, None, None, None, 0])

        async def fake_rpc_call(rpc, *_args, **_kwargs):
            if rpc == "hNvQHb":
                if started["context"].is_set():
                    started["history"].set()
                    await asyncio.wait_for(
                        asyncio.gather(started["context"].wait(), started["activity"].wait()), 0.5
                    )
                return [[[turn]]]
            raise AssertionError(rpc)

        async def fake_sync():
            started["activity"].set()
            await asyncio.wait_for(
                asyncio.gather(started["context"].wait(), started["history"].wait()), 0.5
            )

        client.sync_activity = fake_sync
        provider._connect = AsyncMock(return_value=client)
        provider.rpc.call = AsyncMock(side_effect=fake_rpc_call)
        try:
            chat = await provider._get_chat_data("c_warm")
            self.assertEqual(chat.id, "c_warm")
            self.assertEqual(
                provider._chat_metadata.get("c_warm")[:3], ["c_warm", "r_warm", "rc_warm"]
            )

            provider._resolve_project_context = fake_context
            # Clear cache for c_uncached so hNvQHb actually runs in parallel with context & activity.
            _, req, meta, ctx, _ = await provider._prepare_send_context(
                ProviderRequest("next", conversation_id="c_uncached", project_id="notebooks/abcdef")
            )
            self.assertEqual(req.conversation_id, "c_uncached")
            self.assertEqual(meta[:3], ["c_uncached", "r_warm", "rc_warm"])
            self.assertEqual(ctx[0], "notebooks/abcdef")
        finally:
            await provider.close()

    async def test_notebook_get_raises_remote_not_found_when_server_returns_empty_or_null(self):
        from webaiprovider.errors import RemoteNotFound
        from webaiprovider.providers.gemini.notebooks import Notebooks
        from webaiprovider.providers.gemini.rpc import GeminiRPC

        client = SimpleNamespace(
            batch=AsyncMock(return_value=SimpleNamespace(text='[["wrb.fr","HcT8bb",null]]'))
        )
        rpc = GeminiRPC(AsyncMock(return_value=client))
        notebooks = Notebooks("acc")
        notebooks.reader = rpc.read

        with self.assertRaises(RemoteNotFound):
            await notebooks.get("notebooks/abcdef")

    async def test_stream_emits_deltas_on_appends_and_replacement_on_rewrite(self):
        provider = AsyncGeminiProvider(provider_account("gemini"))
        client = SimpleNamespace(models=("gemini-2.5-flash",), sync_activity=AsyncMock())

        async def fake_prepare(req):
            return client, req, ["", "", ""], None, None

        async def fake_generate(*_args, **_kwargs):
            yield {
                "cid": "c_stream",
                "metadata": ["c_stream", "r1", "rc1"],
                "thoughts": "Step 1",
                "text": "Hel",
                "files": [],
                "sources": [],
                "response_complete": False,
            }
            yield {
                "cid": "c_stream",
                "metadata": ["c_stream", "r1", "rc1"],
                "thoughts": "Step 1 and 2",
                "text": "Hello",
                "files": [],
                "sources": [],
                "response_complete": False,
            }
            yield {
                "cid": "c_stream",
                "metadata": ["c_stream", "r1", "rc1"],
                "thoughts": "Step 1 and 2",
                "text": "Revised Hello ![img](http://googleusercontent.com/image_generation_content/img1)",
                "files": [
                    {
                        "alias": "http://googleusercontent.com/image_generation_content/img1",
                        "name": "img.png",
                        "url": "https://lh3.googleusercontent.com/resolved",
                        "media_type": "image/png",
                    }
                ],
                "sources": [],
                "response_complete": True,
            }

        provider._prepare_send_context = fake_prepare
        with patch("webaiprovider.providers.gemini.generation.generate", fake_generate):
            events = [
                event async for event in provider._stream_provider_events(ProviderRequest("hi"))
            ]

        self.assertIn(TextDelta("Step 1", "analysis"), events)
        self.assertIn(TextDelta(" and 2", "analysis"), events)
        self.assertIn(TextDelta("Hel", "final"), events)
        self.assertIn(TextDelta("lo", "final"), events)
        self.assertIn(
            TextReplacement(
                "Revised Hello ![img](https://lh3.googleusercontent.com/resolved)", "final"
            ),
            events,
        )


# History and deletion verification
def message(role, text, **extra):
    return {"role": role, "text": text, **extra}


class GeminiHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_listing_reads_remote_pages_and_returns_real_pagination(self):
        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused")))
        provider._connect = AsyncMock()
        provider.rpc.call = AsyncMock(
            return_value=[[None, None, [["c_" + str(i), str(i)] for i in range(5)]]]
        )
        first = await provider._list_chats_data(limit=2)
        second = await provider._list_chats_data(cursor=first.next_cursor, limit=2)
        self.assertEqual([x.title for x in first.items], ["0", "1"])
        self.assertEqual([x.title for x in second.items], ["2", "3"])
        self.assertEqual(second.next_cursor, "4")
        self.assertEqual(provider.rpc.call.await_count, 2)

    async def test_bounded_listing_uses_larger_followup_pages(self):
        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused")))
        provider._prepare_session = AsyncMock()
        provider._connect = AsyncMock()
        rows = [[f"c_{index}", str(index)] for index in range(260)]
        provider.rpc.call = AsyncMock(
            side_effect=lambda _method, args, _path: [[None, None, rows[: args[0]]]]
        )

        async with provider:
            for limit, expected_prefixes in ((10, [51]), (250, [51, 151, 251])):
                with self.subTest(limit=limit):
                    provider.rpc.call.reset_mock()
                    chats = await provider.chats.list(limit=limit)
                    self.assertEqual(
                        [chat.remote_id for chat in chats], [r[0] for r in rows[:limit]]
                    )
                    self.assertEqual(
                        [call.args[1][0] for call in provider.rpc.call.await_args_list],
                        expected_prefixes,
                    )

    async def test_retry_validates_history_identity_and_parses_only_the_target_turn(self):
        from webaiprovider.providers.gemini import history
        from webaiprovider.providers.gemini.errors import GeminiCompatibilityError

        turns = [
            [["c_chat", rid], None, [[f"Prompt {rid}"]], [[[f"candidate-{rid}", ["Answer"]]]]]
            for rid in ("r_new", "r_target", "r_parent")
        ]
        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused")))
        provider.rpc.call = AsyncMock(return_value=[[turns]])
        request = ProviderRequest(
            "",
            conversation_id="c_chat",
            action=MessageAction.RETRY,
            message_id="r_target:model",
        )

        with patch.object(history, "_turn_messages", wraps=history._turn_messages) as parse:
            updated, metadata, _ = await provider._load_chat_continuation(request, None)
        parse.assert_called_once_with(turns[1], "r_target")
        self.assertEqual(updated.prompt, "Prompt r_target")
        self.assertEqual(updated.action, MessageAction.EDIT)
        self.assertEqual(metadata[1:3], ["r_parent", "candidate-r_parent"])
        self.assertEqual(provider.rpc.call.await_count, 1)

        for invalid, reason in (
            (turns + [turns[1]], "duplicate target"),
            (turns + [[["c_chat", None]]], "missing a response ID"),
        ):
            provider.rpc.call.return_value = [[invalid]]
            with self.assertRaisesRegex(GeminiCompatibilityError, reason):
                await provider._load_chat_continuation(request, None)

    async def test_message_ids_do_not_shift_when_older_history_arrives(self):
        def turn(rid):
            return [["c_abc", rid], None, [["question"]], [[["candidate", ["answer"]]]]]

        provider = AsyncGeminiProvider(provider_account("gemini", Path("unused")))
        provider.rpc.call = AsyncMock(return_value=[[[turn("r_stable")]]])
        first = await provider._get_chat_data("c_abc")
        provider.rpc.call.return_value = [[[turn("r_stable"), turn("r_old")]]]
        second = await provider._get_chat_data("c_abc")
        self.assertEqual([m.id for m in first.messages], [m.id for m in second.messages[-2:]])


class NotebookDeleteVerificationTests(unittest.IsolatedAsyncioTestCase):
    async def test_delete_project_skips_redundant_readback_after_write(self):
        notebooks = SimpleNamespace(get=AsyncMock(), list=AsyncMock())
        provider = AsyncGeminiProvider(
            provider_account("gemini", Path("unused")), notebooks=notebooks
        )
        with (
            patch.object(provider.editor, "delete", new_callable=AsyncMock) as delete,
            patch("webaiprovider.base.asyncio.sleep", new_callable=AsyncMock) as wait,
        ):
            await provider._delete_project_data("notebooks/abcdef")
            delete.assert_awaited_once_with("notebooks/abcdef")
            wait.assert_not_awaited()
        notebooks.get.assert_not_awaited()
        notebooks.list.assert_not_awaited()


# Notebook contracts
class NotebookTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_notebook_details_and_resources_are_normalized(self):
        service = Notebooks("account")
        service.read = AsyncMock(return_value=[[None, None, [notebook()]]])
        page = await service.list()
        project = page.items[0]
        self.assertEqual(project.id, "notebooks/abcdef")
        self.assertEqual(project.name, "My notebook")
        self.assertEqual(project.instructions, "Use citations")
        self.assertEqual(project.metadata["description"], "Notebook description")
        self.assertEqual(project.resources[0].name, "Research notes")
        self.assertEqual(project.metadata["kind"], "notebook")
        self.assertEqual(project.metadata["notebook_type"], 3)
        self.assertTrue(project.metadata["url"].startswith("https://gemini.google.com/"))
        service.read.return_value = [[notebook()]]
        self.assertEqual(await service.get("notebooks/abcdef"), project)

    async def test_unknown_native_contract_fails_without_inventing_data(self):
        with self.assertRaises(ProviderError):
            project_from_native(["wrong-id", []], "account")
        service = Notebooks("account")
        service.read = AsyncMock(return_value=[[]])
        with self.assertRaises(ProviderError):
            await service.get("notebooks/missing")
