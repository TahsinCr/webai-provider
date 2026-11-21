"""Saved web sessions: no browser processes, redirects or mutation retries."""

import asyncio
import json
import os
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import AsyncMock, Mock, patch

from tests.fakes import FakeCookiesJar, FakeHTTPResponse, bind_stream
from tests.provider_fakes import claude_model_catalog
from webaiprovider.accounts.credentials import async_save_session, read_session, save_session
from webaiprovider.errors import AccountPaused, ProviderError, RateLimited, RemoteNotFound
from webaiprovider.providers.claude.async_client import (
    AsyncClaudeClient as ClaudeClient,
)
from webaiprovider.providers.claude.async_client import (
    AsyncClaudeProjectEditor as ClaudeProjectEditor,
)
from webaiprovider.providers.claude.base import organization_from_bootstrap
from webaiprovider.providers.claude.errors import ClaudeRequestNotSent
from webaiprovider.providers.gemini.async_client import (
    AsyncGeminiProjectEditor as GeminiProjectEditor,
)
from webaiprovider.providers.gemini.async_client import (
    AsyncGeminiRPC as GeminiRPC,
)
from webaiprovider.transport import AsyncHttpSession, HttpSession


class BlockingSessionTests(unittest.TestCase):
    def test_saved_account_http_request_runs_on_caller_and_persists_cookies(self):
        from curl_cffi.requests import Session

        caller = threading.get_ident()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "session.json"
            save_session(path, {"sessionKey": "fixture"})

            def request(client, method, url, *, allow_redirects, content_callback, **options):
                self.assertEqual(threading.get_ident(), caller)
                self.assertFalse(allow_redirects)
                self.assertEqual(client.cookies.get("sessionKey"), "fixture")
                self.assertEqual(client.impersonate, "chrome")
                self.assertEqual(client.headers["origin"], "https://claude.ai")
                client.cookies.set("sessionKey", "rotated", domain="claude.ai")
                content_callback(b'{"ok":true}')
                return FakeHTTPResponse(200)

            with (
                patch.object(Session, "request", autospec=True, side_effect=request) as send,
                patch("webaiprovider.transport.http._prepare_curl_session"),
                HttpSession("https://claude.ai", path, "sessionKey") as session,
            ):
                with self.assertRaises(ValueError):
                    session.request("GET", "https://example.test/")
                send.assert_not_called()
                self.assertEqual(session.request("GET", "/fixture"), {"ok": True})
                self.assertEqual(read_session(path)["sessionKey"], "rotated")
                self.assertTrue(session.authenticated)
                send.assert_called_once()


class SessionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "session.json"
        save_session(self.path, {"sessionKey": "fixture"})
        self.client = SimpleNamespace(
            request=AsyncMock(return_value=FakeHTTPResponse(200, json={"ok": True})),
            cookies={"sessionKey": "rotated"},
            close=AsyncMock(),
        )
        self.factory = Mock(return_value=self.client)
        self.session = AsyncHttpSession(
            "https://claude.ai", self.path, "sessionKey", client_factory=self.factory
        )
        self.addAsyncCleanup(self.session.close)

    async def test_http_request_reuses_connection_without_browser(self):
        with patch("asyncio.create_subprocess_exec", side_effect=AssertionError("browser")):
            await self.session.request("GET", "/api/bootstrap")
            await self.session.request("POST", "/api/projects", json={"name": "fixture"})
        self.assertEqual(self.factory.call_count, 1)
        self.assertFalse(self.client.request.call_args.kwargs["allow_redirects"])
        self.assertEqual(read_session(self.path)["sessionKey"], "rotated")
        if os.name != "nt":
            self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    async def test_session_cannot_send_cookies_to_another_origin(self):
        for path in ["https://example.com/api", "//example.com/api", "/../outside"]:
            with self.assertRaises(ValueError):
                await self.session.request("POST", path)
        self.factory.assert_not_called()

    async def test_remote_mutation_failure_is_not_replayed(self):
        self.client.request.return_value = FakeHTTPResponse(403, json={"error": "denied"})
        with self.assertRaises(ProviderError) as error:
            await self.session.request("POST", "/api/project")
        self.assertNotIsInstance(error.exception, AccountPaused)
        self.client.request.assert_awaited_once()

    async def test_local_persistence_failure_does_not_reject_remote_success(self):
        with patch(
            "webaiprovider.accounts.credentials.save_session", side_effect=OSError("disk full")
        ):
            self.assertEqual(await self.session.request("POST", "/api/project"), {"ok": True})
        self.assertTrue(self.session.persistence_warning)

    async def test_large_json_is_parsed_off_the_provider_loop(self):
        loop_thread = threading.get_ident()
        self.client.request.return_value = SimpleNamespace(
            status_code=200,
            content=b" " * (256 * 1024),
            json=lambda: {"thread": threading.get_ident()},
        )
        value = await self.session.request("GET", "/api/history")
        self.assertNotEqual(value["thread"], loop_thread)

    async def test_cancelled_persistence_finishes_before_returning(self):
        started, release = threading.Event(), threading.Event()

        def write(*_args):
            started.set()
            if not release.wait(2):
                raise TimeoutError("write not released")

        with patch("webaiprovider.accounts.credentials.save_session", side_effect=write):
            task = asyncio.create_task(async_save_session(self.path, {}))
            self.assertTrue(await asyncio.to_thread(started.wait, 2))
            task.cancel()
            await asyncio.sleep(0.01)
            self.assertFalse(task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task

    def test_auth_access_missing_and_limit_are_different_failures(self):
        for status, kind in [
            (401, AccountPaused),
            (403, ProviderError),
            (404, RemoteNotFound),
            (429, RateLimited),
        ]:
            with self.subTest(status=status), self.assertRaises(kind):
                AsyncHttpSession.check(FakeHTTPResponse(status, headers={"retry-after": "2"}))

    def test_rate_limit_accepts_http_date_retry_after(self):
        now = datetime(2030, 1, 1, tzinfo=timezone.utc)
        retry_at = now + timedelta(seconds=30)
        response = FakeHTTPResponse(
            429, headers={"retry-after": format_datetime(retry_at, usegmt=True)}
        )
        with (
            patch("webaiprovider.transport.base.time.time", return_value=now.timestamp()),
            self.assertRaises(RateLimited) as error,
        ):
            AsyncHttpSession.check(response)
        self.assertEqual(error.exception.retry_after, 30)

        response = FakeHTTPResponse(429, headers={"retry-after": "not-a-date"})
        with self.assertRaises(RateLimited) as error:
            AsyncHttpSession.check(response)
        self.assertIsNone(error.exception.retry_after)

    def test_invalid_credentials_are_rejected(self):
        self.path.write_text("[]")
        with self.assertRaises(AccountPaused):
            read_session(self.path)


class NotebookHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_rpc_keeps_native_identity_and_disables_automatic_retry(self):
        client = SimpleNamespace(
            batch=AsyncMock(
                return_value=SimpleNamespace(text=')]}\'\n[["wrb.fr","HcT8bb","[[]]"]]')
            )
        )
        rpc = GeminiRPC(AsyncMock(return_value=client))
        self.assertEqual(await rpc.read("notebooks", remote="notebooks/abcdef"), [[[]]])
        args = client.batch.call_args
        self.assertEqual(json.loads(args.args[0][0].payload), ["notebooks/abcdef", ["en"], 0])

    async def test_missing_rpc_ack_cannot_be_reported_as_success(self):
        client = SimpleNamespace(
            batch=AsyncMock(return_value=SimpleNamespace(text='[["wrb.fr","HcT8bb",null]]'))
        )
        with self.assertRaises(ProviderError):
            await GeminiRPC(AsyncMock(return_value=client)).call("HcT8bb", [])

    async def test_notebook_update_uses_write_settings_not_display_icon(self):
        detail = [None] * 13
        detail[6] = ["icon"]
        detail[12] = [2, None, None, None, 1]
        notebook = SimpleNamespace(
            name="Name", instructions="", metadata={"native": ["notebooks/abcdef", detail]}
        )
        rpc = SimpleNamespace(call=AsyncMock())
        editor = GeminiProjectEditor(rpc, SimpleNamespace(get=AsyncMock(return_value=notebook)))
        await editor.update("notebooks/abcdef", instructions="Instructions")
        payload = rpc.call.call_args.args[1][1]
        self.assertEqual(payload[12], [None, None, 0, 1, 0])
        self.assertIsNone(payload[18])
        self.assertEqual(payload[16], detail[12])

    async def test_cannot_delete_a_source_from_another_notebook(self):
        rpc = SimpleNamespace(call=AsyncMock())
        editor = GeminiProjectEditor(rpc, None)
        with self.assertRaises(ValueError):
            await editor.remove_source(
                "notebooks/abcdef", resource_id="notebooks/another/sources/a"
            )
        rpc.call.assert_not_awaited()


class ClaudeHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_temporary_continuation_checks_remote_state_even_with_parent_hint(self):
        cid = "11111111-1111-1111-1111-111111111111"
        session = SimpleNamespace(
            path=Path("unused"),
            request=AsyncMock(return_value={"is_temporary": False}),
            connect=Mock(),
        )
        transport = ClaudeClient("unused", session=bind_stream(session))
        transport.base = AsyncMock(return_value="/api/organizations/account")
        transport.catalog = claude_model_catalog()
        with self.assertRaises(ClaudeRequestNotSent):
            _ = [
                event
                async for event in transport.events(
                    "send",
                    conversation_id=cid,
                    parent_message_uuid=cid,
                    temporary=True,
                    prompt="Question",
                    model="model",
                )
            ]
        session.request.assert_awaited_once()
        session.connect.assert_not_called()

    async def test_failed_completion_is_not_retried_with_a_parent_hint(self):
        cid = "11111111-1111-1111-1111-111111111111"
        response = SimpleNamespace(status_code=403)
        context = AsyncMock()
        context.__aenter__.return_value = response
        context.__aexit__.return_value = False
        stream = Mock(return_value=context)
        session = SimpleNamespace(
            path=Path("unused"),
            request=AsyncMock(return_value={"current_leaf_message_uuid": cid}),
            connect=Mock(return_value=SimpleNamespace(stream=stream)),
            check=Mock(side_effect=ProviderError("denied")),
            url=lambda value: value,
        )
        transport = ClaudeClient("unused", session=bind_stream(session))
        transport.base = AsyncMock(return_value="/api/organizations/account")
        transport.catalog = claude_model_catalog()
        with self.assertRaises(ProviderError):
            _ = [
                event
                async for event in transport.events(
                    "send",
                    conversation_id=cid,
                    parent_message_uuid=cid,
                    prompt="Question",
                    model="model",
                )
            ]
        stream.assert_called_once()
        context.__aexit__.assert_awaited_once()

    def test_catalog_uses_selected_workspace_and_filters_inactive_models(self):
        data = {
            "account": {
                "settings": {"default_model": "live"},
                "memberships": [
                    {
                        "organization": {
                            "uuid": "workspace",
                            "capabilities": ["chat"],
                            "claude_ai_bootstrap_models_config": [
                                {
                                    "model": "live",
                                    "name": "Live",
                                    "thinking_modes": [{"id": "auto", "title": "Thinking"}],
                                },
                                {"model": "old", "inactive": True},
                            ],
                        }
                    }
                ],
            }
        }
        org = organization_from_bootstrap(data, "workspace")
        self.assertEqual(org, "workspace")
        with self.assertRaises(ProviderError):
            organization_from_bootstrap(data, "other")

    async def test_first_discovery_is_shared_with_requests_during_warmup(self):
        organization = "11111111-1111-1111-1111-111111111111"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "session.json"
            save_session(path, {"cookies": {"lastActiveOrg": organization}})
            calls = []
            bootstrap_started = asyncio.Event()
            release_bootstrap = asyncio.Event()

            async def request(method, endpoint, **_kwargs):
                calls.append(endpoint)
                if endpoint == "/api/bootstrap":
                    bootstrap_started.set()
                    await release_bootstrap.wait()
                    return {
                        "account": {
                            "memberships": [
                                {
                                    "organization": {
                                        "uuid": organization,
                                        "capabilities": ["chat"],
                                    }
                                }
                            ]
                        }
                    }
                if endpoint.endswith("/model_selector/chat"):
                    return {
                        "model_selector_config": [
                            {"id": "chat", "models": [{"id": "model", "name": "Model"}]}
                        ],
                        "model_selector_state": [],
                    }
                if endpoint.endswith("/chat_conversations?limit=50&offset=0"):
                    return []
                raise AssertionError(endpoint)

            session = SimpleNamespace(
                path=path,
                request=AsyncMock(side_effect=request),
                close=AsyncMock(),
            )
            transport = ClaudeClient(path, session=bind_stream(session))
            self.addAsyncCleanup(transport.close)
            preparing = asyncio.create_task(transport.prepare())
            listing = asyncio.create_task(transport.request("chats", cursor=0, limit=50))
            await asyncio.wait_for(bootstrap_started.wait(), 1)
            await asyncio.sleep(0)
            self.assertEqual(calls, ["/api/bootstrap"])
            release_bootstrap.set()
            _, chats = await asyncio.gather(preparing, listing)

        self.assertEqual(chats, [])
        self.assertEqual(
            calls,
            [
                "/api/bootstrap",
                f"/api/organizations/{organization}/model_selector/chat",
                f"/api/organizations/{organization}/chat_conversations?limit=50&offset=0",
            ],
        )

    async def test_stale_saved_claude_workspace_uses_only_unambiguous_bootstrap_fallback(self):
        old_organization = "11111111-1111-1111-1111-111111111111"
        current_organization = "22222222-2222-2222-2222-222222222222"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "session.json"
            save_session(path, {"cookies": {"lastActiveOrg": old_organization}})
            calls = []

            async def request(method, endpoint, **_kwargs):
                calls.append(endpoint)
                if endpoint == "/api/bootstrap":
                    return {
                        "account": {
                            "memberships": [
                                {
                                    "organization": {
                                        "uuid": current_organization,
                                        "capabilities": ["chat"],
                                    }
                                }
                            ]
                        }
                    }
                if endpoint.endswith("/model_selector/chat"):
                    return {
                        "model_selector_config": [
                            {"id": "chat", "models": [{"id": "model", "name": "Model"}]}
                        ],
                        "model_selector_state": [],
                    }
                raise AssertionError(endpoint)

            session = SimpleNamespace(
                path=path,
                request=AsyncMock(side_effect=request),
                close=AsyncMock(),
            )
            transport = ClaudeClient(path, session=bind_stream(session))
            self.addAsyncCleanup(transport.close)
            await transport.prepare()

        self.assertEqual(transport.organization, current_organization)
        self.assertEqual(transport.catalog.default_model, "model")
        self.assertEqual(
            calls,
            ["/api/bootstrap", f"/api/organizations/{current_organization}/model_selector/chat"],
        )

    async def test_stale_claude_workspace_does_not_guess_when_account_has_multiple_workspaces(self):
        old_organization = "11111111-1111-1111-1111-111111111111"
        organizations = (
            "22222222-2222-2222-2222-222222222222",
            "33333333-3333-3333-3333-333333333333",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "session.json"
            save_session(path, {"cookies": {"lastActiveOrg": old_organization}})
            calls = []

            async def request(method, endpoint, **_kwargs):
                calls.append(endpoint)
                if endpoint == "/api/bootstrap":
                    return {
                        "account": {
                            "memberships": [
                                {
                                    "organization": {
                                        "uuid": organization,
                                        "capabilities": ["chat"],
                                    }
                                }
                                for organization in organizations
                            ]
                        }
                    }
                raise AssertionError(endpoint)

            session = SimpleNamespace(
                path=path,
                request=AsyncMock(side_effect=request),
                close=AsyncMock(),
            )
            transport = ClaudeClient(path, session=bind_stream(session))
            self.addAsyncCleanup(transport.close)
            with self.assertRaises(ProviderError):
                await transport.prepare()

        self.assertEqual(calls, ["/api/bootstrap"])

    async def test_discovery_waits_for_bootstrap_before_loading_catalog(self):
        organization = "11111111-1111-1111-1111-111111111111"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "session.json"
            save_session(path, {"cookies": {"lastActiveOrg": organization}})
            bootstrap_started = asyncio.Event()
            release_bootstrap = asyncio.Event()
            calls = []

            async def request(method, endpoint, **_kwargs):
                calls.append(endpoint)
                if endpoint == "/api/bootstrap":
                    bootstrap_started.set()
                    await release_bootstrap.wait()
                    return {
                        "account": {
                            "memberships": [
                                {
                                    "organization": {
                                        "uuid": organization,
                                        "capabilities": ["chat"],
                                    }
                                }
                            ]
                        }
                    }
                if endpoint.endswith("/model_selector/chat"):
                    return {
                        "model_selector_config": [
                            {"id": "chat", "models": [{"id": "model", "name": "Model"}]}
                        ],
                        "model_selector_state": [],
                    }
                raise AssertionError(endpoint)

            session = SimpleNamespace(
                path=path,
                request=AsyncMock(side_effect=request),
                close=AsyncMock(),
            )
            transport = ClaudeClient(path, session=bind_stream(session))
            self.addAsyncCleanup(transport.close)
            preparing = asyncio.create_task(transport.prepare())
            await bootstrap_started.wait()
            await asyncio.sleep(0)
            self.assertEqual(calls, ["/api/bootstrap"])
            release_bootstrap.set()
            await preparing

        self.assertEqual(
            calls,
            ["/api/bootstrap", f"/api/organizations/{organization}/model_selector/chat"],
        )

    async def test_model_catalog_expires_and_explicit_refresh_bypasses_cache(self):
        transport = ClaudeClient("unused")
        self.addAsyncCleanup(transport.close)

        async def discover():
            transport.organization = "org"
            transport.catalog = claude_model_catalog()
            transport._discovered_at = asyncio.get_running_loop().time()
            return transport.catalog

        with patch.object(transport, "_discover", side_effect=discover) as fetch:
            await asyncio.gather(transport.discover(), transport.discover())
            await transport.discover()
            self.assertEqual(fetch.call_count, 1)
            await transport.discover(force=True)
            self.assertEqual(fetch.call_count, 2)
            transport._discovered_at -= 301
            await transport.discover()
            self.assertEqual(fetch.call_count, 3)

    async def test_http_stream_preserves_published_thinking_and_end_marker(self):
        from webaiprovider.core.timing import SendTiming, use_send_timing

        cid = "11111111-1111-1111-1111-111111111111"
        events = [
            {"type": "content_block_start", "index": 0, "content_block": {"type": "thinking"}},
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "thinking_delta", "thinking": "Summary"},
            },
            {"type": "content_block_start", "index": 1, "content_block": {"type": "text"}},
            {
                "type": "content_block_delta",
                "index": 1,
                "delta": {"type": "text_delta", "text": "An"},
            },
            {
                "type": "content_block_delta",
                "index": 1,
                "delta": {"type": "text_delta", "text": "swer"},
            },
            {"type": "message_stop"},
        ]

        class Response:
            status_code = 200
            headers: ClassVar[dict[str, str]] = {}

            async def aiter_content(self):
                for e in events:
                    yield ("data: " + json.dumps(e) + "\n").encode()
                    yield ("" + "\n").encode()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

        session = SimpleNamespace(
            path=Path("unused"),
            request=AsyncMock(return_value={}),
            connect=Mock(return_value=SimpleNamespace(stream=Mock(return_value=Response()))),
            check=Mock(),
            url=lambda path: "https://claude.ai" + path,
        )
        transport = ClaudeClient(Path("unused"), session=bind_stream(session))
        transport.organization = cid
        transport.catalog = claude_model_catalog()
        timing = SendTiming("claude")
        with (
            patch("asyncio.create_subprocess_exec", side_effect=AssertionError("browser")),
            use_send_timing(timing),
        ):
            result = [e async for e in transport.events("send", prompt="Question", model="model")]
        self.assertTrue(any(e.get("channel") == "analysis" for e in result))
        final = [e for e in result if "text" in e and e.get("channel") != "analysis"]
        self.assertEqual("".join(e["text"] for e in final), "Answer")
        self.assertTrue(all(e["type"] == "delta" for e in final))
        types = [event["type"] for event in result]
        self.assertLess(types.index("delta"), types.index("response_complete"))
        self.assertLess(types.index("response_complete"), types.index("done"))
        self.assertEqual(session.request.await_count, 1)
        durations, _counts = timing.snapshot()
        self.assertTrue(
            {
                "provider_client_ready_ms",
                "sse_connect_ms",
                "sse_first_event_ms",
                "sse_first_assistant_text_ms",
                "sse_last_event_ms",
            }.issubset(durations)
        )
        self.assertGreaterEqual(durations["sse_last_event_ms"], durations["sse_first_event_ms"])


class ClaudeFileDeletionTests(unittest.IsolatedAsyncioTestCase):
    async def test_uploaded_files_use_native_file_uuid_and_documents_stay_separate(self):
        pid = "11111111-1111-1111-1111-111111111111"
        transport = SimpleNamespace(
            base=AsyncMock(return_value="/api/organizations/org"),
            request=AsyncMock(
                return_value={
                    "docs": [],
                    "files": [{"uuid": "resource", "file_uuid": "native-file"}],
                }
            ),
            http=SimpleNamespace(request=AsyncMock()),
        )
        editor = ClaudeProjectEditor(transport)
        await editor.remove_source(pid, resource_id="resource")
        transport.http.request.assert_awaited_once_with(
            "POST",
            f"/api/organizations/org/projects/{pid}/files/delete_many",
            json={"file_uuids": ["native-file"]},
        )
        transport.http.request.reset_mock()
        transport.request.return_value = {"docs": [{"uuid": "document"}], "files": []}
        await editor.remove_source(pid, resource_id="document")
        transport.http.request.assert_awaited_once_with(
            "DELETE", f"/api/organizations/org/projects/{pid}/docs/document"
        )

    async def test_deleted_foreign_source_cannot_be_submitted(self):
        transport = SimpleNamespace(
            base=AsyncMock(return_value="/api/organizations/org"),
            request=AsyncMock(return_value={"docs": [], "files": []}),
            http=SimpleNamespace(request=AsyncMock()),
        )
        with self.assertRaises(ValueError):
            await ClaudeProjectEditor(transport).remove_source(
                "11111111-1111-1111-1111-111111111111", resource_id="foreign"
            )
        transport.http.request.assert_not_awaited()


class DebouncedPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_secondary_cookies_debounce_until_close_while_required_cookie_persists_immediately(
        self,
    ):
        from webaiprovider.transport import async_session as session_module
        from webaiprovider.transport.cookie import _SECONDARY_PERSIST_INTERVAL

        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "session.json"
            save_session(path, {"sessionKey": "token-1", "cf_bm": "v1"})
            jar = FakeCookiesJar()
            jar.set("sessionKey", "token-1", domain="claude.ai", path="/")
            jar.set("cf_bm", "v1", domain="claude.ai", path="/")
            steps = iter(
                [
                    ("cf_bm", "v2"),
                    ("cf_bm", "v3"),
                    ("sessionKey", "token-2"),
                    ("cf_bm", "v4"),
                    ("cf_bm", "v5"),
                    ("cf_bm", "v6"),
                ]
            )

            async def fake_request(*_args, **_kwargs):
                name, value = next(steps)
                jar.set(name, value, domain="claude.ai", path="/")
                return FakeHTTPResponse(200, json={"ok": True})

            fake_client = SimpleNamespace(
                cookies=jar,
                request=AsyncMock(side_effect=fake_request),
                close=AsyncMock(),
            )
            save_calls = []
            original_save = session_module.async_save_session

            async def counting_save(target_path, cookies):
                save_calls.append(dict(cookies))
                await original_save(target_path, cookies)

            http_session = AsyncHttpSession(
                "https://claude.ai",
                path,
                "sessionKey",
                client_factory=lambda **_: fake_client,
            )
            try:
                with patch.object(session_module, "async_save_session", side_effect=counting_save):
                    await http_session.send("GET", "/first")
                    self.assertEqual(len(save_calls), 0)
                    self.assertTrue(http_session._dirty)
                    self.assertEqual(read_session(path)["cf_bm"], "v1")

                    await http_session.send("GET", "/second")
                    self.assertEqual(len(save_calls), 0)
                    self.assertTrue(http_session._dirty)

                    await http_session.send("GET", "/third")
                    self.assertEqual(len(save_calls), 1)
                    self.assertFalse(http_session._dirty)
                    self.assertEqual(read_session(path), {"sessionKey": "token-2", "cf_bm": "v3"})

                    await http_session.send("GET", "/fourth")
                    self.assertEqual(len(save_calls), 1)
                    self.assertTrue(http_session._dirty)

                    http_session._last_persisted_at -= _SECONDARY_PERSIST_INTERVAL + 1
                    await http_session.send("GET", "/fifth")
                    self.assertEqual(len(save_calls), 2)
                    self.assertFalse(http_session._dirty)
                    self.assertEqual(read_session(path)["cf_bm"], "v5")

                    await http_session.send("GET", "/sixth")
                    self.assertEqual(len(save_calls), 2)
                    self.assertTrue(http_session._dirty)

                    await http_session.close()
                    self.assertEqual(len(save_calls), 3)
                    self.assertFalse(http_session._dirty)
                    self.assertEqual(read_session(path), {"sessionKey": "token-2", "cf_bm": "v6"})
            finally:
                await http_session.close()

    async def test_chunked_grok_and_cloudflare_challenge_cookies_persist_immediately(self):
        from webaiprovider.transport import async_session as session_module

        cases = [
            (
                "https://claude.ai",
                "sessionKey",
                {"sessionKey.0": "part-a", "sessionKey.1": "part-b", "misc": "1"},
                "sessionKey.0",
                "part-a2",
            ),
            (
                "https://grok.com",
                None,
                {"sso": "sso-1", "sso-rw": "rw-1", "cf_clearance": "cf-1"},
                "sso",
                "sso-2",
            ),
            (
                "https://grok.com",
                None,
                {"sso": "sso-1", "sso-rw": "rw-1", "__cf_bm": "bm-1"},
                "__cf_bm",
                "bm-2",
            ),
            (
                "https://claude.ai",
                "sessionKey",
                {"sessionKey": "k1", "cf_clearance": "cf-1"},
                "cf_clearance",
                "cf-2",
            ),
        ]
        for origin, required_cookie, initial, mutated_name, mutated_value in cases:
            with (
                self.subTest(origin=origin, mutated_name=mutated_name),
                tempfile.TemporaryDirectory() as folder,
            ):
                path = Path(folder) / "session.json"
                save_session(path, initial)
                domain = origin.removeprefix("https://")
                jar = FakeCookiesJar()
                for k, v in initial.items():
                    jar.set(k, v, domain=domain, path="/")

                async def fake_request(
                    *_args,
                    jar=jar,
                    domain=domain,
                    mutated_name=mutated_name,
                    mutated_value=mutated_value,
                    **_kwargs,
                ):
                    jar.set(mutated_name, mutated_value, domain=domain, path="/")
                    return FakeHTTPResponse(200, json={"ok": True})

                fake_client = SimpleNamespace(
                    cookies=jar,
                    request=AsyncMock(side_effect=fake_request),
                    close=AsyncMock(),
                )
                save_calls = []
                original_save = session_module.async_save_session

                async def counting_save(
                    target_path,
                    cookies,
                    save_calls=save_calls,
                    original_save=original_save,
                ):
                    save_calls.append(dict(cookies))
                    await original_save(target_path, cookies)

                http_session = AsyncHttpSession(
                    origin,
                    path,
                    required_cookie,
                    client_factory=lambda *_args, client=fake_client, **_kwargs: client,
                )
                try:
                    with patch.object(
                        session_module, "async_save_session", side_effect=counting_save
                    ):
                        await http_session.send("GET", "/probe")
                        self.assertEqual(len(save_calls), 1)
                        self.assertFalse(http_session._dirty)
                        self.assertEqual(read_session(path)[mutated_name], mutated_value)
                finally:
                    await http_session.close()

    async def test_http_session_uses_async_read_and_persists_dirty_cookies_on_reset(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import AsyncMock, patch

        import webaiprovider.transport.async_session as session_module
        from webaiprovider.accounts.credentials import save_session

        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "session.json"
            save_session(path, {"sessionKey": "initial-cookie"})
            jar_cookies = {"sessionKey": "mutated-before-reset"}
            fake_client = SimpleNamespace(
                cookies=SimpleNamespace(jar=[], get_dict=lambda: dict(jar_cookies)),
                close=AsyncMock(),
            )
            http_session = AsyncHttpSession(
                "https://claude.ai",
                path,
                "sessionKey",
                client_factory=lambda *_a, **_kw: fake_client,
            )
            with (
                patch.object(
                    session_module,
                    "async_read_session",
                    wraps=session_module.async_read_session,
                ) as read_async,
                patch.object(http_session, "persist", wraps=http_session.persist) as persist_spy,
            ):
                client = await http_session._acquire_client()
                self.assertIs(client, fake_client)
                read_async.assert_awaited_once()
                http_session._dirty = True
                await http_session._reset_owned_client(fake_client)
                persist_spy.assert_awaited_with(force=True)
            await http_session.close()
