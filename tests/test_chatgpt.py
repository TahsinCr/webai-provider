"""ChatGPT provider protocol, lifecycle, security, and upload tests."""

import asyncio
import base64
import gc
import json
import tempfile
import time
import unittest
import weakref
from collections import OrderedDict
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import ANY, AsyncMock, Mock, patch

from tests.fakes import FakeAsyncHTTPClient, FakeClient, FakeHTTPRequest, FakeHTTPResponse, cookie
from tests.fixtures import CATALOG
from tests.provider_fakes import provider_account
from webaiprovider.accounts.status import SessionState
from webaiprovider.base import Capability
from webaiprovider.core.timing import SendTiming, use_send_timing
from webaiprovider.errors import ProviderError, RateLimited, RemoteNotFound
from webaiprovider.providers.chatgpt import (
    AsyncChatGPTProvider,
    BrowserSession,
)
from webaiprovider.providers.chatgpt.async_client import (
    AsyncChatGPTClient as ChatGPTClient,
)
from webaiprovider.providers.chatgpt.async_client import (
    AsyncChatGPTProjectEditor as ChatGPTProjectEditor,
)
from webaiprovider.providers.chatgpt.async_client import (
    async_prepare_chatgpt_attachments,
)
from webaiprovider.providers.chatgpt.catalogs import parse_model_catalog
from webaiprovider.providers.chatgpt.discovery import project_id
from webaiprovider.providers.chatgpt.errors import ChatGPTError, SentinelCompatibilityError
from webaiprovider.providers.chatgpt.sentinel import (
    DEFAULT_SENTINEL_SPEC,
    SentinelRuntime,
    SentinelSpec,
    _atob,
    _btoa,
    _decode_initial_context,
    _fnv1a_avalanche,
    _js_number,
    _json_string,
    _xor_strings,
    generate_enforcement_proof,
)
from webaiprovider.resources.message.attachment import policy_for
from webaiprovider.resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    ProviderRequest,
    ResponseComplete,
    TextDelta,
)
from webaiprovider.resources.model import ModelCatalogState
from webaiprovider.resources.model.model import EffortOption, Model, ModelCatalog
from webaiprovider.transport import AsyncHTTPTransport


# Connection reuse and cancellation
class PersistentClient:
    instances: ClassVar[list["PersistentClient"]] = []

    def __init__(self, session, **_kwargs):
        self.session = session
        self.closed = asyncio.Event()
        self.temporary_conversations = None
        self.pending_branches = None
        type(self).instances.append(self)

    async def send_prompt(self, conversation_id, prompt, on_event, **_options):
        on_event({"type": "delta", "text": "OK"})
        if prompt == "cancel me":
            await asyncio.wait_for(self.closed.wait(), timeout=2)
        return "temporary-chat", {"messages": []}, set()

    async def close(self):
        self.closed.set()


class ChatGPTConnectionPoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_stream_reuses_prepared_client_and_discards_it_after_cancel(self):
        PersistentClient.instances.clear()
        with patch(
            "webaiprovider.providers.chatgpt.async_provider.AsyncChatGPTClient", PersistentClient
        ):
            provider = AsyncChatGPTProvider(
                provider_account("chatgpt"), session=object(), client_factory=PersistentClient
            )

        async with provider._client_scope() as prepared_client:
            pass
        self.assertEqual(len(PersistentClient.instances), 1)
        provider._persistent_client = True
        provider._client = prepared_client

        request = ProviderRequest("hello", temporary=True)
        events = [event async for event in provider._stream_provider_events(request)]
        self.assertTrue(any(isinstance(event, ConversationBound) for event in events))
        self.assertIs(provider._client, prepared_client)
        self.assertFalse(prepared_client.closed.is_set())

        cancelled = provider._stream_provider_events(ProviderRequest("cancel me", temporary=True))
        first = await asyncio.wait_for(anext(cancelled), timeout=1)
        self.assertIsInstance(first, TextDelta)
        await asyncio.wait_for(cancelled.aclose(), timeout=2)
        self.assertTrue(prepared_client.closed.is_set())
        self.assertIsNone(provider._client)
        self.assertEqual(len(PersistentClient.instances), 1)
        await provider.close()

    async def test_repeated_cancellation_waits_for_worker_cleanup_before_releasing_gate(self):
        class BlockingCloseClient(PersistentClient):
            def __init__(self, session, **_kwargs):
                super().__init__(session)
                self.send_started = asyncio.Event()
                self.close_started = asyncio.Event()
                self.release_close = asyncio.Event()

            async def send_prompt(self, conversation_id, prompt, on_event, **options):
                self.send_started.set()
                on_event({"type": "delta", "text": "started"})
                await asyncio.wait_for(self.release_close.wait(), timeout=2)
                return "temporary-chat", {"messages": []}, set()

            async def close(self):
                self.close_started.set()
                await asyncio.wait_for(self.release_close.wait(), timeout=2)
                await super().close()

        client = BlockingCloseClient(object())
        provider = AsyncChatGPTProvider(
            provider_account("chatgpt"),
            session=object(),
            client_factory=lambda _session, **_kwargs: client,
        )
        stream = provider._stream_provider_events(ProviderRequest("cancel me", temporary=True))

        async def consume():
            return [event async for event in stream]

        task = asyncio.create_task(consume())
        await asyncio.wait_for(client.send_started.wait(), 1)
        task.cancel()
        await asyncio.wait_for(client.close_started.wait(), 1)
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        self.assertFalse(client.closed.is_set())

        client.release_close.set()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        self.assertTrue(client.closed.is_set())
        await provider.close()


# Discovery, catalogs, and conversation operations
PROJECT = {
    "gizmo": {
        "gizmo": {
            "id": "g-p-work",
            "gizmo_type": "snorlax",
            "display": {"name": "Work"},
            "instructions": "Use concise answers.",
        },
        "files": [{"file_id": "file-ref", "name": "Reference.pdf", "type": "application/pdf"}],
    }
}


class DiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        self.session = BrowserSession(root / "session")
        self.requests = []

        def respond(request):
            self.requests.append(request)
            if request.url.path == "/backend-api/models":
                value = CATALOG
            elif request.url.path.endswith("/sidebar"):
                value = {
                    "items": [PROJECT, {"gizmo": {"id": "g-custom", "display": {"name": "GPT"}}}],
                    "cursor": "next",
                }
            elif request.url.path.endswith("/conversations"):
                value = {
                    "items": [{"id": "remote-chat", "title": "Project conversation"}],
                    "cursor": "next-chat",
                }
            else:
                value = PROJECT["gizmo"]
            return FakeHTTPResponse(200, json=value)

        self._http_handler = respond
        self.http = FakeAsyncHTTPClient(lambda req: self._http_handler(req))
        self.transport = AsyncHTTPTransport(
            "https://chatgpt.com",
            client_getter=lambda: self.http,
        )
        self.client = ChatGPTClient(self.session, authenticate=False, transport=self.transport)
        self.addAsyncCleanup(self.client.close)
        self.addAsyncCleanup(self.transport.close)
        self.addCleanup(self.http.close)

    async def test_rejected_session_uses_the_common_authentication_error(self):
        from webaiprovider.errors import AuthenticationError

        request = FakeHTTPRequest("GET", "https://chatgpt.com/backend-api/models")
        response = FakeHTTPResponse(401, request=request)
        with (
            patch.object(self.client, "_transport_headers", return_value={}),
            patch.object(self.client, "refresh_session", new_callable=AsyncMock),
            patch.object(
                self.client.transport, "send", new_callable=AsyncMock, return_value=response
            ),
            self.assertRaises(AuthenticationError),
        ):
            await self.client.request("GET", str(request.url))

    async def test_http_project_create_reads_back_and_never_launches_a_process(self):
        self.client.request = AsyncMock(
            return_value=FakeHTTPResponse(
                200, json={"resource": {"id": "g-p-created", "display": {"name": "Demo"}}}
            )
        )
        self.client.get_project = AsyncMock(
            return_value={
                "id": "g-p-created",
                "name": "Demo",
                "metadata": {"gizmo": {"memory_scope": "project_v2"}},
            }
        )
        with patch("subprocess.run", side_effect=AssertionError("browser")):
            value = await self.client.create_project("Demo", memory_share=False)
        self.assertEqual(value["id"], "g-p-created")
        self.assertEqual(self.client.request.call_args.kwargs["json"]["memory_scope"], "project_v2")
        self.client.get_project.assert_awaited_once_with("g-p-created")
        self.client.request.reset_mock()
        await self.client.create_project("x" * 50)
        self.assertEqual(len(self.client.request.call_args.kwargs["json"]["name"]), 50)
        self.client.request.reset_mock()
        with self.assertRaisesRegex(ValueError, "1–50"):
            await self.client.create_project("x" * 51)
        self.client.request.assert_not_awaited()

        await self.client.create_project("Demo")
        self.assertEqual(self.client.request.call_args.kwargs["json"]["memory_scope"], "global")
        self.client.request.reset_mock()
        with self.assertRaises(TypeError):
            await self.client.create_project("Demo", memory_share=1)
        self.client.request.assert_not_awaited()

        self.client.get_project.reset_mock()
        self.client.request.return_value = FakeHTTPResponse(
            200,
            json={
                "resource": {
                    "gizmo": {
                        "id": "g-p-created",
                        "display": {"name": "Demo"},
                        "instructions": "Notes",
                        "memory_scope": "project_v2",
                        "current_user_permission": {"can_write": True},
                    }
                }
            },
        )
        project = await self.client.create_project("Demo", "Notes", memory_share=False)
        self.assertEqual((project["id"], project["instructions"]), ("g-p-created", "Notes"))
        self.client.get_project.assert_not_awaited()

    def test_chat_picker_uses_categories_without_exposing_work_variants_or_pinning_versions(self):
        raw = {
            "models": [
                {"slug": "gpt-future-thinking", "title": "Provider engine name"},
                {"slug": "gpt-future-work-wm", "title": "Future work model"},
                {"slug": "research", "title": "Research"},
            ],
            "categories": [
                {
                    "default_model": "gpt-future-thinking",
                    "supported_models": ["gpt-future-thinking"],
                    "model_version": "42",
                    "model_lane": "thinking",
                }
            ],
            "default_model_slug": "gpt-future-thinking",
        }
        catalog = parse_model_catalog(raw)
        self.assertEqual(len(catalog.models), 3)
        self.assertEqual([m.id for m in catalog.chat_models], ["gpt-future-thinking"])
        self.assertEqual(catalog.chat_models[0].chat_title, "GPT-42 Thinking")

    def test_work_variants_stay_hidden_if_an_older_response_has_no_categories(self):
        catalog = parse_model_catalog({"models": [{"slug": "normal"}, {"slug": "private-wm"}]})
        self.assertEqual([m.id for m in catalog.chat_models], ["normal"])

    async def test_project_sidebar_and_details_preserve_instructions_and_files(self):
        listing = await self.client.list_projects()
        self.assertEqual(len(listing["items"]), 1)
        self.assertEqual(listing["cursor"], "next")
        self.assertEqual(listing["items"][0]["name"], "Work")
        detail = await self.client.get_project("g-p-work")
        self.assertEqual(detail["instructions"], "Use concise answers.")
        self.assertEqual(detail["resources"][0]["name"], "Reference.pdf")
        self.assertEqual(self.requests[0].url.params["conversations_per_gizmo"], "0")
        self.assertEqual(self.requests[0].url.params["owned_only"], "false")
        await self.client.list_projects("next")
        self.assertEqual(self.requests[-1].url.params["cursor"], "next")

    async def test_project_chat_pagination_and_cache_preserve_membership(self):
        page = await self.client.list_project_chats("g-p-work", "next-chat", limit=2)
        self.assertEqual(page["items"][0]["project_id"], "g-p-work")
        self.assertEqual(self.requests[-1].url.params["cursor"], "next-chat")
        self.assertEqual(self.requests[-1].url.params["limit"], "2")
        self.assertEqual(project_id({"id": "remote-chat", "gizmo_id": "g-p-work"}), "g-p-work")

    async def test_common_provider_exposes_the_same_real_catalog_and_project_records(self):
        transport = AsyncHTTPTransport("https://chatgpt.com", client_getter=lambda: self.http)
        provider = AsyncChatGPTProvider(
            provider_account("chatgpt"),
            session=self.session,
            client_factory=lambda _session, **kwargs: ChatGPTClient(
                self.session, authenticate=False, transport=kwargs.get("transport", transport)
            ),
            transport=transport,
        )
        try:
            catalog = await provider._load_catalog()
            projects = await provider._list_projects_data()
            detail = await provider._get_project_data("g-p-work")
            chats = await provider._list_project_chats_data("g-p-work")
        finally:
            await provider.close()
        self.assertEqual(catalog.default_model, "account-thinking")
        self.assertEqual(projects.next_cursor, "next")
        self.assertEqual(detail.resources[0].id, "file-ref")
        self.assertEqual(detail.instructions, "Use concise answers.")
        self.assertIsNotNone(chats.items[0].project)
        assert chats.items[0].project is not None
        self.assertEqual(chats.items[0].project.id, "g-p-work")
        self.assertEqual(chats.next_cursor, "next-chat")

    async def test_changed_responses_fail_instead_of_claiming_an_empty_account(self):
        for value in ({}, {"models": {}}, {"models": [None]}, {"models": [{"title": "No slug"}]}):
            with self.subTest(value=value), self.assertRaises(ChatGPTError):
                parse_model_catalog(value)
        self._http_handler = lambda _: FakeHTTPResponse(200, json={"unexpected": []})
        with self.assertRaisesRegex(ChatGPTError, "items"):
            await self.client.list_projects()

    async def test_direct_send_applies_selected_model_effort_and_keeps_project(self):
        previous = {"id": "chat", "current_node": "parent", "gizmo_id": "g-p-work", "messages": []}
        body = self.client.create_conversation_body(previous, "hello")
        self.assertNotIn("thinking_effort", body)
        self.assertNotIn("model", body)
        self.assertEqual(body["gizmo_id"], "g-p-work")
        with (
            patch.object(
                self.client,
                "send_prompt_direct",
                new_callable=AsyncMock,
                return_value=("chat", "user-msg", {"id": "user-msg"}, []),
            ) as send,
            patch.object(
                self.client,
                "get_conversation",
                new_callable=AsyncMock,
                side_effect=[previous, {"messages": [{"id": "user-msg"}]}],
            ),
        ):
            await self.client.send_prompt(
                "chat", "hello", model="account-thinking", effort="standard"
            )
        self.assertEqual(send.call_args.args[-2:], ("account-thinking", "standard"))
        with self.assertRaisesRegex(ChatGPTError, "not offered"):
            await self.client.send_prompt(None, "hello", model="account-instant", effort="extended")

    async def test_truncated_or_malformed_chatgpt_stream_cannot_succeed(self):
        for payload in (
            'data: {"conversation_id":"chat"}\n\n',
            "data: {bad}\n\ndata: [DONE]\n\n",
            'data: {"error":{"message":"secret"}}\n\ndata: [DONE]\n\n',
        ):
            self._http_handler = lambda _, payload=payload: FakeHTTPResponse(
                200, headers={"content-type": "text/event-stream"}, text=payload
            )
            with (
                patch.object(
                    self.client, "create_sentinel_headers", new_callable=AsyncMock, return_value={}
                ),
                self.assertRaises(ChatGPTError),
            ):
                await self.client.send_prompt_direct(None, "hello", [])

    def test_timezone_uses_a_real_iana_name_or_omits_the_optional_field(self):
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

        from webaiprovider.providers.chatgpt.base import system_timezone

        try:
            ZoneInfo("Asia/Tokyo")
        except ZoneInfoNotFoundError:
            with patch.dict("os.environ", {"TZ": "Asia/Tokyo"}):
                self.assertIsNone(system_timezone())
        else:
            with patch.dict("os.environ", {"TZ": "Asia/Tokyo"}):
                self.assertEqual(system_timezone(), "Asia/Tokyo")
        with patch("webaiprovider.providers.chatgpt.base.system_timezone", return_value=None):
            body = self.client.create_conversation_body(None, "Question")
        self.assertNotIn("timezone", body)
        self.assertIsInstance(body["timezone_offset_min"], int)

    async def test_empty_mutation_success_is_explicit_and_json_reads_remain_strict(self):
        self._http_handler = lambda _: FakeHTTPResponse(204)
        await self.client.delete_conversation("chat")
        with self.assertRaises(ChatGPTError):
            await self.client.request("GET", "https://chatgpt.com/backend-api/test")

    async def test_model_and_effort_reach_the_actual_stream_request(self):
        sent = []

        def respond(request):
            sent.append(json.loads(request.content))
            return FakeHTTPResponse(
                200,
                headers={"content-type": "text/event-stream"},
                text='data: {"conversation_id":"chat","message":{"id":"a","author":{"role":"assistant"},"content":{"parts":["Answer"]}}}\n\ndata: [DONE]\n\n',
            )

        self._http_handler = respond
        with patch.object(
            self.client, "create_sentinel_headers", new_callable=AsyncMock, return_value={}
        ):
            await self.client.send_prompt_direct(
                None, "hello", [], model="account-thinking", effort="standard"
            )
        self.assertEqual(
            (sent[0]["model"], sent[0]["thinking_effort"]), ("account-thinking", "standard")
        )

    async def test_chatgpt_stream_accepts_sse_data_without_space_and_multiline_json(self):
        event = json.dumps(
            {
                "conversation_id": "chat",
                "message": {
                    "id": "assistant",
                    "author": {"role": "assistant"},
                    "content": {"parts": ["Answer"]},
                },
            }
        )
        midpoint = event.index('"message"')
        payload = (
            f": keepalive\n\ndata:{event[:midpoint]}\ndata: {event[midpoint:]}\n\ndata:[DONE]\n\n"
        )
        self._http_handler = lambda _: FakeHTTPResponse(
            200, headers={"content-type": "text/event-stream"}, text=payload
        )
        with patch.object(
            self.client, "create_sentinel_headers", new_callable=AsyncMock, return_value={}
        ):
            result = await self.client.send_prompt_direct(None, "hello", [])

        self.assertEqual(result[0], "chat")
        self.assertEqual(result[3][0]["content"]["parts"], ["Answer"])

    async def test_chatgpt_stream_delivers_first_text_before_next_network_chunk(self):
        events = []
        case = self
        event = {
            "conversation_id": "chat",
            "message": {
                "id": "assistant",
                "author": {"role": "assistant"},
                "content": {"parts": ["Answer"]},
            },
        }

        class Stream:
            async def __aiter__(self):
                yield f"data: {json.dumps(event)}\n\n".encode()
                case.assertTrue(any(item.get("text") == "Answer" for item in events))
                yield b"data: [DONE]\n\n"

        self._http_handler = lambda _: FakeHTTPResponse(
            200, headers={"content-type": "text/event-stream"}, stream=Stream()
        )
        with patch.object(
            self.client, "create_sentinel_headers", new_callable=AsyncMock, return_value={}
        ):
            await self.client.send_prompt_direct(None, "hello", [], on_event=events.append)

    async def test_failed_sse_releases_the_partial_message_buffer(self):
        message = {
            "conversation_id": "chat",
            "message": {
                "id": "assistant",
                "author": {"role": "assistant"},
                "content": {"parts": ["partial"]},
            },
        }
        payload = f"data: {json.dumps(message)}\n\ndata: {json.dumps({'error': True})}\n\n"
        self._http_handler = lambda _: FakeHTTPResponse(
            200, headers={"content-type": "text/event-stream"}, text=payload
        )
        buffers = []

        class TrackedOrderedDict(OrderedDict):
            pass

        def create_buffer():
            value = TrackedOrderedDict()
            buffers.append(weakref.ref(value))
            return value

        with (
            patch("webaiprovider.providers.chatgpt.base.OrderedDict", side_effect=create_buffer),
            patch.object(
                self.client, "create_sentinel_headers", new_callable=AsyncMock, return_value={}
            ),
            self.assertRaisesRegex(ChatGPTError, "stream failed"),
        ):
            await self.client.send_prompt_direct(None, "hello", [])

        gc.collect()
        self.assertEqual(len(buffers), 1)
        self.assertIsNone(buffers[0]())
        self.assertFalse(hasattr(self.client, "_last_stream_messages"))

    async def test_chatgpt_ends_generation_only_after_all_visible_messages(self):
        event = {
            "conversation_id": "chat",
            "message": {
                "id": "assistant",
                "author": {"role": "assistant"},
                "status": "finished_successfully",
                "content": {"parts": ["Answer"]},
            },
        }
        second = {
            **event,
            "message": {
                **event["message"],
                "id": "assistant-2",
                "content": {"parts": ["Second answer block"]},
            },
        }
        payload = f"data: {json.dumps(event)}\n\ndata: {json.dumps(second)}\n\ndata: [DONE]\n\n"
        self._http_handler = lambda _: FakeHTTPResponse(
            200, headers={"content-type": "text/event-stream"}, text=payload
        )
        received = []
        with patch.object(
            self.client, "create_sentinel_headers", new_callable=AsyncMock, return_value={}
        ):
            await self.client.send_prompt_direct(None, "hello", [], on_event=received.append)

        self.assertEqual(
            [item["type"] for item in received], ["delta", "delta", "response_complete"]
        )
        self.assertEqual(
            [item["message_id"] for item in received[:-1]], ["assistant", "assistant-2"]
        )

    async def test_model_catalog_reuses_valid_cache_and_refreshes_expired_cache(self):
        cached = parse_model_catalog(CATALOG)
        self.client._model_catalog = (time.monotonic(), cached)
        with patch.object(self.client, "request", new_callable=AsyncMock) as request:
            self.assertIs(await self.client.model_catalog(), cached)
            request.assert_not_awaited()

            response = Mock()
            response.json.return_value = CATALOG
            request.return_value = response
            self.client._model_catalog = (time.monotonic() - 301, cached)
            refreshed = await self.client.model_catalog()

        self.assertIsNot(refreshed, cached)
        request.assert_awaited_once()

    def test_system_timezone_caches_per_tz_environment(self):
        from webaiprovider.providers.chatgpt.base import (
            _cached_system_timezone,
            system_timezone,
        )

        _cached_system_timezone.cache_clear()
        self.addCleanup(_cached_system_timezone.cache_clear)
        with patch(
            "webaiprovider.providers.chatgpt.base.ZoneInfo",
            side_effect=lambda value: Mock(key=value),
        ):
            with (
                patch.dict("os.environ", {"TZ": "Europe/Istanbul"}, clear=False),
                patch.object(
                    Path, "resolve", return_value=Path("/usr/share/zoneinfo/UTC")
                ) as resolve,
            ):
                self.assertEqual(system_timezone(), "Europe/Istanbul")
                self.assertEqual(system_timezone(), "Europe/Istanbul")
                self.assertEqual(resolve.call_count, 1)

            with patch.dict("os.environ", {"TZ": "Asia/Tokyo"}, clear=False):
                self.assertEqual(system_timezone(), "Asia/Tokyo")

    async def test_move_conversation_validates_patch_and_updates_cache_without_get(self):
        snapshot = {
            "id": "conv-move",
            "current_node": "node-1",
            "gizmo_id": "g-p-old",
            "messages": [],
        }
        self.client._remember_conversation(snapshot)
        self.client.get_conversation = AsyncMock(
            side_effect=AssertionError("unexpected readback GET")
        )

        with patch.object(
            self.client,
            "request",
            new_callable=AsyncMock,
            return_value=FakeHTTPResponse(200, json={"success": True}),
        ) as req:
            result = await self.client.move_conversation("conv-move", "g-p-new")

        req.assert_awaited_once()
        self.assertEqual(result["project_id"], "g-p-new")
        cached = self.client._cached_conversation("conv-move")
        self.assertIsNotNone(cached)
        self.assertEqual(cached["gizmo_id"], "g-p-new")

        with (
            patch.object(
                self.client,
                "request",
                new_callable=AsyncMock,
                return_value=FakeHTTPResponse(200, json={"success": False}),
            ),
            self.assertRaises(ChatGPTError),
        ):
            await self.client.move_conversation("conv-move", "g-p-other")


# Profile and session bootstrap
class ChatGPTProfileBootstrapTests(unittest.IsolatedAsyncioTestCase):
    def test_missing_saved_session_uses_the_common_authentication_error(self):
        from webaiprovider.errors import AuthenticationError

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            session = BrowserSession(root / "missing.json")
            with self.assertRaises(AuthenticationError):
                session.load()

    def test_bootstrap_saves_provider_token_and_browser_headers_without_cdp(self):
        from unittest.mock import MagicMock

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            session = BrowserSession(root / "session.json")
            http = MagicMock()
            http.get.return_value.status_code = 200
            http.get.return_value.json.return_value = {"accessToken": "verified-token"}
            http.cookies.jar = [
                cookie("__cf_bm", "fresh", domain=".chatgpt.com"),
                cookie("foreign", "ignore", domain=".evil.test"),
            ]
            factory = MagicMock()
            factory.return_value.__enter__.return_value = http
            with (
                patch("curl_cffi.requests.Session", factory),
                patch.object(session, "is_ready", side_effect=AssertionError("CDP")),
            ):
                session.import_cookies(
                    {"__Secure-next-auth.session-token": "login", "oai-did": "device"}
                )
            url, headers = session.load()
            self.assertEqual(url, "https://chatgpt.com/backend-api/conversations")
            self.assertEqual(headers["authorization"], "Bearer verified-token")
            self.assertIn("Chrome/", headers["user-agent"])
            self.assertEqual(headers["oai-device-id"], "device")
            self.assertIn("__cf_bm=fresh", headers["cookie"])
            self.assertNotIn("foreign", headers["cookie"])
            self.assertFalse(http.get.call_args.kwargs["allow_redirects"])
            self.assertEqual(http.get.call_args.args[0], "https://chatgpt.com/api/auth/session")

    async def test_provider_open_prepares_cookie_credentials_before_catalog(self):
        from webaiprovider.accounts import REQUIRED_PROVIDER_COOKIES
        from webaiprovider.accounts.account import Account

        cookie_name = REQUIRED_PROVIDER_COOKIES["https://chatgpt.com"]
        cookies = {f"{cookie_name}.0": "part-zero", f"{cookie_name}.1": "part-one"}
        provider = AsyncChatGPTProvider(Account.from_cookies("chatgpt", cookies))

        try:
            with (
                patch.object(
                    provider.session, "import_cookies_async", new_callable=AsyncMock
                ) as import_cookies,
                patch.object(
                    provider,
                    "_load_catalog",
                    AsyncMock(return_value=ModelCatalog(ModelCatalogState.AVAILABLE)),
                ) as load_catalog,
            ):
                await provider._prepare_session()

            import_cookies.assert_awaited_once_with(cookies)
            load_catalog.assert_awaited_once_with()
        finally:
            await provider.close()

    def test_bootstrap_rejection_retains_previous_snapshot_and_does_not_follow_redirect(self):
        from unittest.mock import MagicMock

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            session = BrowserSession(root / "session.json")
            session.save(
                "https://chatgpt.com/backend-api/conversations", {"authorization": "previous"}
            )
            before = session.session_file.read_bytes()
            http = MagicMock()
            http.get.return_value.status_code = 302
            factory = MagicMock()
            factory.return_value.__enter__.return_value = http
            with patch("curl_cffi.requests.Session", factory), self.assertRaises(ProviderError):
                session.import_cookies({"__Secure-next-auth.session-token": "login"})
            self.assertEqual(session.session_file.read_bytes(), before)
            http.get.assert_called_once()

    async def test_async_bootstrap_saves_provider_token_and_browser_headers(self):
        from unittest.mock import MagicMock

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            session = BrowserSession(root / "session.json")
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {"accessToken": "async-verified-token"}
            http = MagicMock()
            http.get = AsyncMock(return_value=response)
            http.cookies.jar = [cookie("__cf_bm", "fresh", domain=".chatgpt.com")]
            factory = MagicMock()
            factory.return_value.__aenter__ = AsyncMock(return_value=http)
            factory.return_value.__aexit__ = AsyncMock(return_value=None)
            with patch("curl_cffi.requests.AsyncSession", factory):
                await session.import_cookies_async(
                    {"__Secure-next-auth.session-token": "login", "oai-did": "device"}
                )
            url, headers = await session.load_async()
            self.assertEqual(url, "https://chatgpt.com/backend-api/conversations")
            self.assertEqual(headers["authorization"], "Bearer async-verified-token")
            self.assertEqual(headers["oai-device-id"], "device")
            self.assertIn("__cf_bm=fresh", headers["cookie"])


class _ChatGPTProviderFixture:
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.session = BrowserSession(root / "session.json")
        self.client = FakeClient(self.session)
        self.provider = AsyncChatGPTProvider(
            provider_account("chatgpt"), session=self.session, client_factory=lambda _: self.client
        )


# Provider behavior and optimizations
class ChatGPTProviderTests(_ChatGPTProviderFixture, unittest.IsolatedAsyncioTestCase):
    async def test_structured_text_parts_are_readable_alongside_thinking(self):
        self.client.raw["messages"] = [
            {
                "id": "thought",
                "author": {"role": "assistant"},
                "content": {"content_type": "thoughts", "thoughts": [{"summary": "Thinking"}]},
            },
            {
                "id": "answer",
                "author": {"role": "assistant"},
                "content": {
                    "content_type": "multimodal_text",
                    "parts": [{"content_type": "audio_transcription", "text": "Spoken answer"}],
                },
            },
        ]
        chat = await self.provider._get_chat_data("remote")
        self.assertEqual([m.content[0].text for m in chat.messages], ["Thinking", "Spoken answer"])

    async def test_session_snapshot_is_not_mistaken_for_verified_authentication(self):
        self.assertEqual((await self.provider.session_status()).state, SessionState.LOGIN_REQUIRED)
        self.session.session_file.write_text("{}")
        self.assertEqual((await self.provider.session_status()).state, SessionState.UNKNOWN)
        self.assertEqual((await self.provider.authenticate()).state, SessionState.AUTHENTICATED)

    async def test_forget_session_retains_browser_profile(self):
        self.session.session_file.write_text("{}")
        profile = self.session.session_file.parent / "profile"
        profile.mkdir()
        result = await self.provider.forget_session()
        self.assertEqual(result.state, SessionState.LOGIN_REQUIRED)
        self.assertFalse(self.session.session_file.exists())
        self.assertTrue(profile.exists())

    async def test_history_keeps_provenance_thinking_channel_sources_and_native_fields(self):
        page = await self.provider._list_chats_data(limit=1)
        self.assertEqual(page.next_cursor, "1")
        chat = page.items[0]
        self.assertEqual(chat.id, "remote")
        self.assertEqual(chat.messages[0].channel, "analysis")
        self.assertEqual(chat.messages[0].content[0].text, "Visible summary")
        self.assertEqual(
            chat.messages[0].metadata["citations"], self.client.raw["messages"][0]["citations"]
        )
        self.assertEqual(chat.metadata, self.client.raw)

    async def test_history_reads_active_message_path_from_mapping_responses(self):
        self.client.raw = {
            "id": "remote",
            "title": "Mapped chat",
            "current_node": "answer",
            "messages": [{"id": "stale-flat-message"}],
            "mapping": {
                "root": {"parent": None, "message": None},
                "user": {
                    "parent": "root",
                    "message": {
                        "id": "user",
                        "author": {"role": "user"},
                        "content": {"parts": ["Question"]},
                    },
                },
                "answer": {
                    "parent": "user",
                    "message": {
                        "id": "answer",
                        "author": {"role": "assistant"},
                        "content": {"parts": ["Answer"]},
                        "status": "in_progress",
                        "create_time": 1_800_000_000,
                    },
                },
                "other-branch": {
                    "parent": "user",
                    "message": {
                        "id": "other-branch",
                        "author": {"role": "assistant"},
                        "content": {"parts": ["Inactive branch"]},
                    },
                },
            },
        }

        chat = await self.provider._get_chat_data("remote")

        self.assertEqual([message.id for message in chat.messages], ["user", "answer"])
        self.assertNotIn("mapping", chat.metadata)
        self.assertEqual(chat.metadata["generation"], {"active": True, "started_at": 1_800_000_000})

    async def test_discovery_creation_and_deletion_are_available(self):
        self.assertIn(Capability.PROJECTS, self.provider.capabilities)
        self.client.model_catalog = lambda: ModelCatalog(ModelCatalogState.AVAILABLE)
        self.client.list_projects = lambda cursor: {"items": [], "cursor": None}
        self.assertEqual((await self.provider._load_catalog()).state, ModelCatalogState.AVAILABLE)
        self.assertEqual((await self.provider._list_projects_data()).items, ())
        self.client.create_project = lambda name, instructions, **options: {
            "id": "g-p-created",
            "name": name,
            "instructions": instructions,
            "metadata": options,
        }
        created = await self.provider._create_project_data("New project")
        self.assertEqual(created.name, "New project")
        project_reads = iter([{"id": "g-p-created", "name": "Fixture"}, RemoteNotFound("deleted")])

        def get_project(value):
            result = next(project_reads)
            if isinstance(result, Exception):
                raise result
            return result

        self.client.get_project = get_project
        with (
            patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as browser,
            patch(
                "webaiprovider.providers.chatgpt.async_provider.asyncio.sleep",
                new_callable=AsyncMock,
            ) as delay,
        ):
            await self.provider._delete_project_data("g-p-created")
            browser.assert_not_awaited()
            delay.assert_awaited_once_with(0.2)
            self.assertEqual(
                self.client.deleted,
                ("DELETE", "https://chatgpt.com/backend-api/gizmos/g-p-created"),
            )

    async def test_project_memory_share_maps_boolean_to_web_setting(self):
        memory_values = []

        def create_project(name, instructions, **options):
            memory_values.append(options["memory_share"])
            return {
                "id": name,
                "name": name,
                "instructions": instructions,
                "metadata": options,
            }

        self.client.create_project = create_project

        await self.provider._create_project_data("Shared")
        await self.provider._create_project_data("Private", memory_share=False)
        with self.assertRaises(TypeError):
            await self.provider._create_project_data("Invalid", memory_share=1)

        self.assertEqual(memory_values, [True, False])

    async def test_request_waits_for_background_preparation(self):
        ready, release = asyncio.Event(), asyncio.Event()

        async def prepare_catalog():
            ready.set()
            await asyncio.wait_for(release.wait(), 2)
            return ModelCatalog(ModelCatalogState.AVAILABLE)

        self.client.model_catalog = prepare_catalog
        warmup = asyncio.create_task(self.provider._prepare_session())
        await asyncio.wait_for(ready.wait(), 2)
        history = asyncio.create_task(self.provider._get_chat_data("remote"))
        await asyncio.sleep(0.02)
        self.assertFalse(history.done())
        release.set()
        await warmup
        self.assertEqual((await history).id, "remote")

    async def test_default_http_client_is_reused_after_startup_warmup(self):
        from curl_cffi.requests import Session as CurlSession

        raw = {"id": "remote", "title": "Example", "messages": []}
        catalog = ModelCatalog(ModelCatalogState.AVAILABLE)
        self.session.load_async = AsyncMock(
            return_value=("https://chatgpt.com/", {"authorization": "Bearer test"})
        )
        with (
            patch(
                "webaiprovider.providers.chatgpt.async_provider.AsyncHTTPTransport",
                wraps=AsyncHTTPTransport,
            ) as transports,
            patch(
                "curl_cffi.requests.Session",
                wraps=CurlSession,
            ) as sync_sessions,
            patch.object(
                ChatGPTClient, "model_catalog", new_callable=AsyncMock, return_value=catalog
            ) as model_catalog,
            patch.object(ChatGPTClient, "trigger_sentinel_prewarm") as prewarm,
            patch.object(
                ChatGPTClient, "list_page", new_callable=AsyncMock, return_value={"items": [raw]}
            ),
            patch.object(
                ChatGPTClient, "get_conversation", new_callable=AsyncMock, return_value=raw
            ),
            patch.object(
                ChatGPTClient,
                "get_project",
                new_callable=AsyncMock,
                return_value={"metadata": {}},
            ),
        ):
            provider = AsyncChatGPTProvider(provider_account("chatgpt"), session=self.session)
            try:
                await provider._prepare_session()
                prewarm.assert_called_once_with()
                await provider._load_catalog()
                await provider._load_catalog(force=True)
                await provider._list_chats_data(limit=1)
                await provider._get_chat_data("remote")
                await provider.project_options("project")
                self.assertEqual(transports.call_count, 1)
                self.assertEqual(sync_sessions.call_count, 0)
                self.assertIsNotNone(provider._client)
                self.assertIsNotNone(provider._owned_transport)
                self.assertIs(
                    provider._client.temporary_conversations,
                    provider._temporary_conversations,
                )
                model_catalog.assert_awaited_with(refresh=True)
            finally:
                await provider.close()
            await provider.close()
            self.assertIsNone(provider._client)

    async def test_project_file_source_is_streamed_using_a_dedicated_client(self):

        path = Path(self.directory.name) / "source.txt"
        path.write_text("fixture")
        self.client.upload_file = lambda name, mime, data, **kwargs: (
            {"id": "file", "size_bytes": data.stat().st_size}
            if isinstance(data, Path) and data.resolve() == path.resolve()
            else self.fail("expected a disk path")
        )
        requests = []

        async def record_request(method, url, **kwargs):
            requests.append((method, url, kwargs))
            return {"file_id": "file"}

        self.client.request = record_request
        editor = ChatGPTProjectEditor(self.session, lambda _: self.client)
        with patch("pathlib.Path.read_bytes", side_effect=AssertionError("must stream")):
            await editor.add_source("project", kind="file", path=path)
        self.assertTrue(self.client.closed.is_set())
        self.assertEqual(
            requests[0][2]["json"]["files"][0]["last_modified"],
            path.stat().st_mtime_ns // 1_000_000,
        )

    async def test_completed_snapshot_publishes_thinking_summaries(self):
        self.client.closed.set()
        self.client.raw["messages"][0]["content"] = {
            "content_type": "thoughts",
            "thoughts": [{"summary": "Published summary"}],
        }
        events = [
            event async for event in self.provider._stream_provider_events(ProviderRequest("hello"))
        ]
        parts = next(event.parts for event in events if isinstance(event, MessageMetadata))
        self.assertEqual(
            [(part.id, part.text) for part in parts],
            [("thinking:" + self.client.raw["messages"][0]["id"], "Published summary")],
        )

    async def test_completion_does_not_publish_hidden_or_tool_directed_analysis(self):
        self.client.closed.set()
        self.client.raw["messages"] = [
            {
                "id": "hidden",
                "author": {"role": "assistant"},
                "channel": "analysis",
                "metadata": {"is_visually_hidden_from_conversation": True},
                "content": {"parts": ["Hidden"]},
            },
            {
                "id": "tool",
                "author": {"role": "assistant"},
                "recipient": "browser",
                "channel": "analysis",
                "content": {"parts": ["Tool protocol"]},
            },
        ]
        events = [
            event async for event in self.provider._stream_provider_events(ProviderRequest("hello"))
        ]
        self.assertFalse(
            any(isinstance(event, MessageMetadata) and event.parts for event in events)
        )

    async def test_stream_produces_common_events_and_releases_transport(self):
        self.client.closed.set()

        async def collect():
            with use_send_timing(timing):
                return [
                    event
                    async for event in self.provider._stream_provider_events(
                        ProviderRequest("hello")
                    )
                ]

        timing = SendTiming()
        events = await collect()
        self.assertEqual(events[0], TextDelta("partial"))
        self.assertEqual(events[-1].conversation_id, "remote")
        self.assertTrue(self.client.closed.is_set())
        self.assertEqual(timing.snapshot()[0]["provider_send_gate_wait_ms"], 0)

    async def test_stream_uploads_inline_bytes_attachment_before_sending(self):
        uploaded = []
        sent = []

        async def fake_upload(name, mime, data, **kwargs):
            uploaded.append((name, mime, data, kwargs))
            return {
                "id": "chatgpt-file-1",
                "name": name,
                "mime_type": mime,
                "size_bytes": len(data),
            }

        async def send_prompt(conversation_id, prompt, on_event, **kwargs):
            sent.append(kwargs)
            on_event({"type": "replace", "text": "finished"})
            return "remote", self.client.raw, set()

        self.client.upload_file = fake_upload
        self.client.send_prompt = send_prompt
        request = ProviderRequest(
            "read",
            attachments=({"bytes": b"hello", "name": "note.txt", "mime_type": "text/plain"},),
        )
        _ = [event async for event in self.provider._stream_provider_events(request)]
        self.assertEqual(len(uploaded), 1)
        self.assertEqual(uploaded[0][:3], ("note.txt", "text/plain", b"hello"))
        self.assertEqual(sent[0]["attachments"][0]["id"], "chatgpt-file-1")

    async def test_stream_inline_attachment_upload_honors_cancellation(self):
        started = asyncio.Event()
        saw_cancel = False

        async def slow_upload(name, mime, data, *, project_id=None):
            nonlocal saw_cancel
            del name, mime, data, project_id
            started.set()
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                saw_cancel = True
                raise

        self.client.upload_file = slow_upload
        self.client.send_prompt = lambda *_args, **_kwargs: self.fail(
            "send_prompt must not run after cancellation"
        )
        request = ProviderRequest(
            "read",
            attachments=({"bytes": b"hello", "name": "note.txt", "mime_type": "text/plain"},),
        )
        stream = self.provider._stream_provider_events(request)
        task = asyncio.create_task(anext(stream))
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        self.assertTrue(saw_cancel)

    async def test_upload_cancellation_signals_transport_and_waits_for_worker(self):
        started, stopped = asyncio.Event(), asyncio.Event()
        closed = asyncio.Event()

        class UploadClient:
            async def close(self):
                closed.set()

            async def upload_file(self, _name, _mime, _path, *, cancel_event=None):
                del cancel_event
                started.set()
                try:
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    stopped.set()
                    raise

        client = UploadClient()
        provider = AsyncChatGPTProvider(
            provider_account("chatgpt"),
            session=self.session,
            client_factory=lambda _session: client,
        )
        task = asyncio.create_task(provider._upload_file(Path(self.directory.name) / "cancel.txt"))
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        self.assertTrue(stopped.is_set())
        self.assertTrue(closed.is_set())

    async def test_rate_limit_survives_provider_and_is_not_retried(self):
        self.client.closed.set()
        self.client.failure = RateLimited(30)
        with self.assertRaises(RateLimited) as raised:
            async for _ in self.provider._stream_provider_events(ProviderRequest("hello")):
                pass
        self.assertEqual(raised.exception.retry_after, 30)

    async def test_unexpected_transport_failure_uses_safe_provider_error(self):
        self.client.closed.set()
        self.client.failure = ValueError("secret transport detail")

        with self.assertRaises(ProviderError) as raised:
            async for _ in self.provider._stream_provider_events(ProviderRequest("hello")):
                pass

        self.assertEqual(raised.exception.code, "chatgpt_error")
        self.assertIsNone(raised.exception.message_submitted)
        self.assertNotIn("secret transport detail", str(raised.exception))

    async def test_sentinel_compatibility_error_keeps_its_type_and_submission_state(self):
        self.client.closed.set()
        failure = SentinelCompatibilityError("private signer detail")
        self.client.failure = failure
        with self.assertRaises(SentinelCompatibilityError) as raised:
            async for _ in self.provider._stream_provider_events(ProviderRequest("hello")):
                pass
        self.assertIs(raised.exception, failure)
        self.assertEqual(raised.exception.code, "chatgpt_sentinel_incompatible")
        self.assertFalse(raised.exception.message_submitted)

    def test_medium_effort_only_selected_when_catalog_supports_it(self):
        catalog = ModelCatalog(
            ModelCatalogState.AVAILABLE,
            (
                Model(
                    "a", "A", (EffortOption("low", "Low"), EffortOption("medium", "Medium")), "low"
                ),
                Model("b", "B", (EffortOption("auto", "Automatic"),), "auto"),
                Model(
                    "c",
                    "C",
                    (EffortOption("standard", "Standard"), EffortOption("medium", "Medium")),
                    None,
                ),
            ),
        )
        self.assertEqual(catalog.effort("a"), "medium")
        self.assertEqual(catalog.effort("b"), "auto")
        self.assertIsNone(catalog.effort("c"))
        self.assertEqual(catalog.effort("c", "standard"), "standard")
        self.assertIsNone(catalog.effort("unknown"))


class ChatGPTProviderOptimizationTests(_ChatGPTProviderFixture, unittest.IsolatedAsyncioTestCase):
    async def test_small_chat_listing_expands_only_if_project_filter_leaves_too_few_chats(self):
        pages = AsyncMock()
        self.client.list_page = pages
        self.client.model_catalog = lambda: ModelCatalog(ModelCatalogState.AVAILABLE)
        async with self.provider:
            for project_rows, expected_calls in ((14, [(0, 100)]), (16, [(0, 100)])):
                with self.subTest(project_rows=project_rows):
                    items = [
                        {
                            "id": f"chat-{index}",
                            "project_id": "g-p-one" if index < project_rows else None,
                        }
                        for index in range(35)
                    ]
                    pages.reset_mock()
                    pages.side_effect = lambda offset, limit: {
                        "items": items[offset : offset + limit]
                    }
                    chats = await self.provider.chats.list(limit=2)
                    self.assertEqual(
                        [chat.remote_id for chat in chats],
                        [f"chat-{project_rows}", f"chat-{project_rows + 1}"],
                    )
                    self.assertEqual([call.args for call in pages.await_args_list], expected_calls)

    async def test_chat_delete_and_rename_serialize_per_chat_without_blocking_other_chats(self):
        from webaiprovider.resources.chat.chat import AsyncChat

        started = asyncio.Event()
        release = asyncio.Event()
        deleted: list[str] = []
        renamed: list[tuple[str, str]] = []

        async def slow_stream(_request):
            started.set()
            await release.wait()
            yield TextDelta("done")

        async def fake_delete(cid):
            deleted.append(cid)

        async def fake_rename(cid, title):
            renamed.append((cid, title))

        self.client.model_catalog = lambda: ModelCatalog(ModelCatalogState.AVAILABLE)
        self.provider._stream_provider_events = slow_stream
        self.provider._delete_chat_data = fake_delete
        self.provider._rename_chat_data = fake_rename

        async with self.provider:
            chat_a = AsyncChat(title="A", remote_id="chat-a", provider=self.provider)
            chat_b = AsyncChat(title="B", remote_id="chat-b", provider=self.provider)
            send_a = asyncio.create_task(chat_a.responses.create("hi").start())
            await asyncio.wait_for(started.wait(), 1)

            # Different chat B can rename & delete immediately while chat A is streaming.
            await asyncio.wait_for(chat_b.rename("B2"), 1)
            await asyncio.wait_for(chat_b.delete(), 1)
            self.assertEqual(renamed, [("chat-b", "B2")])
            self.assertEqual(deleted, ["chat-b"])

            # Same chat A must wait until its active stream finishes.
            delete_a = asyncio.create_task(chat_a.delete())
            await asyncio.sleep(0.02)
            self.assertFalse(delete_a.done())
            self.assertEqual(deleted, ["chat-b"])

            release.set()
            await asyncio.wait_for(send_a, 1)
            await asyncio.wait_for(delete_a, 1)
            self.assertEqual(deleted, ["chat-b", "chat-a"])

    async def test_project_attachments_create_many_uses_single_batch_bind(self):
        from webaiprovider import Attachment
        from webaiprovider.resources.project.project import AsyncProject

        uploads: list[str] = []
        bind_payloads: list[dict] = []
        reply = []

        async def fake_upload(name, mime, data, *, project_id=None, cancel_event=None):
            del mime, data, project_id, cancel_event
            uploads.append(name)
            return {"id": f"file-{len(uploads)}", "size_bytes": 5}

        async def fake_request(method, url, **kwargs):
            if method == "POST" and url.endswith("/files"):
                bind_payloads.append(kwargs["json"])
                return SimpleNamespace(
                    content=b"{}",
                    json=lambda: {"files": reply},
                )
            raise AssertionError(f"Unexpected request: {method} {url}")

        self.client.model_catalog = lambda: ModelCatalog(ModelCatalogState.AVAILABLE)
        self.client.upload_file = fake_upload
        self.client.request = fake_request
        files = [
            {"file_id": "file-1", "name": "first.txt"},
            {"file_id": "file-2", "name": "second.txt"},
        ]
        sources = [
            Attachment(id="", name=name, content="test", metadata={"kind": "text"})
            for name in ("first.txt", "second.txt")
        ]

        async with self.provider:
            for bind_reply, readback, succeeds in (
                (files[::-1], None, True),
                (files[:1], files, True),
                ([files[0], {"file_id": "other"}], files[:1], False),
                ([files[0], files[0], files[1]], files[:1], False),
            ):
                with self.subTest(bind=bind_reply, succeeds=succeeds):
                    uploads.clear()
                    bind_payloads.clear()
                    reply[:] = bind_reply
                    self.client.get_project = AsyncMock(
                        return_value={"id": "g-p-work", "resources": readback}
                    )
                    project = AsyncProject(
                        title="Work", remote_id="g-p-work", provider=self.provider
                    )
                    if succeeds:
                        created = await project.attachments.create_many(*sources)
                        self.assertEqual([att.id for att in created], ["file-1", "file-2"])
                        self.assertEqual([att.name for att in created], ["first.txt", "second.txt"])
                    else:
                        with self.assertRaisesRegex(ProviderError, "did not confirm all"):
                            await project.attachments.create_many(*sources)
                        self.assertEqual(project.attachments._items, [])
                    self.assertEqual(self.client.get_project.await_count, int(readback is not None))
                    self.assertEqual(len(bind_payloads), 1)
                    self.assertEqual(
                        [item["name"] for item in bind_payloads[0]["files"]],
                        ["first.txt", "second.txt"],
                    )

    async def test_chatgpt_project_batch_cancels_and_joins_inflight_upload_on_failure(self):

        started = asyncio.Event()
        stopped: list[str] = []

        async def upload(name, *_args, **_kwargs):
            if name == "fail.txt":
                await started.wait()
                raise ProviderError("upload failed")
            self.assertEqual(name, "pending.txt")
            started.set()
            try:
                await asyncio.Future()
            finally:
                stopped.append(name)

        self.client.upload_file = upload
        self.client.request = AsyncMock()
        editor = ChatGPTProjectEditor(self.session, lambda _: self.client)
        items = [
            {"kind": "text", "name": name, "text": "test"} for name in ("fail", "pending", "queued")
        ]
        with self.assertRaisesRegex(ProviderError, "upload failed"):
            await asyncio.wait_for(editor.add_sources("g-p-work", items), 1)
        self.assertEqual(stopped, ["pending.txt"])
        self.client.request.assert_not_awaited()

    async def test_list_project_chats_bounds_concurrency_to_four_and_runs_outside_gate(self):
        active = 0
        max_active = 0

        async def fake_list_project_chats(project_id, cursor=None, *, limit=30):
            del cursor
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            try:
                await asyncio.sleep(0)
                return {
                    "items": [
                        {
                            "conversation_id": f"c-{project_id}",
                            "title": project_id,
                            "project_id": project_id,
                        }
                    ],
                    "cursor": None,
                }
            finally:
                active -= 1

        self.client.list_project_chats = fake_list_project_chats
        async with self.provider._gate:
            pages = await asyncio.gather(
                *(self.provider._list_project_chats_data(f"p-{i}") for i in range(6))
            )
        self.assertEqual(max_active, 4)
        self.assertEqual(
            [page.items[0].id for page in pages],
            ["c-p-0", "c-p-1", "c-p-2", "c-p-3", "c-p-4", "c-p-5"],
        )

    async def test_delete_chat_releases_gate_before_confirm_deleted(self):
        self.client.delete_conversation = AsyncMock(return_value=None)
        confirm_started = asyncio.Event()
        release_confirm = asyncio.Event()

        async def blocked_confirm(*_args, **_kwargs):
            confirm_started.set()
            await asyncio.wait_for(release_confirm.wait(), 1.0)

        with patch(
            "webaiprovider.providers.chatgpt.async_provider.async_confirm_deleted",
            side_effect=blocked_confirm,
        ):
            task = asyncio.create_task(self.provider._delete_chat_data("cid-1"))
            await asyncio.wait_for(confirm_started.wait(), 1.0)
            self.assertFalse(self.provider._gate.locked())
            async with self.provider._gate:
                self.assertTrue(self.provider._gate.locked())
            release_confirm.set()
            await task

    async def test_delete_chat_skips_confirm_deleted_when_server_confirms_patch(self):
        self.client.delete_conversation = AsyncMock(return_value=True)
        with patch(
            "webaiprovider.providers.chatgpt.async_provider.async_confirm_deleted",
            side_effect=AssertionError("unexpected confirm_deleted polling"),
        ):
            await self.provider._delete_chat_data("cid-1")

    async def test_project_update_reuses_staged_state_and_confirms_remote_result(self):
        from webaiprovider.resources.message.attachment import Attachment
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        mock_client = Mock()
        mock_client.request = AsyncMock(return_value={})
        mock_client.get_project = AsyncMock(side_effect=AssertionError("unexpected"))
        editor = ChatGPTProjectEditor(
            self.session,
            lambda _session: mock_client,
            client_scope=lambda: mock_client,
        )
        current = NativeProject(
            "p-1",
            "Old Name",
            "Old inst",
            resources=(Attachment("file-1", "doc.txt", "file"),),
            metadata={
                "gizmo": {
                    "display": {"emoji": "sparkles", "theme": "slate"},
                    "memory_scope": "workspace",
                }
            },
        )
        updated_res = await editor.update("p-1", name="New Name", current=current)
        self.assertIsInstance(updated_res, NativeProject)
        self.assertEqual(updated_res.name, "New Name")
        removed_res = await editor.remove_source(
            "p-1",
            current=updated_res,
            resource_id="file-1",
            resource=Attachment("file-1", "doc.txt", "file"),
        )
        self.assertIsInstance(removed_res, NativeProject)
        self.assertEqual(removed_res.resources, ())
        self.assertEqual(mock_client.get_project.call_count, 0)
        mock_client.get_project = AsyncMock(return_value={"resources": []})
        with self.assertRaises(ValueError):
            await editor.remove_source(
                "p-1",
                resource_id="foreign-id",
                resource=Attachment("file-1", "doc.txt", "file"),
            )
        self.assertEqual(mock_client.get_project.call_count, 1)
        self.assertEqual(mock_client.request.call_args_list[0].args[0], "PATCH")
        self.assertEqual(mock_client.request.call_args_list[0].kwargs["json"]["emoji"], "sparkles")
        self.assertEqual(mock_client.request.call_args_list[1].args[0], "DELETE")

        states = [
            {
                "id": "p-1",
                "name": "Old Name",
                "instructions": "Keep brief",
                "metadata": {"display": {"emoji": "sparkles"}, "memory_scope": "workspace"},
                "resources": [],
            },
            {
                "id": "p-1",
                "name": "Updated Name",
                "instructions": "Keep brief",
                "metadata": {"display": {"emoji": "sparkles"}, "memory_scope": "workspace"},
                "resources": [],
            },
        ]
        self.client.list_projects = AsyncMock(return_value={"items": [states[0]], "cursor": None})
        get_calls = 0

        async def fake_get_project(project_id):
            del project_id
            nonlocal get_calls
            get_calls += 1
            return states[1]

        self.client.get_project = fake_get_project
        self.client.request = AsyncMock(return_value={})
        self.provider._prepare_session = AsyncMock()
        await self.provider.open()
        try:
            loaded = await self.provider.projects.list(limit=None)
            result = await self.provider.projects.update(loaded[0], title="Updated Name")
            self.assertEqual(result.title, "Updated Name")
            self.assertEqual(get_calls, 0)
        finally:
            await self.provider.close()

    async def test_reads_and_metadata_ops_bypass_send_gate_and_list_chats_has_no_idle_sleep(self):
        self.client.list_page = lambda offset=0, limit=28: {
            "items": [{"id": f"c-{offset}", "title": "Chat", "messages": []}]
        }
        self.client.get_conversation = lambda cid: {"id": cid, "title": "Chat", "messages": []}
        self.client.get_project = lambda pid: {
            "id": pid,
            "name": "Proj",
            "instructions": "",
            "resources": [],
            "metadata": {},
        }
        self.client.model_catalog = lambda refresh=False: ModelCatalog(ModelCatalogState.AVAILABLE)
        self.client.rename_conversation = lambda *_args: None
        self.client.move_conversation = lambda *_args: {"project_id": None}

        async with self.provider._gate:
            with patch("asyncio.sleep", side_effect=AssertionError("unexpected idle sleep")):
                page1 = await asyncio.wait_for(self.provider._list_chats_data(limit=1), 1.0)
                page2 = await asyncio.wait_for(
                    self.provider._list_chats_data(cursor="1", limit=1), 1.0
                )
            chat = await asyncio.wait_for(self.provider._get_chat_data("c-0"), 1.0)
            proj = await asyncio.wait_for(self.provider._get_project_data("g-p-1"), 1.0)
            options = await asyncio.wait_for(self.provider._project_options("g-p-1"), 1.0)
            catalog = await asyncio.wait_for(self.provider._load_catalog(), 1.0)
            await asyncio.wait_for(self.provider._rename_chat_data("c-0", "New Title"), 1.0)
            await asyncio.wait_for(
                self.provider._move_chat_data("c-0", None, source_project_id=None), 1.0
            )

        self.assertEqual(page1.items[0].id, "c-0")
        self.assertEqual(page2.items[0].id, "c-1")
        self.assertEqual(chat.id, "c-0")
        self.assertEqual(proj.id, "g-p-1")
        self.assertIn("name", options.fields)
        self.assertEqual(catalog.state, ModelCatalogState.AVAILABLE)

    async def test_persistent_client_reused_across_renames_and_project_uploads(self):
        created = []

        class PersistentNativeClient:
            def __init__(self, session, *, transport=None):
                self.session = session
                self.transport = transport
                self.closed = asyncio.Event()
                self.renamed = []
                self.temporary_conversations = None
                self.pending_branches = None
                created.append(self)

            async def rename_conversation(self, cid, title):
                self.renamed.append((cid, title))

            async def upload_file(self, name, mime, data, **_kwargs):
                return {
                    "id": "file-native",
                    "name": name,
                    "mime_type": mime,
                    "size_bytes": len(data),
                }

            async def request(self, *_args, **_kwargs):
                return {
                    "files": [{"file_id": "file-native", "name": "note.txt", "type": "text/plain"}]
                }

            async def close(self):
                self.closed.set()

        with patch(
            "webaiprovider.providers.chatgpt.async_provider.AsyncChatGPTClient",
            PersistentNativeClient,
        ):
            provider = AsyncChatGPTProvider(
                provider_account("chatgpt"),
                session=self.session,
                client_factory=PersistentNativeClient,
            )
        try:
            await provider._rename_chat_data("c-1", "One")
            await provider._rename_chat_data("c-2", "Two")
            await provider.editor.add_source("g-p-1", kind="text", name="note", text="hello")
            self.assertEqual(len(created), 1)
            self.assertIs(provider._client, created[0])
            self.assertFalse(created[0].closed.is_set())
        finally:
            await provider.close()
        self.assertTrue(created[0].closed.is_set())
        self.assertIsNone(provider._client)

    async def test_editor_and_client_scope_share_one_authenticated_native_client(self):
        auth_flags: list[bool] = []
        created: list[ChatGPTClient] = []

        class InspectableClient(ChatGPTClient):
            def __init__(self, session, *, authenticate: bool = True, **kwargs):
                auth_flags.append(authenticate)
                super().__init__(session, authenticate=False, **kwargs)
                self.list_url = "https://chatgpt.com/backend-api/conversations"
                self.headers = {"authorization": "Bearer primary-tok", "oai-device-id": "dev-1"}
                self._conversation_endpoint = "conversation"
                self._conversation_endpoint_verified = True
                created.append(self)

        with patch(
            "webaiprovider.providers.chatgpt.async_provider.AsyncChatGPTClient", InspectableClient
        ):
            provider = AsyncChatGPTProvider(
                provider_account("chatgpt"),
                session=self.session,
            )
            self.assertIs(provider._factory, InspectableClient)
            try:
                async with provider._client_scope() as primary:
                    self.assertEqual(primary.headers["authorization"], "Bearer primary-tok")
                async with provider.editor._use_client() as editor_client:
                    self.assertEqual(auth_flags, [True])
                    self.assertIs(editor_client, primary)
                    self.assertIs(created[0], primary)
            finally:
                await provider.close()

    async def test_resolve_download_reuses_cached_snapshot_and_falls_back_on_miss(self):
        cached_file_chat = {
            "id": "chat-1",
            "messages": [
                {
                    "id": "m-1",
                    "content": {"parts": []},
                    "metadata": {
                        "attachments": [{"id": "file-cached", "name": "a.txt"}],
                    },
                }
            ],
        }
        self.client._cached_conversation = lambda cid: cached_file_chat if cid == "chat-1" else None
        self.client.get_conversation = AsyncMock(
            return_value={
                "id": "chat-1",
                "messages": [
                    {"id": "m-2", "content": {"parts": ["See /mnt/data/report.csv"]}},
                ],
            }
        )
        self.client.request = AsyncMock(
            side_effect=[
                Mock(json=lambda: {"download_url": "https://cdn.example/f", "file_name": "a.txt"}),
                Mock(json=lambda: {"download_url": "https://cdn.example/r"}),
            ]
        )
        source_cached = await self.provider._resolve_download(
            "file-service://file-cached", "chat-1"
        )
        self.assertEqual(source_cached.filename, "a.txt")
        self.client.get_conversation.assert_not_awaited()

        source_miss = await self.provider._resolve_download(
            "sandbox:/mnt/data/report.csv", "chat-1"
        )
        self.assertEqual(source_miss.url, "https://cdn.example/r")
        self.client.get_conversation.assert_awaited_once_with("chat-1")

    async def test_multi_attachment_upload_runs_concurrently_preserving_order(self):
        entered = 0
        both_entered = asyncio.Event()

        async def upload_file(name, mime, data, **_kwargs):
            del mime, data
            nonlocal entered
            entered += 1
            if entered == 2:
                both_entered.set()
            await asyncio.wait_for(both_entered.wait(), timeout=1.0)
            return {"id": f"id-{name}", "name": name}

        self.client.upload_file = upload_file
        request = ProviderRequest(
            "hi",
            attachments=(
                {"bytes": b"a", "name": "first.txt", "mime_type": "text/plain"},
                {"bytes": b"b", "name": "second.txt", "mime_type": "text/plain"},
            ),
        )
        result = await async_prepare_chatgpt_attachments(self.client, request)
        self.assertEqual([item["id"] for item in result], ["id-first.txt", "id-second.txt"])

    async def test_stream_request_emits_mid_stream_bound_once_and_skips_trailing_duplicate(self):
        self.client.ensure_authenticated = AsyncMock()
        seen_callbacks = []

        async def fake_send_prompt(_cid, _prompt, *, on_event=None, **_kwargs):
            seen_callbacks.append(on_event)
            self.assertTrue(getattr(on_event, "_accepts_bound", False))
            on_event({"type": "bound", "conversation_id": "c_mid"})
            on_event({"type": "delta", "text": "hello", "channel": "final"})
            on_event({"type": "bound", "conversation_id": "c_mid"})
            on_event({"type": "response_complete"})
            return (
                "c_mid",
                {"id": "a1", "role": "assistant", "text": "hello", "created_at": 1.0},
                None,
            )

        self.client.send_prompt = fake_send_prompt
        self.provider._prepare_session = AsyncMock()
        events = [
            event async for event in self.provider._stream_provider_events(ProviderRequest("Hi"))
        ]
        self.assertEqual(len(seen_callbacks), 1)
        self.assertEqual(
            [e for e in events if isinstance(e, ConversationBound)], [ConversationBound("c_mid")]
        )
        self.assertIsInstance(events[0], ConversationBound)
        self.assertEqual(events[1], TextDelta("hello", "final"))
        self.assertIn(ResponseComplete(), events)

    async def test_pending_branch_first_send_binds_real_id_without_get_conversation(self):
        requests_seen = []
        sent_payloads = []
        persisted_conversation = {
            "conversation_id": "c_persisted_branch",
            "title": "Source Chat",
            "default_model_slug": "gpt-4o-mini",
            "messages": [
                {
                    "id": "m_user_branch",
                    "author": {"role": "user"},
                    "content": {"content_type": "text", "parts": ["Branch prompt"]},
                },
                {
                    "id": "m_branch_reply",
                    "author": {"role": "assistant"},
                    "channel": "final",
                    "content": {"content_type": "text", "parts": ["BRANCH"]},
                },
            ],
        }
        root_conversation = {
            "conversation_id": "c_root",
            "title": "Source Chat",
            "default_model_slug": "gpt-4o-mini",
            "current_node": "m_root_a",
            "messages": [
                {
                    "id": "m_root_u",
                    "author": {"role": "user"},
                    "content": {"content_type": "text", "parts": ["Hello"]},
                },
                {
                    "id": "m_root_a",
                    "author": {"role": "assistant"},
                    "channel": "final",
                    "content": {"content_type": "text", "parts": ["Hi"]},
                },
            ],
        }
        preview_conversation = {
            "conversation_id": "preview-branch-1",
            "title": "Source Chat",
            "default_model_slug": "gpt-4o-mini",
            "current_node": "node-a",
            "mapping": {
                "node-a": {
                    "parent": None,
                    "message": {
                        "id": "m_root_a",
                        "author": {"role": "assistant"},
                        "channel": "final",
                        "content": {"content_type": "text", "parts": ["Hi"]},
                    },
                }
            },
        }

        def handler(request: FakeHTTPRequest) -> FakeHTTPResponse:
            requests_seen.append((request.method, request.url.path))
            if request.method == "GET" and request.url.path == "/backend-api/conversations/c_root":
                return FakeHTTPResponse(200, json=root_conversation)
            if (
                request.method == "POST"
                and request.url.path == "/backend-api/conversation/new_branch"
            ):
                return FakeHTTPResponse(200, json={"conversation": preview_conversation})
            if request.method == "POST" and request.url.path == "/backend-api/f/conversation":
                payload = json.loads(request.content)
                sent_payloads.append(payload)
                persisted_conversation["messages"][0]["id"] = payload["messages"][0]["id"]
                events = [
                    {
                        "conversation_id": "c_persisted_branch",
                        "message": {
                            "id": "m_branch_reply",
                            "author": {"role": "assistant"},
                            "channel": "final",
                            "status": "finished_successfully",
                            "content": {"content_type": "text", "parts": ["BRANCH"]},
                        },
                    }
                ]
                body = "".join(f"data: {json.dumps(e)}\n\n" for e in events) + "data: [DONE]\n\n"
                return FakeHTTPResponse(
                    200,
                    headers={"content-type": "text/event-stream"},
                    text=body,
                )
            if (
                request.method == "GET"
                and request.url.path == "/backend-api/conversations/c_persisted_branch"
            ):
                return FakeHTTPResponse(200, json=persisted_conversation)
            raise AssertionError(f"Unexpected request: {request.method} {request.url.path}")

        catalog = ModelCatalog(
            ModelCatalogState.AVAILABLE,
            (Model("gpt-4o-mini", "GPT-4o mini"),),
            default_model="gpt-4o-mini",
        )
        mock_http = FakeAsyncHTTPClient(handler)

        def make_client(session, **_kwargs):
            client = ChatGPTClient(
                session,
                authenticate=False,
                transport=AsyncHTTPTransport(
                    "https://chatgpt.com", client_getter=lambda: mock_http
                ),
            )
            client.headers = {"authorization": "Bearer token", "cookie": "cf=ok"}
            client.list_url = "https://chatgpt.com/backend-api/conversations"
            client.ensure_authenticated = AsyncMock()
            client.create_sentinel_headers = AsyncMock(return_value={})
            client.trigger_sentinel_prewarm = lambda: None
            client.model_catalog = AsyncMock(return_value=catalog)
            client._conversation_endpoint_verified = True
            return client

        provider = AsyncChatGPTProvider(
            provider_account("chatgpt"),
            session=self.session,
            client_factory=make_client,
        )
        provider._prepare_session = AsyncMock()
        provider._catalog = catalog
        await provider.open()
        try:
            chat = provider.chats._new(title="Source Chat", remote_id="c_root")
            branched = await chat.branch("m_root_a")
            self.assertEqual(branched.remote_id, "preview-branch-1")
            self.assertEqual(
                branched.metadata.get("pending_branch"),
                {"conversation_id": "c_root", "message_id": "m_root_a"},
            )
            response = await branched.responses.create("Branch prompt").start()
            self.assertEqual(response.text, "BRANCH")
            self.assertEqual(branched.remote_id, "c_persisted_branch")
            self.assertNotIn("pending_branch", branched.metadata)
            self.assertNotIn(("GET", "/backend-api/conversations/preview-branch-1"), requests_seen)
            self.assertEqual(len(sent_payloads), 1)
            self.assertNotIn("conversation_id", sent_payloads[0])
            self.assertEqual(sent_payloads[0]["branching_from_conversation_id"], "c_root")
            self.assertEqual(sent_payloads[0]["branching_from_message_id"], "m_root_a")
        finally:
            await provider.close()
            mock_http.close()


# Send and continuation behavior
class SendOptionsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        self.requests = []

        def handle(request):
            body = json.loads(request.content)
            self.requests.append(body)
            n = len(self.requests)
            self.messages = [
                {
                    "id": f"r-{n}",
                    "parent": body["messages"][0]["id"] if body.get("messages") else None,
                    "author": {"role": "assistant"},
                    "channel": "analysis",
                    "content": {"parts": ["Summary"]},
                },
                {
                    "id": f"a-{n}",
                    "parent": f"r-{n}",
                    "author": {"role": "assistant"},
                    "channel": "final",
                    "content": {"parts": ["Answer"]},
                },
            ]
            return FakeHTTPResponse(
                200,
                headers={"content-type": "text/event-stream"},
                text="\n\n".join(
                    [
                        "data: " + json.dumps({"conversation_id": "remote", "message": m})
                        for m in self.messages
                    ]
                    + ["data: [DONE]"]
                )
                + "\n\n",
            )

        self._http_handler = handle
        self.http = FakeAsyncHTTPClient(lambda req: self._http_handler(req))
        self.transport = AsyncHTTPTransport("https://chatgpt.com", client_getter=lambda: self.http)
        self.client = ChatGPTClient(
            BrowserSession(root / "session"),
            authenticate=False,
            transport=self.transport,
        )
        self.addAsyncCleanup(self.client.close)
        self.addAsyncCleanup(self.transport.close)
        self.addCleanup(self.http.close)
        self.client.create_sentinel_headers = AsyncMock(return_value={})
        self.client.trigger_sentinel_prewarm = Mock()
        self.client.model_catalog = AsyncMock(return_value=parse_model_catalog(CATALOG))

    async def test_temporary_multiturn_has_no_history_get(self):
        self.client.get_conversation = AsyncMock(side_effect=AssertionError("no temporary GET"))
        events = []
        remote, conv, _ = await self.client.send_prompt(
            None, "First", model="account-thinking", temporary=True, on_event=events.append
        )
        await self.client.send_prompt(remote, "Second", model="account-thinking", temporary=True)
        self.assertTrue(conv["temporary"])
        self.assertTrue(all(b["history_and_training_disabled"] for b in self.requests))
        self.assertEqual(self.requests[1]["parent_message_id"], "a-1")
        self.assertEqual([e["channel"] for e in events if "channel" in e], ["analysis", "final"])
        self.assertEqual(events[-1]["type"], "response_complete")
        self.assertEqual(len(self.client.temporary_conversations["remote"]["messages"]), 6)
        self.client.get_conversation.assert_not_awaited()

    async def test_conversation_history_uses_flat_message_endpoint(self):
        response = Mock()
        response.json.return_value = {"messages": []}
        with patch.object(
            self.client, "request", new_callable=AsyncMock, return_value=response
        ) as request:
            result = await self.client.get_conversation("remote")

        self.assertEqual(result, {"messages": []})
        request.assert_awaited_once_with(
            "GET", "https://chatgpt.com/backend-api/conversations/remote"
        )

    async def test_conversation_history_falls_back_to_alternate_endpoint_and_remembers_it(self):
        first = Mock()
        first.json.return_value = {"messages": [{"id": "first"}]}
        second = Mock()
        second.json.return_value = {"messages": [{"id": "second"}]}
        with patch.object(
            self.client,
            "request",
            new_callable=AsyncMock,
            side_effect=[RemoteNotFound("missing route"), first, second],
        ) as request:
            initial = await self.client.get_conversation("remote")
            subsequent = await self.client.get_conversation("remote")

        self.assertEqual(initial["messages"][0]["id"], "first")
        self.assertEqual(subsequent["messages"][0]["id"], "second")
        self.assertEqual(
            [call.args[1] for call in request.call_args_list],
            [
                "https://chatgpt.com/backend-api/conversations/remote",
                "https://chatgpt.com/backend-api/conversation/remote",
                "https://chatgpt.com/backend-api/conversation/remote",
            ],
        )

    async def test_missing_conversation_is_reported_after_both_endpoint_forms(self):
        with (
            patch.object(
                self.client,
                "request",
                new_callable=AsyncMock,
                side_effect=[RemoteNotFound("plural"), RemoteNotFound("singular")],
            ) as request,
            self.assertRaisesRegex(RemoteNotFound, "singular"),
        ):
            await self.client.get_conversation("remote")

        self.assertEqual(request.await_count, 2)

    async def test_project_reaches_request_body(self):
        self.client.get_conversation = AsyncMock(
            side_effect=lambda _: {
                "id": "remote",
                "messages": self.requests[-1]["messages"] + self.messages,
            }
        )
        await self.client.send_prompt(
            None, "Project", project_id="g-p-work", model="account-thinking"
        )
        self.assertEqual(self.requests[0]["gizmo_id"], "g-p-work")

    async def test_existing_continuation_uses_complete_stream_without_post_send_get(self):
        previous = {
            "id": "remote",
            "conversation_id": "remote",
            "title": "Existing chat",
            "current_node": "old-assistant",
            "messages": [
                {"id": "old-user", "author": {"role": "user"}},
                {"id": "old-assistant", "author": {"role": "assistant"}},
            ],
        }
        self.client.get_conversation = AsyncMock(return_value=previous)

        remote, conversation, previous_ids = await self.client.send_prompt(
            "remote", "Continue", model="account-thinking"
        )

        self.assertEqual(remote, "remote")
        self.assertEqual(self.client.get_conversation.await_count, 1)
        self.assertEqual(previous_ids, {"old-user", "old-assistant"})
        self.assertEqual(conversation["title"], "Existing chat")
        self.assertEqual(conversation["current_node"], "a-1")
        self.assertEqual(
            [item["id"] for item in conversation["messages"]],
            ["old-user", "old-assistant", self.requests[0]["messages"][0]["id"], "r-1", "a-1"],
        )

    async def test_recent_verified_continuation_reuses_provider_snapshot(self):
        previous = {
            "id": "remote",
            "conversation_id": "remote",
            "title": "Existing chat",
            "current_node": "old-assistant",
            "messages": [
                {"id": "old-user", "author": {"role": "user"}},
                {"id": "old-assistant", "author": {"role": "assistant"}},
            ],
        }
        response = Mock()
        response.json.return_value = previous

        with patch.object(
            self.client, "request", new_callable=AsyncMock, return_value=response
        ) as get_request:
            await self.client.send_prompt("remote", "First", model="account-thinking")
            await self.client.send_prompt("remote", "Second", model="account-thinking")

        self.assertEqual(get_request.await_count, 1)
        self.assertEqual(self.requests[1]["parent_message_id"], "a-1")
        self.assertGreater(self.client._continuation_cache.stats().bytes, 0)
        self.assertEqual(self.client._continuation_cache.stats().entries, 1)

    def test_continuation_cache_ttl_is_measured_from_verification(self):
        conversation = {
            "id": "remote",
            "current_node": "node",
            "messages": [{"id": "user", "content": {"parts": ["text"]}}],
        }
        with patch("webaiprovider.providers.chatgpt.base.time.monotonic", return_value=100.0):
            self.client._remember_conversation(conversation)
        with patch("webaiprovider.providers.chatgpt.base.time.monotonic", return_value=159.0):
            self.assertIs(self.client._cached_conversation("remote"), conversation)
        with patch("webaiprovider.providers.chatgpt.base.time.monotonic", return_value=160.0):
            self.assertIsNone(self.client._cached_conversation("remote"))

    async def test_cached_send_failure_does_not_resubmit_after_streamed_content(self):
        self.client._remember_conversation({"id": "remote", "current_node": "node", "messages": []})
        events = []

        async def fail_after_content(*args, **kwargs):
            del kwargs
            args[3]({"type": "delta", "text": "partial"})
            raise ChatGPTError("stream interrupted")

        self.client.send_prompt_direct = AsyncMock(side_effect=fail_after_content)
        self.client.get_conversation = AsyncMock()
        with self.assertRaisesRegex(ChatGPTError, "stream interrupted"):
            await self.client.send_prompt(
                "remote", "Continue", model="account-thinking", on_event=events.append
            )

        self.client.send_prompt_direct.assert_awaited_once()
        self.client.get_conversation.assert_not_awaited()
        self.assertEqual(events, [{"type": "delta", "text": "partial"}])
        self.assertIsNone(self.client._cached_conversation("remote"))

    async def test_delete_invalidates_verified_continuation(self):
        self.client._remember_conversation({"id": "remote", "current_node": "node", "messages": []})
        response = Mock(content=b"")
        with patch.object(self.client, "request", new_callable=AsyncMock, return_value=response):
            await self.client.delete_conversation("remote")

        self.assertIsNone(self.client._cached_conversation("remote"))

    async def test_actions_bypass_reusable_snapshot(self):
        previous = {
            "id": "remote",
            "current_node": "old-assistant",
            "messages": [
                {"id": "old-user", "author": {"role": "user"}},
                {"id": "old-assistant", "author": {"role": "assistant"}},
            ],
        }
        submitted = {"id": "submitted", "author": {"role": "user"}}
        updated = {
            **previous,
            "current_node": "new-assistant",
            "messages": previous["messages"]
            + [submitted, {"id": "new-assistant", "author": {"role": "assistant"}}],
        }

        for operation in (
            {"action": "retry", "message_id": "old-assistant"},
            {"action": "edit", "message_id": "old-user"},
        ):
            with self.subTest(action=operation["action"]):
                self.client._remember_conversation(previous)
                with (
                    patch.object(
                        self.client,
                        "_cached_conversation",
                        side_effect=AssertionError("message actions must read the provider"),
                    ),
                    patch.object(
                        self.client,
                        "get_conversation",
                        new_callable=AsyncMock,
                        side_effect=[previous, updated],
                    ) as get_conversation,
                    patch.object(
                        self.client,
                        "send_prompt_direct",
                        new_callable=AsyncMock,
                        return_value=("remote", "submitted", submitted, []),
                    ),
                ):
                    await self.client.send_prompt(
                        "remote", "Continue", model="account-thinking", **operation
                    )

                self.assertEqual(get_conversation.await_count, 2)
                self.client._forget_conversation("remote")

    async def test_uncertain_send_invalidates_the_reusable_snapshot(self):
        self.client._remember_conversation({"id": "remote", "current_node": "node", "messages": []})
        with (
            patch.object(
                self.client,
                "send_prompt_direct",
                new_callable=AsyncMock,
                side_effect=RuntimeError("transport failed"),
            ),
            self.assertRaisesRegex(RuntimeError, "transport failed"),
        ):
            await self.client.send_prompt("remote", "Continue", model="account-thinking")

        self.assertIsNone(self.client._cached_conversation("remote"))

    async def test_missing_temporary_context_never_falls_back_to_saved_chat(self):
        self.client.get_conversation = AsyncMock(side_effect=AssertionError("no fallback GET"))
        with self.assertRaises(ChatGPTError):
            await self.client.send_prompt("expired", "Q", temporary=True)
        self.client.get_conversation.assert_not_awaited()
        self.assertEqual(self.requests, [])

    async def test_temporary_sentinel_failure_stops_before_http_or_recipe_capture(self):
        self.client.create_sentinel_headers = AsyncMock(
            side_effect=SentinelCompatibilityError("changed")
        )
        self.client.capture_send_recipe = Mock(
            side_effect=AssertionError("no private browser draft")
        )
        with self.assertRaises(ChatGPTError):
            await self.client.send_prompt(None, "Private", model="account-thinking", temporary=True)
        self.client.capture_send_recipe.assert_not_called()
        self.assertEqual(self.requests, [])

    async def test_temporary_project_fails_before_network(self):
        with self.assertRaises(ChatGPTError):
            await self.client.send_prompt(None, "Q", temporary=True, project_id="g-p-work")
        self.assertEqual(self.requests, [])

    async def test_get_conversation_stops_after_first_404_once_endpoint_verified(self):
        calls = []

        def handle(request):
            calls.append(request.url.path)
            if request.url.path.endswith("/c-ok"):
                return FakeHTTPResponse(
                    200, json={"id": "c-ok", "mapping": {}, "current_node": None}
                )
            return FakeHTTPResponse(404, json={"detail": "Not found"})

        self._http_handler = handle
        self.client._conversation_endpoint_verified = False

        with self.assertRaises(RemoteNotFound):
            await self.client.get_conversation("missing-1")
        self.assertEqual(len(calls), 2)

        calls.clear()
        await self.client.get_conversation("c-ok")
        self.assertTrue(self.client._conversation_endpoint_verified)

        calls.clear()
        with self.assertRaises(RemoteNotFound):
            await self.client.get_conversation("missing-2")
        self.assertEqual(len(calls), 1)

    async def test_send_prompt_direct_gates_bound_event_on_callback_flag(self):
        plain = []
        await self.client.send_prompt_direct(None, "Hi", [], on_event=plain.append)
        self.assertEqual([item["type"] for item in plain], ["delta", "delta", "response_complete"])

        opted = []

        def on_opt_in(item):
            opted.append(item)

        on_opt_in._accepts_bound = True
        await self.client.send_prompt_direct(None, "Hi", [], on_event=on_opt_in)
        self.assertEqual(opted[0], {"type": "bound", "conversation_id": "remote"})
        self.assertEqual(
            [item["type"] for item in opted], ["bound", "delta", "delta", "response_complete"]
        )

    async def test_consume_chat_stream_maps_http_429_to_pre_send_rate_limited(self):
        def handle(_request):
            return FakeHTTPResponse(429, headers={"retry-after": "3"})

        self._http_handler = handle
        with self.assertRaises(RateLimited) as ctx:
            await self.client.send_prompt_direct(None, "Hi", [])
        self.assertEqual(ctx.exception.retry_after, 3.0)
        self.assertIs(ctx.exception.message_submitted, False)


# Sentinel compatibility and challenge execution
def encode_vm_program(instructions: list[list[object]], key: str) -> str:
    raw = json.dumps(instructions, separators=(",", ":"))
    return _btoa(_xor_strings(raw, key))


def run_turnstile(runtime: SentinelRuntime, dx: str) -> str | None:
    return runtime.create_enforcement_tokens(
        {
            "proofofwork": {"required": False},
            "turnstile": {"required": True, "dx": dx},
        }
    )[2]


def make_client() -> ChatGPTClient:
    client = ChatGPTClient.__new__(ChatGPTClient)
    client.headers = {"user-agent": "Mozilla/5.0 (X11; Linux x86_64)"}
    client._authenticated = False
    client._needs_initial_auth = False
    client._sentinel_lock = None
    client._refresh_lock = None
    client._catalog_lock = None
    client._sentinel_runtime = None
    client._sentinel_runtime_key = None
    client._prewarmed_sentinel = None
    client._prewarm_task = None
    client._continuation_cache = OrderedDict()
    client.temporary_conversations = OrderedDict()
    client.pending_branches = OrderedDict()
    client._active_streams = {}
    client._upload_http = None
    client._closed = False
    client._owned_transport = False
    client.transport = Mock()
    return client


class SentinelRuntimeTests(unittest.TestCase):
    def test_enforcement_creates_initial_proof_when_one_was_not_prepared(self):
        with SentinelRuntime(user_agent="Mozilla/5.0") as runtime:
            initial, proof, turnstile = runtime.create_enforcement_tokens(
                {
                    "proofofwork": {"required": False},
                    "turnstile": {"required": False},
                }
            )

        self.assertTrue(initial.startswith("gAAAAAC"))
        self.assertEqual(proof, initial)
        self.assertIsNone(turnstile)

    def test_sentinel_runtime_generates_valid_initial_proof_and_25_slot_fingerprint(self):
        with SentinelRuntime(user_agent="Mozilla/5.0 (X11; Linux x86_64)") as runtime:
            initial = runtime.create_initial_proof()

        self.assertTrue(initial.startswith("gAAAAAC"))
        self.assertTrue(initial.endswith("~S"))
        context = _decode_initial_context(initial)
        self.assertEqual(len(context), 25)
        self.assertEqual(context[0], 3000)
        self.assertIn("GMT", context[1])
        self.assertEqual(context[2], 2_172_649_472)
        self.assertIsInstance(context[3], int)
        self.assertEqual(context[4], "Mozilla/5.0 (X11; Linux x86_64)")
        self.assertIn(context[5], DEFAULT_SENTINEL_SPEC.scripts)
        self.assertIsNone(context[6])
        self.assertEqual(context[7], "en-US")
        self.assertEqual(context[8], "en-US")
        self.assertIsInstance(context[9], int)
        self.assertIn("\u2212", context[10])
        self.assertIn(context[11], DEFAULT_SENTINEL_SPEC.document_keys)
        self.assertIn(context[12], DEFAULT_SENTINEL_SPEC.window_keys)
        self.assertIsInstance(context[13], (int, float))
        self.assertEqual(len(context[14]), 36)
        self.assertEqual(context[15], "")
        self.assertEqual(context[16], 8)
        self.assertIsInstance(context[17], (int, float))
        self.assertEqual(context[18:], [0, 0, 0, 0, 0, 0, 0])

        encoded_payload = initial[len("gAAAAAC") : -len("~S")]
        digest = _fnv1a_avalanche(runtime._requirements_seed + encoded_payload)
        self.assertEqual(digest[0], "0")

    def test_sentinel_runtime_solves_pow_and_turnstile_vm_challenges(self):
        with SentinelRuntime(user_agent="Mozilla/5.0 (X11; Linux x86_64)") as runtime:
            initial = runtime.create_initial_proof()
            program = [
                [2, 20, "navigator"],
                [6, 21, 10, 20],
                [2, 22, "userAgent"],
                [6, 23, 21, 22],
                [2, 24, r"sentinel/[0-9a-z]+/sdk\.js"],
                [11, 25, 24],
                [2, 26, "|"],
                [5, 23, 26],
                [5, 23, 25],
                [7, 3, 23],
            ]
            dx = encode_vm_program(program, initial)
            init_used, proof, turnstile = runtime.create_enforcement_tokens(
                {
                    "prepare_token": "prep-1",
                    "proofofwork": {
                        "required": True,
                        "seed": "turnstile-seed",
                        "difficulty": "f",
                    },
                    "turnstile": {
                        "required": True,
                        "dx": dx,
                    },
                }
            )

        self.assertEqual(init_used, initial)
        self.assertTrue(proof.startswith("gAAAAAB"))
        self.assertTrue(proof.endswith("~S"))
        self.assertIsNotNone(turnstile)
        self.assertEqual(
            _atob(turnstile),
            "Mozilla/5.0 (X11; Linux x86_64)|sentinel/20260810913b/sdk.js",
        )


class SentinelVMTests(unittest.TestCase):
    def test_vm_executes_arithmetic_conditionals_and_subroutines(self):
        with SentinelRuntime(user_agent="Mozilla/5.0") as runtime:
            initial = runtime.create_initial_proof()
            sub_dx = encode_vm_program([[2, 40, "sub-ok"], [7, 3, 40]], initial)
            program = [
                [2, 50, 6],
                [2, 51, 7],
                [33, 52, 50, 51],
                [2, 53, 2],
                [35, 54, 52, 53],
                [27, 54, 53],
                [30, 60, 62, [61], [[2, 63, 1], [5, 61, 63], [8, 62, 61]]],
                [17, 64, 60, 54],
                [2, 65, 20],
                [20, 64, 65, 2, 70, "branch-matched"],
                [2, 71, sub_dx],
                [17, 72, 0, 71],
                [18, 72],
                [2, 73, ":"],
                [5, 70, 73],
                [5, 70, 72],
                [7, 3, 70],
            ]
            dx = encode_vm_program(program, initial)
            turnstile = run_turnstile(runtime, dx)
            self.assertEqual(_atob(turnstile), "branch-matched:sub-ok")

    def test_vm_hides_remote_rejection_details(self):
        with SentinelRuntime(user_agent="Mozilla/5.0") as runtime:
            initial = runtime.create_initial_proof()
            reject_dx = encode_vm_program(
                [[2, 40, "private-rejection-detail"], [7, 4, 40]], initial
            )
            with self.assertRaisesRegex(
                SentinelCompatibilityError, "rejected challenge"
            ) as rejected:
                run_turnstile(runtime, reject_dx)
        self.assertNotIn(_btoa("private-rejection-detail"), str(rejected.exception))

    def test_vm_rejects_malformed_challenge(self):
        with SentinelRuntime(user_agent="Mozilla/5.0") as runtime:
            initial = runtime.create_initial_proof()
            broken_dx = _btoa(_xor_strings("{not-valid-json", initial))
            with self.assertRaisesRegex(SentinelCompatibilityError, "could not be decoded"):
                run_turnstile(runtime, broken_dx)

    def test_vm_rejects_unknown_register(self):
        with SentinelRuntime(user_agent="Mozilla/5.0") as runtime:
            initial = runtime.create_initial_proof()
            unknown_op_dx = encode_vm_program([[17, 80, 999, 50], [7, 3, 80]], initial)
            with self.assertRaisesRegex(
                SentinelCompatibilityError, "Unsupported or non-callable Sentinel VM register"
            ):
                run_turnstile(runtime, unknown_op_dx)

    def test_vm_try_call_raw_captures_type_error_for_noncallable_reg_and_reflect_name(self):
        with SentinelRuntime(user_agent="Mozilla/5.0") as runtime:
            initial = runtime.create_initial_proof()
            program = [
                [2, 20, "Reflect"],
                [6, 21, 10, 20],
                [2, 22, "set"],
                [6, 23, 21, 22],
                [2, 24, "name"],
                [6, 25, 23, 24],
                [13, 30, 17, 31, 999.5],
                [2, 32, "|"],
                [5, 25, 32],
                [5, 25, 30],
                [7, 3, 25],
            ]
            dx = encode_vm_program(program, initial)
            turnstile = run_turnstile(runtime, dx)
            decoded = _atob(turnstile)
            self.assertTrue(decoded.startswith("set|Error: TypeError:"), decoded)

    def test_vm_requires_a_resolved_token(self):
        with SentinelRuntime(user_agent="Mozilla/5.0") as runtime:
            initial = runtime.create_initial_proof()
            unresolved_dx = encode_vm_program([[2, 40, "never-resolved"]], initial)
            with self.assertRaisesRegex(SentinelCompatibilityError, "without resolving a token"):
                run_turnstile(runtime, unresolved_dx)

    def test_vm_rejects_newer_sdk_script_version(self):
        with SentinelRuntime(user_agent="Mozilla/5.0") as runtime:
            initial = runtime.create_initial_proof()
            version_mismatch_dx = encode_vm_program(
                [[2, 24, r"sentinel/20999999xxxx/sdk\.js"], [11, 25, 24], [7, 3, 25]],
                initial,
            )
            with self.assertRaisesRegex(SentinelCompatibilityError, "newer SDK script version"):
                run_turnstile(runtime, version_mismatch_dx)

    def test_vm_enforces_step_limit(self):
        bounded_spec = replace(DEFAULT_SENTINEL_SPEC, max_vm_steps=5)
        with SentinelRuntime(bounded_spec) as bounded_runtime:
            init_bounded = bounded_runtime.create_initial_proof()
            many_steps = [[2, 40, idx] for idx in range(10)]
            loop_dx = encode_vm_program(many_steps, init_bounded)
            with self.assertRaisesRegex(SentinelCompatibilityError, "step limit"):
                run_turnstile(bounded_runtime, loop_dx)


class SentinelSpecTests(unittest.TestCase):
    def test_sentinel_spec_adapts_from_updated_sdk_source(self):
        future_sdk_source = """
        const sdkUrl = "https://chatgpt.com/sentinel/20261201abcd/sdk.js";
        class PowSolver {
          constructor() {
            this.requirementsTokenPrefix = "gAAAAAD";
            this.enforcementTokenPrefix = "gAAAAAE";
            this.maxAttempts = 250000;
          }
          getConfig() {
            return [3000, ...["ai", "webgpu", "quantum"].map(e => Number(e in window))];
          }
          getRequirementsToken() {
            return this.requirementsTokenPrefix + "tok" + "~Z";
          }
          _runCheck(e, t, n, r, o) {
            e[4] = n, e[10] = r;
          }
        }
        Rn.set(99, t => { Cn = 0, e(btoa("" + t)) });
        Rn.set(88, (e, t) => {
          const n = new RegExp(vn.get(t));
          for (const t of document.scripts) { }
        });
        """
        spec = SentinelSpec.from_sdk_source(future_sdk_source)
        self.assertEqual(spec.sdk_version, "20261201abcd")
        self.assertEqual(spec.sdk_url, "https://chatgpt.com/sentinel/20261201abcd/sdk.js")
        self.assertEqual(spec.initial_prefix, "gAAAAAD")
        self.assertEqual(spec.enforcement_prefix, "gAAAAAE")
        self.assertEqual(spec.proof_suffix, "~Z")
        self.assertEqual(spec.max_attempts, 250_000)
        self.assertEqual(spec.nonce_index, 4)
        self.assertEqual(spec.elapsed_index, 10)
        self.assertEqual(spec.window_probe_keys, ("ai", "webgpu", "quantum"))
        self.assertEqual(spec.vm_opcodes["resolve"], 99)
        self.assertEqual(spec.vm_opcodes["match_script"], 88)

        with SentinelRuntime(future_sdk_source, user_agent="Mozilla/5.0") as runtime:
            initial = runtime.create_initial_proof()
            self.assertTrue(initial.startswith("gAAAAAD"))
            self.assertTrue(initial.endswith("~Z"))
            dx = encode_vm_program(
                [
                    [2, 20, r"20261201abcd"],
                    [88, 21, 20],
                    [7, 99, 21],
                ],
                initial,
            )
            _, proof, turnstile = runtime.create_enforcement_tokens(
                {
                    "proofofwork": {
                        "required": True,
                        "seed": "future-seed",
                        "difficulty": "f",
                    },
                    "turnstile": {
                        "required": True,
                        "dx": dx,
                    },
                }
            )
            self.assertTrue(proof.startswith("gAAAAAE"))
            self.assertTrue(proof.endswith("~Z"))
            self.assertEqual(_atob(turnstile), "20261201abcd")


class SentinelCompatibilityTests(unittest.TestCase):
    def test_session_observer_token_execution(self):
        with SentinelRuntime(user_agent="Mozilla/5.0") as runtime:
            initial = runtime.create_initial_proof()
            dx = encode_vm_program([[2, 20, "so-snapshot-ok"], [7, 3, 20]], initial)
            token = runtime.create_session_observer_token({"required": True, "snapshot_dx": dx})
            self.assertEqual(_atob(token), "so-snapshot-ok")
            self.assertIsNone(runtime.create_session_observer_token({"required": False}))

    def test_sentinel_runtime_rejects_proof_after_close(self):
        runtime = SentinelRuntime()
        runtime.close()
        with self.assertRaisesRegex(SentinelCompatibilityError, "closed"):
            runtime.create_initial_proof()

    def test_proof_matches_the_published_sdk_calculation(self):
        context = [
            3000,
            "Fri Oct 30 2026 12:00:00 GMT+0300 (GMT+03:00)",
            None,
            1,
            "Mozilla/5.0 (X11; Linux x86_64)",
            "/sentinel/20260810913b/sdk.js",
            None,
            "en-US",
            "en-US",
            0,
            "",
            [],
            [],
            0,
            "00000000-0000-4000-8000-000000000000",
            "",
            8,
            1761814800000,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ]
        encoded_context = base64.b64encode(
            json.dumps(context, separators=(",", ":"), ensure_ascii=False).encode()
        ).decode("ascii")
        initial = "gAAAAAC" + encoded_context + "~S"
        requirements = {
            "proofofwork": {
                "required": True,
                "seed": "golden-seed",
                "difficulty": "0fffff",
            }
        }
        with patch(
            "webaiprovider.providers.chatgpt.sentinel.proof.time.perf_counter_ns",
            side_effect=[0] + [123_490_000] * 120,
        ):
            proof = generate_enforcement_proof(initial, requirements)

        self.assertTrue(proof.startswith("gAAAAAB"))
        self.assertTrue(proof.endswith("~S"))
        solved_json = json.loads(
            base64.b64decode(proof[len("gAAAAAB") : -len("~S")]).decode("utf-8")
        )
        self.assertEqual(solved_json[9], 123)
        digest = _fnv1a_avalanche("golden-seed" + proof[len("gAAAAAB") : -len("~S")])
        self.assertLessEqual(digest[:6], "0fffff")

    def test_rejects_invalid_initial_context_and_unknown_challenge_shape(self):
        requirements = {
            "proofofwork": {
                "required": True,
                "seed": "seed",
                "difficulty": "0fffff",
            }
        }
        with self.assertRaises(SentinelCompatibilityError):
            generate_enforcement_proof("invalid", requirements)
        with self.assertRaises(SentinelCompatibilityError):
            generate_enforcement_proof("gAAAAACe30=", {"proofofwork": {"required": False}})

    def test_rejects_invalid_server_difficulty_without_weakening_validation(self):
        initial = "gAAAAAC" + base64.b64encode(b"[0,0,0,0,0,0,0,0,0,0]").decode() + "~S"
        requirements = {
            "proofofwork": {
                "required": True,
                "seed": "seed",
                "difficulty": "not-hex",
            }
        }
        with self.assertRaises(SentinelCompatibilityError):
            generate_enforcement_proof(initial, requirements)

    def test_context_serialization_matches_javascript_string_and_number_rules(self):
        self.assertEqual(
            _json_string('quote " slash \\ newline\n'), '"quote \\" slash \\\\ newline\\n"'
        )
        self.assertEqual(_json_string(chr(0xD83D) + chr(0xDE00)), '"😀"')
        self.assertEqual(_json_string(chr(0xD800)), '"\\ud800"')
        self.assertEqual(_js_number(1e20), "100000000000000000000")
        self.assertEqual(_js_number(1e21), "1e+21")
        self.assertEqual(_js_number(1e-6), "0.000001")
        self.assertEqual(_js_number(1e-7), "1e-7")
        self.assertEqual(_js_number(-0.0), "0")


class ChatGPTSentinelClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_client_reuses_one_sentinel_runtime_and_handles_pow_and_turnstile_headers(self):
        client = make_client()
        self.addAsyncCleanup(client.close)
        pow_requirements = {
            "prepare_token": "server-issued-1",
            "token": "sdk-requirements-token",
            "proofofwork": {
                "required": True,
                "seed": "server-seed",
                "difficulty": "ffffffff",
            },
            "turnstile": {"required": False},
        }
        response = Mock()
        response.json.return_value = pow_requirements
        client.request = AsyncMock(return_value=response)

        first = await client.create_sentinel_headers()
        runtime = client._sentinel_runtime
        self.assertIsNotNone(runtime)
        self.assertEqual(
            first["openai-sentinel-chat-requirements-prepare-token"],
            "server-issued-1",
        )
        self.assertTrue(first["openai-sentinel-proof-token"].startswith("gAAAAAB"))
        self.assertNotIn("openai-sentinel-turnstile-token", first)

        async def prepare_with_turnstile(_method, _url, **kwargs):
            initial_sent = kwargs["json"]["p"]
            stage3 = encode_vm_program(
                [[15, 50, 50], [1, 50, 51], [7, 3, 50]],
                "46.17",
            )
            stage2 = encode_vm_program(
                [
                    [2, 40, "Reflect"],
                    [6, 40, 10, 40],
                    [2, 41, "set"],
                    [6, 41, 40, 41],
                    [2, 42, "Object"],
                    [6, 42, 10, 42],
                    [2, 43, "create"],
                    [6, 43, 42, 43],
                    [2, 44, None],
                    [17, 50, 43, 44],
                    [2, 45, "createElement"],
                    [2, 46, "document"],
                    [6, 46, 10, 46],
                    [24, 45, 46, 45],
                    [2, 47, "canvas"],
                    [17, 48, 45, 47],
                    [2, 49, "getContext"],
                    [13, 60, 24, 49, 48, 49],
                    [2, 52, "k"],
                    [2, 53, "turnstile-solved"],
                    [7, 41, 50, 52, 53],
                    [2, 51, "46.17"],
                    [18, 54],
                    [1, 54, 51],
                    [14, 9, 54],
                ],
                "29.93",
            )
            dx = encode_vm_program(
                [
                    [2, 54, stage3],
                    [2, 55, stage2],
                    [2, 56, "29.93"],
                    [18, 55],
                    [1, 55, 56],
                    [14, 9, 55],
                ],
                initial_sent,
            )
            resp = Mock()
            resp.json.return_value = {
                "prepare_token": "server-issued-2",
                "persona": "chatgpt-paid",
                "arkose": {"required": False},
                "proofofwork": {
                    "required": True,
                    "seed": "server-seed-2",
                    "difficulty": "ffffffff",
                },
                "turnstile": {"required": True, "dx": dx},
            }
            return resp

        client.request = AsyncMock(side_effect=prepare_with_turnstile)
        second = await client.create_sentinel_headers()
        self.assertIs(client._sentinel_runtime, runtime)
        self.assertEqual(
            second["openai-sentinel-chat-requirements-prepare-token"],
            "server-issued-2",
        )
        self.assertTrue(second["openai-sentinel-proof-token"].startswith("gAAAAAB"))
        self.assertEqual(
            _xor_strings(_atob(second["openai-sentinel-turnstile-token"]), "46.17"),
            '{"k":"turnstile-solved"}',
        )

    async def test_client_rejects_unsupported_required_challenges_before_submission(self):
        client = make_client()
        self.addAsyncCleanup(client.close)
        for unsupported in (
            {"future_required_challenge": {"required": True}},
            {"arkose": {"required": True}},
            {"unsupported_challenge": {"required": True}},
            {"error": "blocked_by_sentinel"},
        ):
            bad_resp = Mock()
            bad_resp.json.return_value = {
                "prepare_token": "server-issued-3",
                "proofofwork": {"required": False},
                **unsupported,
            }
            client.request = AsyncMock(return_value=bad_resp)
            with self.assertRaises(SentinelCompatibilityError) as raised:
                await client.create_sentinel_headers()
            self.assertFalse(raised.exception.message_submitted)
            self.assertEqual(raised.exception.code, "chatgpt_sentinel_incompatible")

    async def test_client_maps_prepare_rejection_to_pre_submit_compatibility_error(self):
        client = make_client()
        self.addAsyncCleanup(client.close)
        client.request = AsyncMock(
            side_effect=ChatGPTError(
                "ChatGPT returned HTTP 403 for POST /backend-api/sentinel/chat-requirements/prepare"
            )
        )
        with self.assertRaises(SentinelCompatibilityError) as rejected_prepare:
            await client.create_sentinel_headers()
        self.assertFalse(rejected_prepare.exception.message_submitted)
        self.assertEqual(rejected_prepare.exception.code, "chatgpt_sentinel_incompatible")

    async def test_client_allows_session_observer_requirements(self):
        client = make_client()
        self.addAsyncCleanup(client.close)
        response = Mock()
        response.json.return_value = {
            "prepare_token": "server-issued-so",
            "proofofwork": {"required": False},
            "turnstile": {"required": False},
            "so": {"required": True, "collector_dx": "dx1", "snapshot_dx": "dx2"},
        }
        client.request = AsyncMock(return_value=response)
        with patch.object(
            SentinelRuntime, "create_session_observer_token", return_value="so-token"
        ) as create_so:
            headers = await client.create_sentinel_headers()
        create_so.assert_called_once()
        self.assertEqual(create_so.call_args.args, (response.json.return_value["so"],))
        self.assertTrue(create_so.call_args.kwargs["initial_proof"].startswith("gAAAAAC"))
        self.assertEqual(
            headers["openai-sentinel-chat-requirements-prepare-token"],
            "server-issued-so",
        )
        self.assertEqual(headers["openai-sentinel-so-token"], "so-token")

    async def test_client_rejects_malformed_session_observer_requirements(self):
        client = make_client()
        self.addAsyncCleanup(client.close)
        response = Mock()
        response.json.return_value = {
            "prepare_token": "server-issued-so",
            "proofofwork": {"required": False},
            "so": {"required": True},
        }
        client.request = AsyncMock(return_value=response)
        with self.assertRaises(SentinelCompatibilityError):
            await client.create_sentinel_headers()

    async def test_client_prewarm_caches_headers_and_reuses_them(self):
        client = make_client()
        self.addAsyncCleanup(client.close)
        response = Mock()
        response.json.return_value = {
            "prepare_token": "server-issued-prewarm",
            "proofofwork": {"required": False},
            "turnstile": {"required": False},
        }
        client.request = AsyncMock(return_value=response)
        client.trigger_sentinel_prewarm()
        assert client._prewarm_task is not None
        await asyncio.wait_for(client._prewarm_task, 1.0)
        self.assertIsNotNone(client._prewarmed_sentinel)
        headers = await client.create_sentinel_headers()
        self.assertEqual(
            headers["openai-sentinel-chat-requirements-prepare-token"],
            "server-issued-prewarm",
        )
        self.assertIsNone(client._prewarmed_sentinel)

    async def test_prewarm_exchange_is_serialized_with_a_foreground_send(self):
        client = make_client()
        self.addAsyncCleanup(client.close)
        started = asyncio.Event()
        release = asyncio.Event()
        response = Mock()
        response.json.return_value = {
            "prepare_token": "server-issued-prewarm",
            "proofofwork": {"required": False},
            "turnstile": {"required": False},
        }

        async def prepare(*_args, **_kwargs):
            started.set()
            await asyncio.wait_for(release.wait(), 1)
            return response

        client.request = AsyncMock(side_effect=prepare)
        client.trigger_sentinel_prewarm()
        await asyncio.wait_for(started.wait(), 1)
        foreground = asyncio.create_task(client.create_sentinel_headers())
        await asyncio.sleep(0.01)
        self.assertFalse(foreground.done())
        release.set()
        fg_headers = await asyncio.wait_for(foreground, 1)
        self.assertEqual(client.request.await_count, 1)
        self.assertEqual(
            fg_headers["openai-sentinel-chat-requirements-prepare-token"],
            "server-issued-prewarm",
        )

    async def test_client_close_releases_its_sentinel_runtime(self):
        client = make_client()
        client._continuation_cache["conversation"] = object()
        client.temporary_conversations["temporary"] = object()
        response = Mock()
        response.json.return_value = {
            "prepare_token": "server-issued",
            "proofofwork": {"required": False},
            "turnstile": {"required": False},
        }
        client.request = AsyncMock(return_value=response)
        await client.create_sentinel_headers()
        self.assertIsNotNone(client._sentinel_runtime)
        await client.close()
        self.assertIsNone(client._sentinel_runtime)
        self.assertFalse(client._continuation_cache)
        self.assertFalse(client.temporary_conversations)

    def test_solve_pow_matches_reference_fnv1a_across_all_base64_alignments(self):
        from webaiprovider.providers.chatgpt.sentinel.proof import _solve_pow

        seed = "alignment-seed"
        difficulty = "0fff"
        seen_remainders = set()
        for tail in ([], ["extra-tail"]):
            for pad in ("", "x", "xy"):
                context = ["slot0-" + pad, 1, 2, None, 4, 5, 6, 7, 8, None, *tail]
                prefix = ("[" + ",".join(json.dumps(x) for x in context[:3]) + ",").encode("utf-8")
                seen_remainders.add(len(prefix) % 3)

                solved = _solve_pow(context, seed, difficulty, started_ns=time.perf_counter_ns())
                digest = _fnv1a_avalanche(seed + solved)
                self.assertLessEqual(digest[: len(difficulty)], difficulty)

                decoded = json.loads(base64.b64decode(solved).decode("utf-8"))
                self.assertEqual(decoded, context)
                self.assertIsInstance(context[DEFAULT_SENTINEL_SPEC.nonce_index], int)
                self.assertIsInstance(context[DEFAULT_SENTINEL_SPEC.elapsed_index], int)

        self.assertEqual(seen_remainders, {0, 1, 2})

        with self.assertRaises(SentinelCompatibilityError):
            _solve_pow([1, 2], seed, difficulty)
        with self.assertRaises(SentinelCompatibilityError):
            _solve_pow(["a"] * 10, "", difficulty)
        with self.assertRaises(SentinelCompatibilityError):
            _solve_pow(["a"] * 10, seed, "xyz")

    async def test_temporary_turn_triggers_sentinel_prewarm(self):
        client = make_client()
        self.addAsyncCleanup(client.close)
        client._model_catalog = (
            time.monotonic(),
            ModelCatalog(
                ModelCatalogState.AVAILABLE,
                (Model("gpt-5", "GPT-5", (EffortOption("standard", "Standard"),), "standard"),),
            ),
        )
        client.send_prompt_direct = AsyncMock(
            return_value=(
                "temp-conv-1",
                "user-1",
                {"id": "user-1", "author": {"role": "user"}},
                [{"id": "asst-1", "parent": "user-1", "author": {"role": "assistant"}}],
            )
        )
        with patch.object(client, "trigger_sentinel_prewarm") as prewarm:
            resolved_id, conversation, previous_ids = await client.send_prompt(
                None, "Hello temporary", model="gpt-5", temporary=True
            )

        self.assertEqual(resolved_id, "temp-conv-1")
        self.assertEqual(previous_ids, set())
        self.assertIn("temp-conv-1", client.temporary_conversations)
        self.assertEqual(conversation["id"], "temp-conv-1")
        prewarm.assert_called_once_with()

    async def test_rate_limit_cooldown_blocks_subsequent_calls_and_closed_skips_prewarm(self):
        client = make_client()
        self.addAsyncCleanup(client.close)
        http = FakeAsyncHTTPClient(
            lambda _req: FakeHTTPResponse(429, headers={"retry-after": "30"})
        )
        http.request = AsyncMock(return_value=FakeHTTPResponse(429, headers={"retry-after": "30"}))
        client.transport = AsyncHTTPTransport(
            "https://chatgpt.com",
            client_getter=lambda: http,
            headers=client._transport_headers,
        )
        self.addAsyncCleanup(client.transport.close)
        self.addCleanup(http.close)

        with self.assertRaises(RateLimited):
            await client.request("GET", "https://chatgpt.com/backend-api/conversations")
        self.assertEqual(http.request.await_count, 1)

        with self.assertRaises(RateLimited):
            await client.request("GET", "https://chatgpt.com/backend-api/conversations")
        self.assertEqual(http.request.await_count, 1)

        with self.assertRaises(RateLimited):
            await client.create_sentinel_headers()
        self.assertEqual(http.request.await_count, 1)

        await client.close()
        client.trigger_sentinel_prewarm()
        self.assertIsNone(client._prewarmed_sentinel)
        self.assertIsNone(client._prewarm_task)

    async def test_close_does_not_wait_for_inflight_prewarm_and_guards_unsafe_vm_regex(self):
        from webaiprovider.providers.chatgpt.sentinel.vm import _compile_vm_regex

        with self.assertRaisesRegex(SentinelCompatibilityError, "unsafe"):
            _compile_vm_regex("(a+)+$")
        with self.assertRaisesRegex(SentinelCompatibilityError, "length"):
            _compile_vm_regex("a" * 300)
        self.assertIsNotNone(_compile_vm_regex("([a-z+]+)"))

        with self.assertRaises(SentinelCompatibilityError):
            SentinelSpec.from_sdk_source(
                "_runCheck(e,t,r,n){n[0]=r,n[4]=a}",
                sdk_url="https://cdn.example/sentinel/x/sdk.js",
            )

        client = make_client()
        started = asyncio.Event()
        release = asyncio.Event()
        cancelled_prewarm = asyncio.Event()
        closed_runtime = asyncio.Event()
        runtime = Mock()
        runtime.close.side_effect = closed_runtime.set

        async def slow_exchange(_rt, _timing):
            started.set()
            try:
                await asyncio.wait_for(release.wait(), timeout=5.0)
            except asyncio.CancelledError:
                cancelled_prewarm.set()
                raise
            return {"openai-sentinel-proof-token": "gAAAAAB"}

        def create_runtime(*_args):
            client._sentinel_runtime = runtime
            return runtime

        with (
            patch.object(client, "_get_or_create_sentinel_runtime", side_effect=create_runtime),
            patch.object(client, "_create_sentinel_headers_direct", side_effect=slow_exchange),
        ):
            client.trigger_sentinel_prewarm()
            prewarm_task = client._prewarm_task
            assert prewarm_task is not None
            await asyncio.wait_for(started.wait(), timeout=2.0)
            await asyncio.wait_for(client.close(), timeout=2.0)
            self.assertTrue(closed_runtime.is_set())
            await asyncio.wait_for(cancelled_prewarm.wait(), timeout=2.0)

    def test_native_c99_extension_matches_pure_python_fallback_and_solves_live_clock(self):
        from webaiprovider.providers.chatgpt.sentinel import proof

        self.assertIsNotNone(proof._native)
        samples = (
            "",
            "hello",
            "café",
            "中文",
            "seed-\U0001f680",
            "mixed-\xe9-\u4e2d-\U0001f600",
        )
        for text in samples:
            with self.subTest(text=text):
                expected = proof._fnv1a_finalize_py(proof._fnv1a_step_utf16_py(2166136261, text))
                self.assertEqual(proof._fnv1a_avalanche(text), expected)
                with patch.object(proof, "_native", None):
                    self.assertEqual(proof._fnv1a_avalanche(text), expected)

        for data, key in (
            ("HelloSentinel", "k3y"),
            ("café-\u4e2d", "key-\U0001f680"),
            ("\U0001f680", "\x01\x02"),
        ):
            with patch.object(proof, "_native", None):
                py_enc = _xor_strings(data, key)
            c_enc = _xor_strings(data, key)
            self.assertEqual(c_enc, py_enc)
            self.assertEqual(_xor_strings(c_enc, key), data)

        with patch.object(proof, "_native", None), self.assertRaises(SentinelCompatibilityError):
            _xor_strings("abc", "")
        with self.assertRaises(SentinelCompatibilityError):
            _xor_strings("abc", "")

        # Verify patched-clock PoW parity between C extension and Python fallback.
        base_ctx = [1920, "1080", 4000, None, "Mozilla/5.0", "", "script", "dpl", "en-US", None, 1]
        c_ctx = list(base_ctx)
        py_ctx = list(base_ctx)
        with patch.object(proof.time, "perf_counter_ns", side_effect=[0] + [123_490_000] * 120):
            c_token = proof._solve_pow(
                c_ctx, "0.987654321", "00fff", spec=SentinelSpec(max_attempts=20000)
            )
        with (
            patch.object(proof, "_native", None),
            patch.object(proof.time, "perf_counter_ns", side_effect=[0] + [123_490_000] * 120),
        ):
            py_token = proof._solve_pow(
                py_ctx, "0.987654321", "00fff", spec=SentinelSpec(max_attempts=20000)
            )
        self.assertEqual(c_token, py_token)
        self.assertEqual(c_ctx, py_ctx)

        # Verify autonomous live-clock path honors target difficulty and started_ns offset.
        live_ctx = list(base_ctx)
        started_ns = time.perf_counter_ns() - 15_000_000
        live_token = proof._solve_pow(
            live_ctx,
            "0.1234567890abcdef",
            "000fff",
            spec=SentinelSpec(max_attempts=500000),
            started_ns=started_ns,
        )
        self.assertLessEqual(
            int(_fnv1a_avalanche("0.1234567890abcdef" + live_token)[:6], 16), 0xFFF
        )
        self.assertGreaterEqual(live_ctx[9], 14)


if __name__ == "__main__":
    unittest.main()


# Attachment and upload contracts
class AttachmentPolicyContractTests(unittest.TestCase):
    def test_provider_limits_are_runtime_owned_and_distinct(self):
        self.assertEqual(policy_for("gemini").max_files, 10)
        self.assertEqual(policy_for("gemini").max_video_bytes, 2 * 1024 * 1024 * 1024)
        self.assertEqual(policy_for("claude").max_files, 20)
        self.assertEqual(policy_for("claude").max_files_scope, "conversation")
        self.assertEqual(policy_for("chatgpt").max_bytes, 512 * 1024 * 1024)
        self.assertEqual(policy_for("chatgpt").max_image_bytes, 20 * 1024 * 1024)
        self.assertEqual(policy_for("chatgpt").max_spreadsheet_bytes, 50 * 1024 * 1024)
        self.assertIsNone(policy_for("unknown"))


async def _aiter_chunks(chunks):
    for chunk in chunks:
        yield chunk


class ChatGPTStreamingUploadTests(unittest.IsolatedAsyncioTestCase):
    async def test_project_upload_validates_processing_events_before_finalization(self):
        ready = b'{"type":"file.processing.file_ready"}\n'
        completed = b'{"type":"file.processing.completed"}'
        cases = (
            ("completed", (ready, completed), None),
            ("fragmented", (ready + completed[:13], completed[13:] + b"\n"), None),
            ("coalesced", (ready * 100 + completed,), None),
            ("truncated", (ready,), "without completion"),
            ("failed", (b'{"type":"file.processing.failed"}\n',), "could not process"),
            ("invalid", (b"not JSON\n",), "invalid project source event"),
            ("nonobject", (b"[]\n",), "invalid project source event"),
            (
                "oversized",
                (b'{"data":"' + b"x" * (1024 * 1024) + b'"}\n' + completed,),
                "oversized",
            ),
        )
        for label, chunks, error in cases:
            with self.subTest(case=label):
                client = object.__new__(ChatGPTClient)
                client._active_streams = {}
                client._closed = False
                client.request = AsyncMock(
                    return_value=SimpleNamespace(
                        json=lambda: {"file_id": "file-id", "upload_url": "https://upload"}
                    )
                )
                client._upload_request = AsyncMock(
                    return_value=SimpleNamespace(raise_for_status=Mock())
                )
                response = SimpleNamespace(
                    status_code=200,
                    headers={"content-type": "text/event-stream"},
                    aiter_bytes=lambda chunks=chunks: _aiter_chunks(chunks),
                )

                @asynccontextmanager
                async def fake_stream(_response=response):
                    yield _response

                client.transport = SimpleNamespace(stream=Mock(return_value=fake_stream()))
                with self.assertRaisesRegex(ChatGPTError, error) if error else nullcontext():
                    result = await client.upload_file(
                        "source.txt", "text/plain", b"source", project_id="project-id"
                    )
                    self.assertEqual(result["id"], "file-id")

                client.request.assert_awaited_once()
                client.transport.stream.assert_called_once_with(
                    "POST",
                    "/backend-api/files/process_upload_stream",
                    check_status=False,
                    json={
                        "entry_surface": "project_sources",
                        "file_id": "file-id",
                        "file_name": "source.txt",
                        "gizmo_id": "project-id",
                        "index_for_retrieval": True,
                        "mime_type": "text/plain",
                        "use_case": "gizmo",
                    },
                    timeout=ANY,
                )

    async def test_path_upload_streams_chunks_instead_of_reading_the_whole_file(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "large.txt"
            path.write_bytes(b"a" * (2 * 1024 * 1024 + 7))
            client = object.__new__(ChatGPTClient)
            client._active_streams = {}
            client._closed = False
            client.request = AsyncMock(
                side_effect=[
                    SimpleNamespace(
                        json=lambda: {"file_id": "file-id", "upload_url": "https://upload"}
                    ),
                    SimpleNamespace(json=lambda: {"status": "success"}),
                ]
            )
            uploaded = []

            async def put(_method, _url, *, content, **_kwargs):
                async for chunk in content:
                    uploaded.append(chunk)
                return SimpleNamespace(raise_for_status=Mock(), content=b"", status_code=200)

            client._upload_http = SimpleNamespace(
                is_closed=False, request=AsyncMock(side_effect=put)
            )
            with patch.object(Path, "read_bytes", side_effect=AssertionError("full file read")):
                result = await client.upload_file(path.name, "text/plain", path)
            self.assertEqual(b"".join(uploaded), b"a" * (2 * 1024 * 1024 + 7))
            client._upload_http.request.assert_awaited_once()
            self.assertEqual(result["size_bytes"], path.stat().st_size)

    async def test_path_upload_cancel_stops_later_chunks_and_skips_finalization(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "cancel.txt"
            path.write_bytes(b"a" * (512 * 1024))
            cancel_event = asyncio.Event()
            client = object.__new__(ChatGPTClient)
            client._active_streams = {}
            client._closed = False
            client.request = AsyncMock(
                return_value=SimpleNamespace(
                    json=lambda: {"file_id": "file-id", "upload_url": "https://upload"}
                )
            )

            async def put(_method, _url, *, content, **_kwargs):
                chunks = aiter(content)
                self.assertEqual(len(await anext(chunks)), 256 * 1024)
                cancel_event.set()
                with self.assertRaisesRegex(ChatGPTError, "cancelled"):
                    await anext(chunks)
                return SimpleNamespace(raise_for_status=Mock(), content=b"", status_code=200)

            client._upload_http = SimpleNamespace(
                is_closed=False, request=AsyncMock(side_effect=put)
            )
            with self.assertRaisesRegex(ChatGPTError, "cancelled"):
                await client.upload_file(path.name, "text/plain", path, cancel_event=cancel_event)
            self.assertEqual(client.request.await_count, 1)
            client._upload_http.request.assert_awaited_once()

    async def test_chatgpt_client_rejects_files_above_documented_ceiling_before_network(self):
        client = object.__new__(ChatGPTClient)
        client._active_streams = {}
        client._closed = False
        client.request = AsyncMock()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "too-large.txt"
            with path.open("wb") as stream:
                stream.truncate(512 * 1024 * 1024 + 1)
            with self.assertRaisesRegex(ChatGPTError, "size limit"):
                await client.upload_file(path.name, "text/plain", path)
        client.request.assert_not_awaited()

    async def test_upload_uses_cookie_isolated_client_and_sanitizes_sas_errors(self):
        from curl_cffi.requests.exceptions import ConnectionError as CurlConnectionError

        client = ChatGPTClient.__new__(ChatGPTClient)
        client._closed = False
        client.headers = {"authorization": "Bearer secret-tok", "cookie": "session=supersecret"}

        seen_calls = []

        def fake_send(request) -> FakeHTTPResponse:
            seen_calls.append(dict(request.headers))
            return FakeHTTPResponse(403, request=request)

        client._upload_http = FakeAsyncHTTPClient(fake_send)
        self.addCleanup(client._upload_http.close)

        sas_url = "https://blob.example/container/file?sig=VERYSECRETSIG"
        with self.assertRaisesRegex(ChatGPTError, "HTTP 403") as ctx:
            await client._upload_request(sas_url, content=b"abc")
        self.assertNotIn("VERYSECRETSIG", str(ctx.exception))
        self.assertNotIn("cookie", seen_calls[0])
        self.assertNotIn("authorization", seen_calls[0])

        def failing_send(request) -> FakeHTTPResponse:
            raise CurlConnectionError(f"failed reaching {request.url}")

        failing_http = FakeAsyncHTTPClient(failing_send)
        self.addCleanup(failing_http.close)
        client._upload_http = failing_http
        with self.assertRaisesRegex(ChatGPTError, "upload request failed") as ctx:
            await client._upload_request(sas_url, content=b"abc")
        self.assertNotIn("VERYSECRETSIG", str(ctx.exception))

    async def test_tracked_stream_injects_transport_headers_for_headerless_transport(self):
        captured = []

        class HeaderlessTransport:
            origin = "https://chatgpt.com"

            async def send(self, method, path, **options):
                captured.append(("send", method, path, dict(options.get("headers") or {})))
                return SimpleNamespace(
                    status_code=200,
                    headers={"content-type": "application/json"},
                    json=lambda: {"file_id": "file-id", "upload_url": "https://upload.example/put"},
                )

            @asynccontextmanager
            async def stream(self, method, path, **options):
                captured.append(("stream", method, path, dict(options.get("headers") or {})))
                response = SimpleNamespace(
                    status_code=200,
                    headers={"content-type": "text/event-stream"},
                    aiter_bytes=lambda: _aiter_chunks((b'{"type":"file.processing.completed"}\n',)),
                )
                yield response

        session = SimpleNamespace(cdp_url=None, load=lambda: {}, headers={})
        client = ChatGPTClient(session, transport=HeaderlessTransport(), authenticate=False)
        client.headers = {
            "authorization": "Bearer tok",
            "cookie": "__Secure-next-auth.session-token=secret",
            "user-agent": "UA",
        }
        client._upload_request = AsyncMock(return_value=SimpleNamespace(raise_for_status=Mock()))

        result = await client.upload_file("note.txt", "text/plain", b"hello", project_id="g-p-1")
        self.assertEqual(result["id"], "file-id")
        self.assertEqual(captured[1][0], "stream")
        self.assertEqual(captured[1][2], "/backend-api/files/process_upload_stream")
        stream_headers = captured[1][3]
        self.assertEqual(stream_headers.get("authorization"), "Bearer tok")
        self.assertIn("__Secure-next-auth.session-token=secret", stream_headers.get("cookie", ""))
        self.assertEqual(
            stream_headers.get("x-openai-target-path"), "/backend-api/files/process_upload_stream"
        )
        self.assertEqual(
            stream_headers.get("x-openai-target-route"), "/backend-api/files/process_upload_stream"
        )

    def test_curl_cookie_jar_handles_prefixed_cookies_without_warning(self):
        import warnings

        from curl_cffi.requests import Session as CurlSession

        from webaiprovider.transport.network import curl_cookie_jar

        jar = curl_cookie_jar(
            {
                "__Secure-1PSID": "val1",
                "__Host-next-auth.csrf-token": "val2",
                "oai-did": "device",
            }
        )
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            session = CurlSession(cookies=jar, trust_env=False)
            session.close()
        self.assertEqual([w for w in recorded if "CurlCffiWarning" in w.category.__name__], [])

    async def test_upload_file_and_image_accept_real_transport_response(self):
        from webaiprovider.transport.base import _BufferedResponse

        class SlotTransport:
            origin = "https://chatgpt.com"

            async def send(self, method, path, **options):
                del method, options
                if path == "/backend-api/files":
                    payload = {"file_id": "f-real", "upload_url": "https://upload.example/put"}
                else:
                    payload = {"status": "success"}
                return SimpleNamespace(
                    status_code=200,
                    headers={"content-type": "application/json"},
                    json=lambda p=payload: p,
                )

        png_header = b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\x0dIHDR" + b"\x00\x00\x00\x01" * 2
        session = SimpleNamespace(cdp_url=None, load=lambda: {}, headers={})
        client = ChatGPTClient(session, transport=SlotTransport(), authenticate=False)
        buffered = _BufferedResponse(SimpleNamespace(status_code=201, headers={}), b"")
        self.assertFalse(hasattr(buffered, "raise_for_status"))
        client._upload_request = AsyncMock(return_value=buffered)

        file_res = await client.upload_file("doc.txt", "text/plain", b"hello")
        image_res = await client.upload_file("img.png", "image/png", png_header)
        self.assertEqual(file_res["id"], "f-real")
        self.assertEqual(image_res["id"], "f-real")
        self.assertEqual(client._upload_request.await_count, 2)
