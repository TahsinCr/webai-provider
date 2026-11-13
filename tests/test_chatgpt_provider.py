import asyncio
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from tests.provider_fakes import provider_account
from webaiprovider.accounts.status import SessionState
from webaiprovider.base import (
    Capability,
)
from webaiprovider.errors import ProviderError, RateLimited, RemoteNotFound
from webaiprovider.providers.chatgpt import AsyncChatGPTProvider, BrowserSession, SessionPaths
from webaiprovider.providers.chatgpt.client import ChatGPTClient
from webaiprovider.providers.chatgpt.errors import SentinelCompatibilityError
from webaiprovider.resources.common import Choice
from webaiprovider.resources.message.protocol import (
    ProviderRequest,
    TextDelta,
    TextReplacement,
)
from webaiprovider.resources.model import ModelCatalogState
from webaiprovider.resources.model.model import (
    Model,
    ModelCatalog,
)
from webaiprovider.utils.timing import SendTiming, use_send_timing

from .fakes import FakeClient


class ChatGPTProviderTests(unittest.IsolatedAsyncioTestCase):
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
        chat = await self.provider.get_chat("remote")
        self.assertEqual([m.content[0].text for m in chat.messages], ["Thinking", "Spoken answer"])

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.session = BrowserSession(SessionPaths(root / "session.json", root / "profile"))
        self.client = FakeClient(self.session)
        self.provider = AsyncChatGPTProvider(
            provider_account("chatgpt"), session=self.session, client_factory=lambda _: self.client
        )

    async def test_session_snapshot_is_not_mistaken_for_verified_authentication(self):
        self.assertEqual((await self.provider.session_status()).state, SessionState.LOGIN_REQUIRED)
        self.session.paths.session_file.write_text("{}")
        self.assertEqual((await self.provider.session_status()).state, SessionState.UNKNOWN)
        self.assertEqual((await self.provider.authenticate()).state, SessionState.AUTHENTICATED)

    async def test_forget_session_retains_browser_profile(self):
        self.session.paths.session_file.write_text("{}")
        self.session.paths.profile.mkdir()
        result = await self.provider.forget_session()
        self.assertEqual(result.state, SessionState.LOGIN_REQUIRED)
        self.assertFalse(self.session.paths.session_file.exists())
        self.assertTrue(self.session.paths.profile.exists())

    async def test_history_keeps_provenance_thinking_channel_sources_and_native_fields(self):
        page = await self.provider.list_chats(limit=1)
        self.assertEqual(page.next_cursor, "1")
        chat = page.items[0]
        self.assertEqual(chat.origin.provider_id, "chatgpt")
        self.assertEqual(chat.origin.remote_id, "remote")
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

        chat = await self.provider.get_chat("remote")

        self.assertEqual([message.id for message in chat.messages], ["user", "answer"])
        self.assertNotIn("mapping", chat.metadata)
        self.assertEqual(chat.metadata["generation"], {"active": True, "started_at": 1_800_000_000})

    async def test_discovery_creation_and_deletion_are_available(self):
        self.assertIn(Capability.PROJECTS, self.provider.capabilities)
        self.client.model_catalog = lambda: ModelCatalog(ModelCatalogState.AVAILABLE)
        self.client.list_projects = lambda cursor: {"items": [], "cursor": None}
        self.assertEqual(
            (await self.provider._load_model_catalog()).state, ModelCatalogState.AVAILABLE
        )
        self.assertEqual((await self.provider.list_projects()).items, ())
        self.client.create_project = lambda name, instructions, **options: {
            "id": "g-p-created",
            "name": name,
            "instructions": instructions,
            "metadata": options,
        }
        created = await self.provider.create_project("New project")
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
            patch.object(self.provider.session, "launch"),
        ):
            await self.provider.delete_project("g-p-created")
            browser.assert_not_awaited()
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

        await self.provider.create_project("Shared")
        await self.provider.create_project("Private", memory_share=False)
        with self.assertRaises(TypeError):
            await self.provider.create_project("Invalid", memory_share=1)

        self.assertEqual(memory_values, [True, False])

    async def test_request_waits_for_background_preparation(self):
        ready, release = threading.Event(), threading.Event()

        def prepare_catalog():
            ready.set()
            if not release.wait(2):
                raise TimeoutError("test warmup was not released")
            return ModelCatalog(ModelCatalogState.AVAILABLE)

        self.client.model_catalog = prepare_catalog
        warmup = asyncio.create_task(self.provider.prepare())
        self.assertTrue(await asyncio.to_thread(ready.wait, 2))
        history = asyncio.create_task(self.provider.get_chat("remote"))
        await asyncio.sleep(0.02)
        self.assertFalse(history.done())
        release.set()
        await warmup
        self.assertEqual((await history).origin.remote_id, "remote")

    async def test_default_http_client_is_reused_after_startup_warmup(self):
        import httpx

        raw = {"id": "remote", "title": "Example", "messages": []}
        catalog = ModelCatalog(ModelCatalogState.AVAILABLE)
        self.session.load = lambda: ("https://chatgpt.com/", {"authorization": "Bearer test"})
        real_http_client = httpx.Client
        with (
            patch(
                "webaiprovider.providers.chatgpt.client.httpx.Client", wraps=real_http_client
            ) as clients,
            patch.object(ChatGPTClient, "model_catalog", return_value=catalog) as model_catalog,
            patch.object(ChatGPTClient, "list_page", return_value={"items": [raw]}),
            patch.object(ChatGPTClient, "get_conversation", return_value=raw),
            patch.object(ChatGPTClient, "get_project", return_value={"metadata": {}}),
            patch.object(ChatGPTClient, "trigger_sentinel_prewarm") as sentinel_prewarm,
        ):
            provider = AsyncChatGPTProvider(provider_account("chatgpt"), session=self.session)
            await provider.prepare()
            await provider.prepare_for_send()
            sentinel_prewarm.assert_called_once_with()
            await provider._load_model_catalog()
            await provider._load_model_catalog(force=True)
            await provider.list_chats(limit=1)
            await provider.get_chat("remote")
            await provider.project_options("project")
            self.assertEqual(clients.call_count, 1)
            self.assertIsNotNone(provider._http_client)
            self.assertIs(
                provider._http_client.temporary_conversations, provider._temporary_conversations
            )
            provider._temporary_conversations.update({"temporary": {}, "other": {}})
            await provider.forget_temporary("temporary")
            self.assertEqual(list(provider._http_client.temporary_conversations), ["other"])
            model_catalog.assert_called_with(refresh=True)
            await provider.close()
            self.assertIsNone(provider._http_client)

    async def test_project_file_source_is_streamed_using_a_dedicated_client(self):
        from webaiprovider.providers.chatgpt.projects import ChatGPTProjectEditor

        path = Path(self.directory.name) / "source.txt"
        path.write_text("fixture")
        self.client.upload_file = lambda name, mime, data, **kwargs: (
            {"id": "file", "size_bytes": data.stat().st_size}
            if isinstance(data, Path) and data.resolve() == path.resolve()
            else self.fail("expected a disk path")
        )
        requests = []
        self.client.request = lambda method, url, **kwargs: requests.append((method, url, kwargs))
        editor = ChatGPTProjectEditor(self.session, lambda _: self.client)
        with patch("pathlib.Path.read_bytes", side_effect=AssertionError("must stream")):
            await editor.write("add_source", "project", kind="file", path=path)
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
        events = [event async for event in self.provider._stream_request(ProviderRequest("hello"))]
        self.assertIn(TextReplacement("Published summary", "analysis"), events)

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
        events = [event async for event in self.provider._stream_request(ProviderRequest("hello"))]
        self.assertFalse(
            any(
                isinstance(event, TextReplacement) and event.channel == "analysis"
                for event in events
            )
        )

    async def test_stream_produces_common_events_and_releases_transport(self):
        self.client.closed.set()

        async def collect():
            with use_send_timing(timing):
                return [
                    event async for event in self.provider._stream_request(ProviderRequest("hello"))
                ]

        timing = SendTiming()
        await self.provider._gate.acquire()
        stream = asyncio.create_task(collect())
        await asyncio.sleep(0.02)
        self.assertFalse(stream.done())
        self.provider._gate.release()
        events = await stream
        self.assertEqual(events[0], TextDelta("partial"))
        self.assertEqual(events[-1].conversation_id, "remote")
        self.assertTrue(self.client.closed.is_set())
        self.assertGreater(timing.snapshot()[0]["provider_send_gate_wait_ms"], 0)

    async def test_stream_uploads_inline_bytes_attachment_before_sending(self):
        uploaded = []
        sent = []
        self.client.upload_file = lambda name, mime, data, **kwargs: (
            uploaded.append((name, mime, data, kwargs))
            or {"id": "chatgpt-file-1", "name": name, "mime_type": mime, "size_bytes": len(data)}
        )

        def send_prompt(conversation_id, prompt, on_event, **kwargs):
            sent.append(kwargs)
            on_event({"type": "replace", "text": "finished"})
            return "remote", self.client.raw, set()

        self.client.send_prompt = send_prompt
        request = ProviderRequest(
            "read",
            attachments=({"bytes": b"hello", "name": "note.txt", "mime_type": "text/plain"},),
        )
        _ = [event async for event in self.provider._stream_request(request)]
        self.assertEqual(len(uploaded), 1)
        self.assertEqual(uploaded[0][:3], ("note.txt", "text/plain", b"hello"))
        self.assertEqual(sent[0]["attachments"][0]["id"], "chatgpt-file-1")

    async def test_stream_inline_attachment_upload_honors_cancellation(self):
        import threading

        started = threading.Event()
        saw_cancel = False

        def slow_upload(name, mime, data, *, project_id=None, cancel_event=None):
            nonlocal saw_cancel
            started.set()
            saw_cancel = bool(cancel_event and cancel_event.wait(1))
            return {
                "id": "chatgpt-file-1",
                "name": name,
                "mime_type": mime,
                "size_bytes": len(data),
            }

        self.client.upload_file = slow_upload
        self.client.send_prompt = lambda *_args, **_kwargs: self.fail(
            "send_prompt must not run after cancellation"
        )
        request = ProviderRequest(
            "read",
            attachments=({"bytes": b"hello", "name": "note.txt", "mime_type": "text/plain"},),
        )
        stream = self.provider._stream_request(request)
        task = asyncio.create_task(anext(stream))
        await asyncio.to_thread(started.wait, 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        self.assertTrue(saw_cancel)

    async def test_upload_cancellation_signals_transport_and_waits_for_worker(self):
        import threading
        import time

        started, stopped = threading.Event(), threading.Event()
        closed = threading.Event()

        class UploadClient:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                self.close()

            def close(self):
                closed.set()

            def upload_file(self, _name, _mime, _path, *, cancel_event=None):
                started.set()
                while not (cancel_event and cancel_event.is_set()) and not closed.is_set():
                    time.sleep(0.002)
                stopped.set()
                raise RuntimeError("upload stopped")

        client = UploadClient()
        provider = AsyncChatGPTProvider(
            provider_account("chatgpt"),
            session=self.session,
            client_factory=lambda _session: client,
        )
        cancel_event = threading.Event()
        task = asyncio.create_task(
            provider.upload_file(
                Path(self.directory.name) / "cancel.txt", cancel_event=cancel_event
            )
        )
        self.assertTrue(await asyncio.to_thread(started.wait, 2))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        self.assertTrue(cancel_event.is_set())
        self.assertTrue(stopped.is_set())
        self.assertTrue(closed.is_set())

    async def test_rate_limit_survives_provider_and_is_not_retried(self):
        self.client.closed.set()
        self.client.failure = RateLimited(30)
        with self.assertRaises(RateLimited) as raised:
            async for _ in self.provider._stream_request(ProviderRequest("hello")):
                pass
        self.assertEqual(raised.exception.retry_after, 30)

    async def test_unexpected_transport_failure_uses_safe_provider_error(self):
        self.client.closed.set()
        self.client.failure = ValueError("secret transport detail")

        with self.assertRaises(ProviderError) as raised:
            async for _ in self.provider._stream_request(ProviderRequest("hello")):
                pass

        self.assertEqual(raised.exception.code, "chatgpt_error")
        self.assertIsNone(raised.exception.message_submitted)
        self.assertNotIn("secret transport detail", str(raised.exception))

    async def test_sentinel_compatibility_error_keeps_its_type_and_submission_state(self):
        self.client.closed.set()
        failure = SentinelCompatibilityError("private signer detail")
        self.client.failure = failure
        with self.assertRaises(SentinelCompatibilityError) as raised:
            async for _ in self.provider._stream_request(ProviderRequest("hello")):
                pass
        self.assertIs(raised.exception, failure)
        self.assertEqual(raised.exception.code, "chatgpt_sentinel_incompatible")
        self.assertFalse(raised.exception.message_submitted)

    def test_medium_effort_only_selected_when_catalog_supports_it(self):
        catalog = ModelCatalog(
            ModelCatalogState.AVAILABLE,
            (
                Model("a", "A", (Choice("low", "Low"), Choice("medium", "Medium")), "low"),
                Model("b", "B", (Choice("auto", "Automatic"),), "auto"),
                Model("c", "C", (Choice("standard", "Standard"), Choice("medium", "Medium")), None),
            ),
        )
        self.assertEqual(catalog.effort("a"), "medium")
        self.assertEqual(catalog.effort("b"), "auto")
        self.assertIsNone(catalog.effort("c"))
        self.assertEqual(catalog.effort("c", "standard"), "standard")
        self.assertIsNone(catalog.effort("unknown"))


class ChatGPTProviderOptimizationTests(ChatGPTProviderTests):
    async def test_list_project_chats_bounds_concurrency_to_two_and_runs_outside_gate(self):
        import time

        lock = threading.Lock()
        active = 0
        max_active = 0

        def fake_list_project_chats(project_id, cursor=None):
            nonlocal active, max_active
            with lock:
                active += 1
                max_active = max(max_active, active)
            try:
                time.sleep(0.03)
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
                with lock:
                    active -= 1

        self.client.list_project_chats = fake_list_project_chats
        async with self.provider._gate:
            pages = await asyncio.gather(
                *(self.provider.list_project_chats(f"p-{i}") for i in range(4))
            )
        self.assertEqual(max_active, 2)
        self.assertEqual(
            [page.items[0].origin.remote_id for page in pages],
            ["c-p-0", "c-p-1", "c-p-2", "c-p-3"],
        )

    async def test_delete_chat_releases_gate_before_confirm_deleted(self):
        self.client.delete_conversation = lambda _cid: None
        confirm_started = asyncio.Event()
        release_confirm = asyncio.Event()

        async def blocked_confirm(*_args, **_kwargs):
            confirm_started.set()
            await asyncio.wait_for(release_confirm.wait(), 1.0)

        with patch(
            "webaiprovider.providers.chatgpt.provider.confirm_deleted",
            side_effect=blocked_confirm,
        ):
            task = asyncio.create_task(self.provider.delete_chat("cid-1"))
            await asyncio.wait_for(confirm_started.wait(), 1.0)
            self.assertFalse(self.provider._gate.locked())
            async with self.provider._gate:
                self.assertTrue(self.provider._gate.locked())
            release_confirm.set()
            await task

    async def test_project_editor_and_update_skip_redundant_get_project(self):
        from contextlib import nullcontext
        from unittest.mock import Mock

        from webaiprovider.providers.chatgpt.projects import ChatGPTProjectEditor
        from webaiprovider.resources.common import Origin, ResourceData
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        mock_client = Mock()
        mock_client.request = Mock(return_value={})
        mock_client.get_project = Mock(side_effect=AssertionError("unexpected"))
        editor = ChatGPTProjectEditor(
            self.session,
            lambda _session: mock_client,
            client_scope=lambda: nullcontext(mock_client),
        )
        current = NativeProject(
            Origin("chatgpt", "acc", "p-1"),
            "Old Name",
            "Old inst",
            metadata={
                "gizmo": {
                    "display": {"emoji": "sparkles", "theme": "slate"},
                    "memory_scope": "workspace",
                }
            },
        )
        await editor.write("update", "p-1", name="New Name", current=current)
        await editor.write(
            "remove_source",
            "p-1",
            resource_id="file-1",
            resource=ResourceData("file-1", "doc.txt", "file"),
        )
        self.assertEqual(mock_client.get_project.call_count, 0)
        mock_client.get_project = Mock(return_value={"resources": []})
        with self.assertRaises(ValueError):
            await editor.write(
                "remove_source",
                "p-1",
                resource_id="foreign-id",
                resource=ResourceData("file-1", "doc.txt", "file"),
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
        self.client.list_projects = lambda cursor=None: {"items": [states[0]], "cursor": None}
        get_calls = 0

        def fake_get_project(project_id):
            nonlocal get_calls
            get_calls += 1
            return states[0] if get_calls == 1 else states[1]

        self.client.get_project = fake_get_project
        self.client.request = Mock(return_value={})
        self.provider.prepare = AsyncMock()
        await self.provider.open()
        try:
            loaded = await self.provider.projects.refresh()
            result = await self.provider.projects.update(loaded[0], title="Updated Name")
            self.assertEqual(result.title, "Updated Name")
            self.assertEqual(get_calls, 2)
            self.assertFalse(self.provider._staged_project_reads)
        finally:
            await self.provider.close()
