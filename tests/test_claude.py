"""Claude contracts, transport completion and account isolation."""

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import AsyncMock, patch

from tests.fakes import bind_stream
from tests.provider_fakes import claude_model_catalog, provider_account
from webaiprovider.base import (
    Capability,
)
from webaiprovider.errors import ProviderError
from webaiprovider.providers.claude import AsyncClaudeProvider
from webaiprovider.providers.claude.catalogs import parse_model_catalog
from webaiprovider.providers.claude.client import ClaudeClient
from webaiprovider.providers.claude.records import (
    conversation,
    conversation_attachment_count,
    project,
)
from webaiprovider.resources.message.protocol import (
    ConversationBound,
    ProviderRequest,
    TextReplacement,
)
from webaiprovider.resources.model import ModelCatalogState

CHAT = "12345678-abcd-1234-abcd-123456789abc"
PROJECT = "87654321-abcd-1234-abcd-123456789abc"


class ClaudeRecordsTests(unittest.TestCase):
    def test_history_separates_public_thinking_and_retains_resources(self):
        data = {
            "uuid": CHAT,
            "name": "Chat",
            "project_uuid": PROJECT,
            "chat_messages": [
                {
                    "uuid": "u",
                    "sender": "human",
                    "content": [{"type": "text", "text": "Question"}],
                    "files": [{"uuid": "f", "file_name": "reference.pdf"}],
                },
                {
                    "uuid": "a",
                    "sender": "assistant",
                    "content": [
                        {"type": "thinking", "thinking": "Published summary"},
                        {"type": "text", "text": "Answer"},
                    ],
                },
            ],
        }
        value = conversation(data, "account")
        self.assertEqual(value.origin.provider_id, "claude")
        self.assertIsNotNone(value.project)
        assert value.project is not None
        self.assertEqual(value.project.origin.remote_id, PROJECT)
        self.assertEqual(value.messages[-2].channel, "analysis")
        self.assertEqual(value.messages[-1].content[0].text, "Answer")
        self.assertEqual(value.messages[1].content[0].resource.name, "reference.pdf")

    def test_history_attaches_web_search_results_and_citations_to_final_answer(self):
        data = {
            "uuid": CHAT,
            "name": "Research",
            "chat_messages": [
                {
                    "uuid": "user",
                    "sender": "human",
                    "content": [{"type": "text", "text": "Find sources"}],
                },
                {
                    "uuid": "assistant",
                    "sender": "assistant",
                    "content": [
                        {
                            "type": "server_tool_use",
                            "name": "web_search",
                            "input": {"query": "example"},
                        },
                        {
                            "type": "web_search_tool_result",
                            "content": [
                                {
                                    "type": "web_search_result",
                                    "title": "Search result",
                                    "url": "https://example.com/search",
                                }
                            ],
                        },
                        {
                            "type": "text",
                            "text": "Answer",
                            "citations": [
                                {
                                    "type": "web_search_result_location",
                                    "title": "Cited source",
                                    "url": "https://example.com/cited",
                                    "cited_text": "A cited passage",
                                },
                                {
                                    "type": "web_search_result_location",
                                    "title": "Unsafe",
                                    "url": "javascript:alert(1)",
                                },
                            ],
                        },
                    ],
                },
            ],
        }

        result = conversation(data, "account")
        answer = next(message for message in result.messages if message.content[0].text == "Answer")

        self.assertEqual(
            answer.metadata["sources"],
            (
                {"title": "Search result", "url": "https://example.com/search"},
                {"title": "Cited source", "url": "https://example.com/cited"},
            ),
        )

    def test_unknown_content_blocks_survive_for_other_extensions(self):
        block = {"type": "future_tool", "payload": {"source": "document"}}
        chat = conversation(
            {
                "uuid": CHAT,
                "chat_messages": [{"uuid": "a", "sender": "assistant", "content": [block]}],
            },
            "account",
        )
        self.assertEqual(chat.messages[0].content[0].metadata["native"], block)
        self.assertIsNone(chat.messages[0].content[0].text)

    def test_history_uses_the_selected_branch(self):
        value = conversation(
            {
                "uuid": CHAT,
                "current_leaf_message_uuid": "a2",
                "chat_messages": [
                    {
                        "uuid": "u",
                        "sender": "human",
                        "parent_message_uuid": None,
                        "text": "Question",
                    },
                    {
                        "uuid": "a1",
                        "sender": "assistant",
                        "parent_message_uuid": "u",
                        "text": "Old branch",
                    },
                    {
                        "uuid": "a2",
                        "sender": "assistant",
                        "parent_message_uuid": "u",
                        "text": "Selected branch",
                    },
                ],
            },
            "account",
        )
        self.assertEqual(
            [x.content[0].text for x in value.messages], ["Question", "Selected branch"]
        )

    def test_history_counts_unique_files_across_all_conversation_branches(self):
        value = {
            "uuid": CHAT,
            "current_leaf_message_uuid": "a2",
            "chat_messages": [
                {
                    "uuid": "u",
                    "sender": "human",
                    "parent_message_uuid": None,
                    "files": [{"uuid": "f1", "file_name": "first.txt"}],
                },
                {
                    "uuid": "a1",
                    "sender": "assistant",
                    "parent_message_uuid": "u",
                    "attachments": [{"id": "f2", "name": "old-branch.pdf"}],
                },
                {
                    "uuid": "a2",
                    "sender": "assistant",
                    "parent_message_uuid": "u",
                    "files": [{"uuid": "f3", "file_name": "selected.png"}],
                },
                {
                    "uuid": "a3",
                    "sender": "assistant",
                    "parent_message_uuid": "u",
                    "attachments": [{"uuid": "f3", "file_name": "selected.png"}],
                },
            ],
        }

        self.assertEqual(conversation_attachment_count(value), 3)

        summary = {"uuid": CHAT, "name": "List item"}
        self.assertIsNone(conversation_attachment_count(summary))
        unidentified_file = {"uuid": CHAT, "chat_messages": [{"files": [{"name": "unknown"}]}]}
        self.assertIsNone(conversation_attachment_count(unidentified_file))

    def test_project_instructions_and_sources_are_provider_neutral(self):
        value = project(
            {
                "uuid": PROJECT,
                "name": "Research",
                "prompt_template": "Instructions",
                "docs": [{"uuid": "f", "file_name": "source.pdf"}],
            },
            "account",
        )
        self.assertEqual(value.instructions, "Instructions")
        self.assertEqual(value.resources[0].name, "source.pdf")
        self.assertEqual(value.metadata["kind"], "project")


class AsyncClaudeProviderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.provider = AsyncClaudeProvider(provider_account("claude", Path("unused"), "account"))
        self.provider.client.authenticated = True
        self.provider.client.request = AsyncMock()

    async def test_close_releases_transport_when_editor_cleanup_fails(self):
        self.provider.editor.close = AsyncMock(side_effect=RuntimeError("editor close failed"))
        self.provider.client.close = AsyncMock()

        with self.assertRaisesRegex(RuntimeError, "editor close failed"):
            await self.provider.close()

        self.provider.client.close.assert_awaited_once_with()

    async def test_attachment_count_uses_the_compact_history_operation(self):
        self.provider.client.request.return_value = {
            "uuid": CHAT,
            "chat_messages": [
                {"uuid": "u", "attachments": [{"uuid": "file-1"}]},
                {"uuid": "a", "files": [{"id": "file-2"}]},
            ],
        }

        count = await self.provider.conversation_attachment_count(CHAT)

        self.assertEqual(count, 2)
        self.provider.client.request.assert_awaited_once_with("history", conversation_id=CHAT)

    async def test_dynamic_models_keep_only_discovered_efforts_and_default(self):
        self.provider.client.request.return_value = parse_model_catalog(
            {
                "model_selector_config": [
                    {
                        "id": "chat",
                        "models": [
                            {
                                "id": "claude-future",
                                "name": "Future",
                                "thinking": {"effort_options": [{"id": "high", "name": "High"}]},
                            }
                        ],
                    }
                ],
                "model_selector_state": [
                    {"id": "chat", "model": "claude-future", "thinking": {"effort": "high"}}
                ],
            }
        )
        result = await self.provider._load_model_catalog()
        self.assertEqual(result.state, ModelCatalogState.AVAILABLE)
        self.assertEqual(result.models[0].default_effort, "high")
        self.assertEqual(result.models[0].id, "claude-future")

    async def test_chat_and_project_queries_use_scoped_transport(self):
        self.provider.client.request.return_value = [{"uuid": CHAT, "name": "Chat"}]
        result = await self.provider.list_project_chats(PROJECT)
        self.assertIsNotNone(result.items[0].project)
        assert result.items[0].project is not None
        self.assertEqual(result.items[0].project.origin.remote_id, PROJECT)
        self.provider.client.request.assert_awaited_once_with("project_chats", project_id=PROJECT)

    async def test_chatgpt_project_option_is_not_exposed_by_claude(self):
        with self.assertRaises(TypeError):
            await self.provider.create_project("Name", memory_share=False)
        self.provider.client.request.assert_not_awaited()

    async def test_delete_and_move_do_not_retry_mutations(self):
        await self.provider.delete_chat(CHAT)
        self.provider.client.request.assert_awaited_once_with("delete_chat", conversation_id=CHAT)
        self.provider.client.request.reset_mock()
        self.provider.client.request.side_effect = ProviderError("network")
        with self.assertRaises(ProviderError):
            await self.provider.move_chat(CHAT, PROJECT)
        self.provider.client.request.assert_awaited_once()

    async def test_truncated_stream_is_not_success(self):
        async def events(*args, **kwargs):
            yield {"type": "bound", "id": CHAT}
            yield {"type": "replace", "text": "Partial", "channel": "final"}

        self.provider.client.events = events
        with self.assertRaises(ProviderError):
            _ = [x async for x in self.provider._stream_request(ProviderRequest("Question"))]

    async def test_transport_attaches_web_search_results_and_citations(self):
        frames = [
            {
                "type": "message_start",
                "message": {
                    "uuid": "answer",
                    "parent_uuid": "user",
                    "model": "claude-test",
                },
            },
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {
                    "type": "web_search_tool_result",
                    "content": [
                        {
                            "type": "web_search_result",
                            "title": "Search result",
                            "url": "https://example.com/search",
                        }
                    ],
                },
            },
            {
                "type": "content_block_start",
                "index": 1,
                "content_block": {
                    "type": "text",
                    "text": "Answer",
                    "citations": [
                        {
                            "type": "web_search_result_location",
                            "title": "Cited source",
                            "url": "https://example.com/cited",
                        }
                    ],
                },
            },
            {"type": "message_stop"},
        ]

        class Response:
            status_code = 200
            headers: ClassVar[dict[str, str]] = {}

            async def aiter_content(self):
                for frame in frames:
                    yield ("data: " + json.dumps(frame) + "\n").encode()
                    yield ("" + "\n").encode()

        class Stream:
            async def __aenter__(self):
                return Response()

            async def __aexit__(self, *_):
                return False

        class Client:
            def stream(self, *_args, **_kwargs):
                return Stream()

        session = SimpleNamespace(
            path="/",
            request=AsyncMock(return_value={}),
            connect=lambda: Client(),
            url=lambda path: path,
            check=lambda _response: None,
        )
        transport = ClaudeClient(Path("unused"), session=bind_stream(session))
        transport.organization = PROJECT
        transport.catalog = claude_model_catalog("claude-test")
        result = [
            event
            async for event in transport.events(
                "send",
                prompt="Question",
                model="claude-test",
                conversation_id=CHAT,
                temporary=False,
                action=None,
                project_id=None,
                effort=None,
            )
        ]
        metadata = next(event for event in result if event["type"] == "metadata")

        self.assertEqual(
            metadata["sources"],
            (
                {"title": "Search result", "url": "https://example.com/search"},
                {"title": "Cited source", "url": "https://example.com/cited"},
            ),
        )

    async def test_stream_preserves_thinking_channels_and_remote_identity(self):
        async def events(*args, **kwargs):
            yield {"type": "bound", "id": CHAT}
            yield {"type": "replace", "text": "Summary", "channel": "analysis"}
            yield {"type": "replace", "text": "Answer", "channel": "final"}
            yield {"type": "done"}

        self.provider.client.events = events
        result = [x async for x in self.provider._stream_request(ProviderRequest("Question"))]
        self.assertIsInstance(result[0], ConversationBound)
        self.assertIsInstance(result[1], TextReplacement)
        self.assertEqual(result[1].channel, "analysis")

    async def test_attachments_are_forwarded_as_provider_file_ids(self):
        captured = {}

        async def events(*args, **kwargs):
            captured.update(kwargs)
            yield {"type": "bound", "id": CHAT}
            yield {"type": "done"}

        self.provider.client.events = events
        _ = [
            x
            async for x in self.provider._stream_request(
                ProviderRequest("Q", attachments=({"id": "file-id"},))
            )
        ]
        self.assertIn(Capability.ATTACHMENTS, self.provider.capabilities)
        self.assertEqual(captured["attachments"], ({"id": "file-id"},))

    async def test_claude_upload_uses_local_file_multipart_and_checks_response(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "notes.pdf"
            path.write_bytes(b"pdf")
            transport = self.provider.client
            transport.organization = PROJECT
            transport.session.request = AsyncMock(return_value={"file_uuid": "remote-file"})
            with patch("curl_cffi.CurlMime") as mime_type:
                result = await transport.upload_file(path)
            self.assertEqual(result["id"], "remote-file")
            transport.session.request.assert_awaited_once()
            self.assertEqual(
                transport.session.request.await_args.args[:2],
                ("POST", f"/api/organizations/{PROJECT}/convert_document"),
            )
            mime_type.return_value.addpart.assert_called_once()
            mime_type.return_value.close.assert_called_once()

            captured = {}

            async def events(_mode, **kwargs):
                captured.update(kwargs)
                yield {"type": "bound", "id": CHAT}
                yield {"type": "done"}

            self.provider.client.events = events
            _ = [
                x
                async for x in self.provider._stream_request(
                    ProviderRequest(
                        "Q",
                        attachments=(
                            {"bytes": b"hello", "name": "notes.txt", "mime_type": "text/plain"},
                        ),
                    )
                )
            ]
            self.assertEqual(captured["attachments"][0]["id"], "remote-file")

            transport.session.request.reset_mock()
            with self.assertRaisesRegex(ValueError, "30 MB"):
                _ = [
                    x
                    async for x in self.provider._stream_request(
                        ProviderRequest(
                            "Q",
                            attachments=(
                                {"bytes": b"hello", "name": "notes.txt", "mime_type": "text/plain"},
                                {"bytes": b"bad", "name": "script.exe"},
                            ),
                        )
                    )
                ]
            transport.session.request.assert_not_awaited()

    async def test_claude_convert_document_extracted_content_is_sent_as_inline_attachment(self):
        doc_response = {
            "file_name": "note.txt",
            "file_size": 29,
            "file_type": "txt",
            "extracted_content": "Verification token: ALPHA-42\n",
        }
        recorded_bodies = []

        class Response:
            status_code = 200
            headers: ClassVar[dict[str, str]] = {}

            async def aiter_content(self):
                for frame in (
                    {
                        "type": "content_block_delta",
                        "delta": {"type": "text_delta", "text": "ALPHA-42"},
                    },
                    {"type": "message_stop"},
                ):
                    yield ("data: " + json.dumps(frame) + "\n\n").encode()

        class Stream:
            def __init__(self, payload):
                recorded_bodies.append(payload)

            async def __aenter__(self):
                return Response()

            async def __aexit__(self, *_):
                return False

        class Client:
            def stream(self, _method, _url, **kwargs):
                return Stream(kwargs.get("json"))

        session = SimpleNamespace(
            path="/",
            request=AsyncMock(side_effect=[doc_response, {"uuid": CHAT}]),
            connect=lambda: Client(),
            url=lambda path: path,
            check=lambda _response: None,
        )
        from webaiprovider.providers.claude.client import ClaudeClient

        client = ClaudeClient(Path("unused"), session=bind_stream(session))
        client.organization = PROJECT
        client.catalog = claude_model_catalog("claude-test")

        with patch("curl_cffi.CurlMime"):
            uploaded = await client.upload_file(
                None,
                content=b"Verification token: ALPHA-42\n",
                name="note.txt",
                mime_type="text/plain",
            )
        self.assertEqual(uploaded["claude_document"], doc_response)

        events = [
            event
            async for event in client.events(
                "send",
                prompt="Read note.txt",
                model="claude-test",
                conversation_id=None,
                temporary=False,
                action=None,
                project_id=None,
                effort=None,
                attachments=(uploaded,),
            )
        ]
        self.assertTrue(any(event["type"] == "done" for event in events))
        self.assertEqual(recorded_bodies[0]["attachments"], [doc_response])
        self.assertEqual(recorded_bodies[0]["files"], [])


class ClaudeOptimizationTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_project_fetches_detail_docs_and_files_concurrently(self):
        import asyncio

        started = []
        release = asyncio.Event()

        async def fake_request(method, path, **_kwargs):
            self.assertEqual(method, "GET")
            started.append(path)
            if len(started) == 3:
                release.set()
            await asyncio.wait_for(release.wait(), 1.0)
            if path.endswith("/docs"):
                return [{"uuid": "doc-1"}]
            if path.endswith("/files"):
                return [{"uuid": "file-1", "file_uuid": "fu-1"}]
            return {"uuid": PROJECT, "name": "Demo"}

        session = SimpleNamespace(
            path="/",
            request=fake_request,
            url=lambda path: path,
            check=lambda _response: None,
        )
        client = ClaudeClient(Path("unused"), session=session)
        client.organization = PROJECT

        detail = await client.request("project", project_id=PROJECT)
        self.assertEqual(len(started), 3)
        self.assertEqual(detail["name"], "Demo")
        self.assertEqual(detail["docs"], [{"uuid": "doc-1"}])
        self.assertEqual(detail["files"], [{"uuid": "file-1", "file_uuid": "fu-1"}])

    async def test_project_editor_remove_source_with_resource_skips_project_get(self):
        from webaiprovider.providers.claude.client import ClaudeProjectEditor
        from webaiprovider.resources.common import ResourceData

        transport = SimpleNamespace(
            base=AsyncMock(return_value="/api/organizations/org"),
            request=AsyncMock(),
            http=SimpleNamespace(request=AsyncMock(return_value={})),
        )
        editor = ClaudeProjectEditor(transport)

        await editor.write(
            "remove_source",
            PROJECT,
            resource_id="f-1",
            resource=ResourceData(
                "f-1", "file.txt", metadata={"native": {"uuid": "f-1", "file_uuid": "fu-1"}}
            ),
        )
        transport.http.request.assert_awaited_once_with(
            "POST",
            f"/api/organizations/org/projects/{PROJECT}/files/delete_many",
            json={"file_uuids": ["fu-1"]},
        )
        transport.request.assert_not_awaited()

        transport.http.request.reset_mock()
        await editor.write(
            "remove_source",
            PROJECT,
            resource_id="d-1",
            resource=ResourceData("d-1", "doc.txt", metadata={"native": {"uuid": "d-1"}}),
        )
        transport.http.request.assert_awaited_once_with(
            "DELETE", f"/api/organizations/org/projects/{PROJECT}/docs/d-1"
        )
        transport.request.assert_not_awaited()

        transport.request = AsyncMock(return_value={"docs": [], "files": []})
        with self.assertRaises(ValueError):
            await editor.write(
                "remove_source",
                PROJECT,
                resource_id="foreign-id",
                resource=ResourceData(
                    "d-1", "doc.txt", metadata={"native": {"uuid": "d-1", "file_uuid": "fu-1"}}
                ),
            )
        transport.request.assert_awaited_once_with("project", project_id=PROJECT)

    async def test_projects_update_reuses_staged_snapshot_without_duplicate_get(self):
        from webaiprovider.collections import Page
        from webaiprovider.resources.common import Origin
        from webaiprovider.resources.project.types import ProjectData as NativeProject

        provider = AsyncClaudeProvider(provider_account("claude", Path("unused")))
        origin = Origin("claude", provider.account_id, PROJECT)
        initial = NativeProject(origin, "Old Name", "Keep brief", metadata={"native": {}})
        updated = NativeProject(origin, "New Name", "Keep brief", metadata={"native": {}})
        provider.prepare = AsyncMock()
        provider.list_projects = AsyncMock(return_value=Page((initial,), None))
        provider.get_project = AsyncMock(side_effect=[initial, updated])
        provider.editor.write = AsyncMock()
        await provider.open()
        try:
            loaded = await provider.projects.refresh()
            result = await provider.projects.update(loaded[0], title="New Name")
            self.assertEqual(result.title, "New Name")
            self.assertEqual(provider.get_project.await_count, 2)
            provider.editor.write.assert_awaited_once_with(
                "update", PROJECT, current=initial, name="New Name"
            )
            self.assertFalse(provider._staged_project_reads)
        finally:
            await provider.close()

    async def test_continuation_cache_normalizes_sources_files_and_handles_invalidations(self):
        from webaiprovider.async_provider import MessageAction
        from webaiprovider.providers.claude.errors import (
            ClaudeRequestNotSent,
            ClaudeStreamError,
        )

        get_paths = []
        post_bodies = []
        stream_batches = iter(
            [
                [
                    {"type": "message_start", "message": {"uuid": "a1", "model": "claude-test"}},
                    {"type": "content_block_start", "index": 0, "content_block": {"type": "text"}},
                    {
                        "type": "content_block_delta",
                        "index": 0,
                        "delta": {
                            "type": "citations_delta",
                            "citation": {"title": "Doc", "url": "https://example.com/a"},
                        },
                    },
                    {
                        "type": "content_block_delta",
                        "index": 0,
                        "delta": {"type": "text_delta", "text": "Answer one"},
                    },
                    {"type": "content_block_stop", "index": 0},
                    {
                        "type": "content_block_start",
                        "index": 1,
                        "content_block": {"type": "tool_use", "name": "present_files"},
                    },
                    {
                        "type": "content_block_delta",
                        "index": 1,
                        "delta": {
                            "type": "input_json_delta",
                            "partial_json": '{"filepaths":["/mnt/user-data/outputs/out.txt"]}',
                        },
                    },
                    {"type": "content_block_stop", "index": 1},
                    {"type": "message_stop"},
                ],
                [
                    {"type": "message_start", "message": {"uuid": "a2", "model": "claude-test"}},
                    {"type": "content_block_start", "index": 0, "content_block": {"type": "text"}},
                    {
                        "type": "content_block_delta",
                        "index": 0,
                        "delta": {"type": "text_delta", "text": "Follow up"},
                    },
                    {"type": "content_block_stop", "index": 0},
                    {"type": "message_stop"},
                ],
                [
                    {"type": "message_start", "message": {"uuid": "a3", "model": "claude-test"}},
                    {"type": "content_block_start", "index": 0, "content_block": {"type": "text"}},
                    {
                        "type": "content_block_delta",
                        "index": 0,
                        "delta": {"type": "text_delta", "text": "Edited"},
                    },
                    {"type": "content_block_stop", "index": 0},
                    {"type": "message_stop"},
                ],
                [
                    {"type": "message_start", "message": {"uuid": "a4", "model": "claude-test"}},
                    {"type": "content_block_start", "index": 0, "content_block": {"type": "text"}},
                    {
                        "type": "content_block_delta",
                        "index": 0,
                        "delta": {"type": "text_delta", "text": "Truncated"},
                    },
                ],
            ]
        )
        history_payloads = iter(
            [
                {
                    "uuid": CHAT,
                    "is_temporary": False,
                    "current_leaf_message_uuid": "root-msg",
                },
                {
                    "uuid": CHAT,
                    "is_temporary": False,
                    "current_leaf_message_uuid": "a2",
                    "chat_messages": [
                        {
                            "uuid": "u1",
                            "sender": "human",
                            "parent_message_uuid": "root-msg",
                            "text": "Old",
                        },
                        {
                            "uuid": "a2",
                            "sender": "assistant",
                            "parent_message_uuid": "u1",
                            "text": "Follow up",
                        },
                    ],
                },
                {
                    "uuid": CHAT,
                    "is_temporary": False,
                    "current_leaf_message_uuid": "a3",
                },
            ]
        )

        async def fake_http_request(method, path, **_kwargs):
            self.assertEqual(method, "GET")
            get_paths.append(path)
            return next(history_payloads)

        class Response:
            status_code = 200
            headers: ClassVar[dict[str, str]] = {}

            def __init__(self, frames):
                self._frames = frames

            async def aiter_content(self):
                for frame in self._frames:
                    yield ("data: " + json.dumps(frame) + "\n\n").encode()

        class Stream:
            def __init__(self, payload):
                post_bodies.append(payload)
                self._frames = next(stream_batches)

            async def __aenter__(self):
                return Response(self._frames)

            async def __aexit__(self, *_):
                return False

        class Client:
            def stream(self, _method, _url, **kwargs):
                return Stream(kwargs.get("json"))

        session = SimpleNamespace(
            path="/",
            request=fake_http_request,
            connect=lambda: Client(),
            url=lambda path: path,
            check=lambda _response: None,
            close=AsyncMock(),
        )
        client = ClaudeClient(Path("unused"), session=bind_stream(session))
        client.organization = PROJECT
        client.catalog = claude_model_catalog("claude-test")

        events1 = [
            e
            async for e in client.events(
                "send",
                prompt="Q1",
                model="claude-test",
                conversation_id=CHAT,
                temporary=False,
                action=None,
                project_id=None,
                effort=None,
            )
        ]
        meta1 = next(e for e in events1 if e["type"] == "metadata")
        self.assertEqual(meta1["sources"], ({"title": "Doc", "url": "https://example.com/a"},))
        self.assertEqual(len(meta1["files"]), 1)
        self.assertEqual(meta1["files"][0]["name"], "out.txt")
        self.assertEqual(len(get_paths), 1)
        self.assertEqual(client._continuations.get(CHAT), ("a1", False))

        _ = [
            e
            async for e in client.events(
                "send",
                prompt="Q2",
                model="claude-test",
                conversation_id=CHAT,
                temporary=False,
                action=None,
                project_id=None,
                effort=None,
            )
        ]
        self.assertEqual(len(get_paths), 1)
        self.assertEqual(post_bodies[1]["parent_message_uuid"], "a1")

        _ = [
            e
            async for e in client.events(
                "send",
                prompt="Edit Q1",
                model="claude-test",
                conversation_id=CHAT,
                temporary=False,
                action=MessageAction.EDIT,
                message_id="u1:0",
                project_id=None,
                effort=None,
            )
        ]
        self.assertEqual(len(get_paths), 2)
        self.assertEqual(client._continuations.get(CHAT), ("a3", False))

        with self.assertRaises(ClaudeRequestNotSent):
            _ = [
                e
                async for e in client.events(
                    "send",
                    prompt="Temp check",
                    model="claude-test",
                    conversation_id=CHAT,
                    temporary=True,
                    action=None,
                    project_id=None,
                    effort=None,
                )
            ]
        self.assertEqual(len(get_paths), 3)
        self.assertIsNone(client._continuations.get(CHAT))

        client._continuations.put(CHAT, ("a3", False))
        with self.assertRaises(ClaudeStreamError):
            _ = [
                e
                async for e in client.events(
                    "send",
                    prompt="Truncated",
                    model="claude-test",
                    conversation_id=CHAT,
                    temporary=False,
                    action=None,
                    project_id=None,
                    effort=None,
                )
            ]
        self.assertIsNone(client._continuations.get(CHAT))

        client._continuations.put(CHAT, ("a3", False))
        await client.close()
        self.assertIsNone(client._continuations.get(CHAT))

    async def test_stream_request_uploads_multiple_attachments_bounded_to_two_in_order(self):
        import asyncio

        provider = AsyncClaudeProvider(provider_account("claude", Path("unused")))
        delays = {"f0.txt": 0.04, "f1.txt": 0.0, "f2.txt": 0.03, "f3.txt": 0.01}
        active = 0
        max_active = 0
        captured_attachments = []

        async def fake_upload(_path=None, *, content=None, name=None, mime_type=None):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            try:
                await asyncio.sleep(delays[name])
                return {"kind": "attachment", "data": {"file_name": name, "id": f"id-{name}"}}
            finally:
                active -= 1

        async def fake_events(_mode, **kwargs):
            captured_attachments.extend(kwargs["attachments"])
            yield {"type": "done"}

        provider.client.authenticated = True
        provider.client.catalog = claude_model_catalog("claude-test")
        provider.client.upload_file = fake_upload
        provider.client.events = fake_events
        try:
            events = [
                e
                async for e in provider._stream_request(
                    ProviderRequest(
                        "Upload all",
                        attachments=tuple(
                            {"bytes": b"data", "name": f"f{i}.txt", "mime_type": "text/plain"}
                            for i in range(4)
                        ),
                    )
                )
            ]
            self.assertEqual(events, [])
            self.assertEqual(max_active, 2)
            self.assertEqual(
                [item["data"]["file_name"] for item in captured_attachments],
                ["f0.txt", "f1.txt", "f2.txt", "f3.txt"],
            )
        finally:
            await provider.close()
