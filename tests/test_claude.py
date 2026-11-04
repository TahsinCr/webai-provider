"""Claude contracts, transport completion and account isolation."""

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from providers import Capability, ConversationBound, ReplaceText, SendRequest
from providers.claude import ClaudeProvider
from providers.claude.records import conversation, conversation_attachment_count, project
from providers.claude.transport import ClaudeTransport
from providers.errors import FeatureUnavailable, ProviderError
from providers.models import FeatureState

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
        self.assertEqual(value.project_id, PROJECT)
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


class ClaudeProviderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.provider = ClaudeProvider(Path("unused"), "account")
        self.provider.transport.authenticated = True
        self.provider.transport.request = AsyncMock()

    async def test_close_releases_transport_when_editor_cleanup_fails(self):
        self.provider.editor.close = AsyncMock(side_effect=RuntimeError("editor close failed"))
        self.provider.transport.close = AsyncMock()

        with self.assertRaisesRegex(RuntimeError, "editor close failed"):
            await self.provider.close()

        self.provider.transport.close.assert_awaited_once_with()

    async def test_attachment_count_uses_the_compact_history_operation(self):
        self.provider.transport.request.return_value = {
            "uuid": CHAT,
            "chat_messages": [
                {"uuid": "u", "attachments": [{"uuid": "file-1"}]},
                {"uuid": "a", "files": [{"id": "file-2"}]},
            ],
        }

        count = await self.provider.conversation_attachment_count(CHAT)

        self.assertEqual(count, 2)
        self.provider.transport.request.assert_awaited_once_with("history", conversation_id=CHAT)

    async def test_dynamic_models_keep_only_discovered_efforts_and_default(self):
        self.provider.transport.request.return_value = {
            "models": [
                {
                    "id": "claude-future",
                    "title": "Future",
                    "efforts": [{"id": "high", "title": "High", "selected": True}],
                }
            ],
            "default_model": "claude-future",
        }
        result = await self.provider.models()
        self.assertEqual(result.state, FeatureState.AVAILABLE)
        self.assertEqual(result.models[0].default_effort, "high")
        self.assertEqual(result.models[0].id, "claude-future")

    async def test_one_model_feature_failure_does_not_remove_working_models(self):
        self.provider.transport.request.return_value = {
            "models": [
                {"id": "claude-main", "title": "Main", "efforts": [], "effort_error": "changed"},
                {
                    "id": "claude-broken",
                    "title": "Broken",
                    "efforts": [],
                    "selection_error": "unavailable",
                },
            ]
        }
        catalog = await self.provider.models()
        self.assertEqual([x.id for x in catalog.chat_models], ["claude-main"])
        self.assertEqual(catalog.chat_models[0].metadata["effort_error"], "changed")

    async def test_chat_and_project_queries_use_scoped_transport(self):
        self.provider.transport.request.return_value = [{"uuid": CHAT, "name": "Chat"}]
        result = await self.provider.list_project_chats(PROJECT)
        self.assertEqual(result.items[0].project_id, PROJECT)
        self.provider.transport.request.assert_awaited_once_with(
            "project_chats", project_id=PROJECT
        )

    async def test_unsupported_creation_options_fail_before_remote_write(self):
        with self.assertRaises(FeatureUnavailable):
            await self.provider.create_project("Name", visibility="shared")
        self.provider.transport.request.assert_not_awaited()

    async def test_delete_and_move_do_not_retry_mutations(self):
        await self.provider.delete_chat(CHAT)
        self.provider.transport.request.assert_awaited_once_with(
            "delete_chat", conversation_id=CHAT
        )
        self.provider.transport.request.reset_mock()
        self.provider.transport.request.side_effect = ProviderError("network")
        with self.assertRaises(ProviderError):
            await self.provider.move_chat(CHAT, PROJECT)
        self.provider.transport.request.assert_awaited_once()

    async def test_truncated_stream_is_not_success(self):
        async def events(*args, **kwargs):
            yield {"type": "bound", "id": CHAT}
            yield {"type": "replace", "text": "Partial", "channel": "final"}

        self.provider.transport.events = events
        with self.assertRaises(ProviderError):
            _ = [
                x
                async for x in self.provider.stream(
                    SendRequest("claude", "account", "local", "Question")
                )
            ]

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
            async def aiter_lines(self):
                for frame in frames:
                    yield "data: " + json.dumps(frame)
                    yield ""

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
        transport = ClaudeTransport(Path("unused"), session=session)
        transport.organization = PROJECT
        transport.catalog = {
            "default_model": "claude-test",
            "models": [{"id": "claude-test", "efforts": []}],
        }
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

        self.provider.transport.events = events
        result = [
            x
            async for x in self.provider.stream(
                SendRequest("claude", "account", "local", "Question")
            )
        ]
        self.assertIsInstance(result[0], ConversationBound)
        self.assertIsInstance(result[1], ReplaceText)
        self.assertEqual(result[1].channel, "analysis")

    async def test_attachments_are_forwarded_as_provider_file_ids(self):
        captured = {}

        async def events(*args, **kwargs):
            captured.update(kwargs)
            yield {"type": "bound", "id": CHAT}
            yield {"type": "done"}

        self.provider.transport.events = events
        _ = [
            x
            async for x in self.provider.stream(
                SendRequest("claude", "account", "local", "Q", attachments=({"id": "file-id"},))
            )
        ]
        self.assertIn(Capability.ATTACHMENTS, self.provider.capabilities)
        self.assertEqual(captured["attachments"], ({"id": "file-id"},))

    async def test_claude_upload_uses_local_file_multipart_and_checks_response(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "notes.pdf"
            path.write_bytes(b"pdf")
            transport = self.provider.transport
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
