"""Grok continuation and browser-cookie contracts."""

import asyncio
import base64
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from providers.api import (
    Capability,
    MessageAction,
    MessageMetadata,
    ProviderError,
    ReplaceText,
    ResponseComplete,
    SendRequest,
    TextDelta,
)
from providers.errors import AccountPaused
from providers.grok import GrokProvider
from providers.grok.records import GrokTextFilter, conversation
from providers.grok.transport import (
    _MAX_LINE_BYTES,
    GrokProjectEditor,
    GrokTransport,
    _iter_bounded_lines,
)
from providers.models import Resource
from providers.profiles import select_cookies


class GrokTransportFixture:
    def __init__(self):
        self.session = SimpleNamespace(authenticated=True)
        self.responses = [
            {"responseId": "user-0", "sender": "human", "parentResponseId": ""},
            {"responseId": "answer-0", "sender": "assistant", "parentResponseId": "user-0"},
        ]
        self.requests = []
        self.bodies = []
        self.response_node_payload = None
        self.model_details = [
            {"modelId": "grok-3", "name": "", "modelMode": "MODEL_MODE_FAST"},
            {"modelId": "grok-paid", "name": "Paid", "modelMode": "MODEL_MODE_EXPERT"},
        ]
        self.turn = 0

    async def request(self, method, path, **options):
        self.requests.append((method, path, options))
        if path == "/rest/models":
            return {"models": self.model_details}
        if path == "/rest/modes":
            return {
                "modes": [
                    {
                        "id": "fast",
                        "title": "Fast",
                        "description": "Quick responses",
                        "availability": {"available": {}},
                        "tags": ["TAG_PRIMARY"],
                    },
                    {
                        "id": "build",
                        "title": "Build",
                        "availability": {"available": {}},
                    },
                    {
                        "id": "auto",
                        "title": "Auto",
                        "availability": {"requiresUpgrade": {"message": "Upgrade"}},
                    },
                    {
                        "id": "expert",
                        "title": "Expert",
                        "availability": {"requiresUpgrade": {"message": "Upgrade"}},
                    },
                    {
                        "id": "unknown",
                        "title": "Unknown availability",
                        "availability": {},
                    },
                ],
                "defaultModeId": "expert",
            }
        if path.endswith("/response-node"):
            if self.response_node_payload is not None:
                return self.response_node_payload
            return {
                "responseNodes": self.responses,
                "inflightResponses": [],
            }
        if path.endswith("/responses"):
            return {"responses": self.responses, "inflightResponses": []}
        raise AssertionError(f"unexpected request path: {path}")

    async def events(self, _path, body):
        self.bodies.append(body)
        self.turn += 1
        user_id = f"user-{self.turn}"
        answer_id = f"answer-{self.turn}"
        parent_id = body["parentResponseId"]
        if body.get("isRegenRequest") is not True:
            self.responses.append(
                {"responseId": user_id, "sender": "human", "parentResponseId": parent_id}
            )
            answer_parent = user_id
        else:
            answer_parent = parent_id
        self.responses.append(
            {"responseId": answer_id, "sender": "assistant", "parentResponseId": answer_parent}
        )
        yield {
            "result": {
                "conversation": {"conversationId": "chat"},
                "response": {
                    "userResponse": {"responseId": user_id},
                    "token": "Answer",
                    "modelResponse": {
                        "responseId": answer_id,
                        "model": "grok-test",
                        "message": "Answer",
                        "partial": False,
                        "webSearchResults": [{"url": "https://example.com", "title": "Source"}],
                    },
                },
            }
        }

    async def close(self):
        return None


class GrokProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_grok_stream_lines_are_bounded_and_preserve_split_sse(self):
        class Response:
            def __init__(self, chunks):
                self.chunks = iter(chunks)
                self.read = 0

            async def aiter_content(self):
                for chunk in self.chunks:
                    self.read += 1
                    yield chunk

        response = Response([b'data: {"answer":', b'"ok"}\r\n\n{"last": true}'])
        lines = [line async for line in _iter_bounded_lines(response)]
        self.assertEqual(lines, ['data: {"answer":"ok"}', "", '{"last": true}'])

        response = Response([b"x" * _MAX_LINE_BYTES, b"x", b"must not be read"])
        with self.assertRaises(ProviderError):
            [line async for line in _iter_bounded_lines(response)]
        self.assertEqual(response.read, 2)

    async def test_grok_stream_marks_expired_session_unauthenticated(self):
        class Stream:
            async def __aenter__(self):
                return SimpleNamespace(status_code=401)

            async def __aexit__(self, *_args):
                return False

        client = SimpleNamespace(stream=Mock(return_value=Stream()))
        session = SimpleNamespace(
            authenticated=True,
            connect=Mock(return_value=client),
            url=Mock(return_value="https://grok.com/rest/app-chat/conversations/chat/responses"),
            check=Mock(side_effect=AccountPaused("expired")),
            persist=AsyncMock(),
        )
        with patch("providers.grok.transport.WebSession", return_value=session):
            transport = GrokTransport(Path("unused"))

        with self.assertRaises(AccountPaused):
            await anext(transport.events("/rest/app-chat/conversations/chat/responses", {}))

        self.assertFalse(session.authenticated)
        session.persist.assert_awaited_once()

    async def test_authentication_uses_protected_user_settings_resource(self):
        session = SimpleNamespace(authenticated=False, request=AsyncMock(return_value={}))
        with patch("providers.grok.transport.WebSession", return_value=session):
            transport = GrokTransport(Path("unused"))
            status = await transport.authenticate()

        self.assertEqual(status, {})
        self.assertTrue(session.authenticated)
        session.request.assert_awaited_once_with("GET", "/rest/user-settings")

    async def test_authentication_rejects_missing_resource_payload(self):
        session = SimpleNamespace(authenticated=False, request=AsyncMock(return_value=None))
        with patch("providers.grok.transport.WebSession", return_value=session):
            transport = GrokTransport(Path("unused"))
            with self.assertRaises(AccountPaused):
                await transport.authenticate()

        self.assertFalse(session.authenticated)

    async def test_continuation_cache_skips_history_and_retry_keeps_user_parent(self):
        provider = GrokProvider(Path("unused"), "test")
        transport = GrokTransportFixture()
        provider.transport = transport

        async def send(prompt="Question", *, action=None, message_id=None, model="fast"):
            request = SendRequest(
                provider_id="grok",
                account_id="test",
                chat_id="local",
                prompt=prompt,
                conversation_id="chat",
                model=model,
                action=action,
                message_id=message_id,
            )
            return [event async for event in provider.stream(request)]

        try:
            first = await send()
            second = await send("Follow-up")
            retried = await send(action=MessageAction.RETRY, message_id="answer-2")
            transport.response_node_payload = {}
            provider._continuations.clear()
            with self.assertRaises(ProviderError):
                await send("Do not send without a valid parent list")
        finally:
            await provider.close()

        self.assertEqual(
            [body["parentResponseId"] for body in transport.bodies],
            ["answer-0", "answer-1", "user-2"],
        )
        retry = transport.bodies[2]
        self.assertEqual(retry["message"], "")
        self.assertTrue(retry["isRegenRequest"])
        self.assertNotIn("requestType", retry)
        for body in transport.bodies:
            self.assertEqual(body["modelMode"], "MODEL_MODE_FAST")
            self.assertEqual(body["modelName"], "grok-3")
        history_paths = [
            path.rsplit("/", 1)[-1]
            for _, path, _ in transport.requests
            if path.endswith(("/response-node", "/responses"))
        ]
        self.assertEqual(history_paths, ["response-node", "responses", "response-node"])
        self.assertEqual(len(transport.bodies), 3)
        for events in (first, second, retried):
            self.assertIn(TextDelta("Answer"), events)
            self.assertIn(ResponseComplete(), events)
            metadata = next(event for event in events if isinstance(event, MessageMetadata))
            self.assertEqual(metadata.actions, (MessageAction.RETRY.value,))
            self.assertEqual(metadata.sources[0]["url"], "https://example.com")

    async def test_model_catalog_only_exposes_account_available_modes(self):
        provider = GrokProvider(Path("unused"), "test")
        transport = GrokTransportFixture()
        provider.transport = transport
        try:
            catalog = await provider.models()
        finally:
            await provider.close()

        self.assertEqual(
            {model.id for model in catalog.chat_models},
            {"fast"},
        )
        self.assertEqual(catalog.default_model, "fast")
        self.assertEqual(catalog.chat_model("fast").title, "Grok 3")
        self.assertEqual(catalog.chat_model("fast").chat_title, "Grok 3 · Fast")
        self.assertEqual(catalog.chat_model("fast").metadata["modelMode"], "MODEL_MODE_FAST")
        self.assertEqual(catalog.chat_model("fast").metadata["tags"], ["TAG_PRIMARY"])
        self.assertIsNone(catalog.chat_model("auto"))
        self.assertIsNone(catalog.chat_model("expert"))
        self.assertIsNone(catalog.chat_model("unknown"))
        self.assertIsNone(catalog.chat_model("build"))
        self.assertIsNone(catalog.chat_model("grok-paid"))
        before = len(transport.requests)
        self.assertIs(await provider.models(), catalog)
        self.assertEqual(len(transport.requests), before)

    async def test_catalog_keeps_paid_modes_dynamic_without_guessing_ambiguous_models(self):
        for details, description, expected in (
            (
                [{"modelId": "grok-next", "name": "Grok Next", "modelMode": "MODEL_MODE_EXPERT"}],
                "Thinks hard",
                ("Grok Next · Expert", "grok-next"),
            ),
            (
                [
                    {"modelId": identifier, "modelMode": "MODEL_MODE_EXPERT"}
                    for identifier in ("grok-a", "grok-b")
                ],
                "Thinks hard",
                ("Grok · Expert", None),
            ),
            ([], "Thinks hard · Grok 4.7", ("Grok 4.7 · Expert", None)),
        ):
            with self.subTest(details=details):
                provider = GrokProvider(Path("unused"), "test")
                provider.transport.request = AsyncMock(
                    side_effect=lambda _, path, **kw: (
                        {"models": details}
                        if path == "/rest/models"
                        else {
                            "modes": [
                                {
                                    "id": "expert",
                                    "title": "Expert",
                                    "description": description,
                                    "availability": {"available": {}},
                                }
                            ],
                            "defaultModeId": "expert",
                        }
                    )
                )
                try:
                    catalog = await provider.models()
                    model = catalog.chat_model("expert")
                    self.assertEqual((model.chat_title, model.metadata.get("modelName")), expected)
                    self.assertEqual(catalog.default_model, "expert")
                finally:
                    await provider.close()

    async def test_server_defined_models_route_without_a_hardcoded_model_list(self):
        provider = GrokProvider(Path("unused"), "test")
        transport = GrokTransportFixture()
        provider.transport = transport
        transport.request = AsyncMock(
            side_effect=lambda _, path, **kw: (
                {
                    "models": [
                        {
                            "modelId": "grok-future",
                            "name": "Grok Future",
                            "modelMode": "MODEL_MODE_SERVER_NEW",
                        }
                    ]
                }
                if path == "/rest/models"
                else {
                    "modes": [
                        {
                            "id": "grok-future",
                            "title": "Grok Future",
                            "availability": {"available": {}},
                        }
                    ],
                    "defaultModeId": "grok-future",
                }
            )
        )
        try:
            catalog = await provider.models()
            self.assertEqual(catalog.default_chat_model.chat_title, "Grok Future")
            [
                event
                async for event in provider.stream(
                    SendRequest(
                        provider_id="grok",
                        account_id="test",
                        chat_id="local",
                        prompt="Question",
                        model="grok-future",
                    )
                )
            ]
            self.assertEqual(transport.bodies[0]["modelName"], "grok-future")
            self.assertEqual(transport.bodies[0]["modelMode"], "MODEL_MODE_SERVER_NEW")
            self.assertNotIn("isReasoning", transport.bodies[0])
            self.assertEqual(transport.request.await_count, 2)
        finally:
            await provider.close()

    async def test_stream_cleans_provider_markup_and_initial_whitespace(self):
        provider = GrokProvider(Path("unused"), "test")
        transport = GrokTransportFixture()
        provider.transport = transport

        async def events(_path, _body):
            yield {
                "result": {
                    "conversation": {"conversationId": "chat"},
                    "response": {
                        "userResponse": {"responseId": "user-1"},
                        "token": " \tAnswer <grok:ren",
                    },
                }
            }
            yield {
                "result": {
                    "response": {
                        "token": 'der card_id="citation" type="render_inline_citation">1</grok:render>'
                    }
                }
            }
            yield {
                "result": {
                    "response": {
                        "token": "  Thinking <xai:tool_usage_card>private</xai:tool_usage_card> done",
                        "isThinking": True,
                    }
                }
            }
            yield {
                "result": {
                    "response": {
                        "modelResponse": {
                            "responseId": "answer-1",
                            "model": "grok-4-1-thinking-1129",
                            "message": ' \tAnswer <grok:render card_id="citation" type="render_inline_citation">1</grok:render>',
                            "thinkingTrace": "  Thinking <xai:tool_usage_card>private</xai:tool_usage_card> done",
                            "partial": False,
                        }
                    }
                }
            }

        transport.events = events
        request = SendRequest(
            provider_id="grok",
            account_id="test",
            chat_id="local",
            prompt="Question",
            model="fast",
        )
        try:
            stream = [event async for event in provider.stream(request)]
        finally:
            await provider.close()

        deltas = [event for event in stream if isinstance(event, TextDelta)]
        self.assertEqual(
            "".join(event.text for event in deltas if event.channel is None), "Answer 1"
        )
        self.assertEqual(
            "".join(event.text for event in deltas if event.channel == "analysis"),
            "Thinking  done",
        )
        self.assertFalse(any("<grok:render" in event.text for event in deltas))
        self.assertFalse(any("tool_usage_card" in event.text for event in deltas))
        metadata = next(event for event in stream if isinstance(event, MessageMetadata))
        self.assertEqual(metadata.model, "grok-4-1-thinking-1129")

    def test_text_filter_preserves_every_protocol_tag_split(self):
        text = (
            '  Answer <grok:render type="render_inline_citation">2</grok:render> '
            "<xai:tool_usage_card>ignored</xai:tool_usage_card> end"
        )
        for chunks in [list(text), *([text[:i], text[i:]] for i in range(len(text) + 1))]:
            cleaner = GrokTextFilter()
            value = "".join([cleaner.feed(chunk) for chunk in chunks] + [cleaner.feed(final=True)])
            self.assertEqual(value, "Answer 2  end")

    async def test_stream_reconciles_late_final_snapshots_and_completes_once(self):
        provider = GrokProvider(Path("unused"), "test")
        transport = GrokTransportFixture()
        provider.transport = transport

        async def events(_path, _body):
            yield {"result": {"conversationId": "chat", "response": {"token": "Draft"}}}
            for snapshot in ("Final", "Authoritative final"):
                yield {
                    "result": {
                        "response": {
                            "modelResponse": {
                                "responseId": "answer",
                                "model": "grok-3",
                                "message": snapshot,
                                "partial": False,
                            }
                        }
                    }
                }

        transport.events = events
        try:
            received = [
                event
                async for event in provider.stream(
                    SendRequest(
                        provider_id="grok",
                        account_id="test",
                        chat_id="local",
                        prompt="Question",
                    )
                )
            ]
        finally:
            await provider.close()
        text = ""
        for event in received:
            if isinstance(event, TextDelta):
                text += event.text
            elif isinstance(event, ReplaceText):
                text = event.text
        self.assertEqual(text, "Authoritative final")
        self.assertEqual(received.count(ResponseComplete()), 1)
        metadata = next(event for event in received if isinstance(event, MessageMetadata))
        self.assertEqual((metadata.model, metadata.message_id), ("grok-3", "answer"))

    async def test_failed_stream_does_not_cache_replay_or_hide_auth_errors(self):
        for code in (None, "AUTH_REQUIRED", "UNAUTHORIZED", "RATE_LIMITED"):
            with self.subTest(code=code):
                provider = GrokProvider(Path("unused"), "test")
                transport = GrokTransportFixture()
                provider.transport = transport

                async def events(_path, body):
                    transport.bodies.append(body)
                    yield {"result": {"conversationId": "chat", "response": {"token": "Partial"}}}
                    if code:
                        yield {"result": {"response": {}, "error": {"code": code}}}

                transport.events = events
                try:
                    error = (
                        AccountPaused
                        if code in ("AUTH_REQUIRED", "UNAUTHORIZED")
                        else ProviderError
                    )
                    with self.assertRaises(error):
                        [
                            event
                            async for event in provider.stream(
                                SendRequest(
                                    provider_id="grok",
                                    account_id="test",
                                    chat_id="local",
                                    prompt="Question",
                                )
                            )
                        ]
                    self.assertEqual(provider._continuations, {})
                    self.assertEqual(len(transport.bodies), 1)
                    self.assertEqual(transport.session.authenticated, error is not AccountPaused)
                finally:
                    await provider.close()

    async def test_history_exposes_only_the_remote_generation_state(self):
        provider = GrokProvider(Path("unused"), "test")
        provider.transport = GrokTransportFixture()
        provider._conversation_metadata = AsyncMock(return_value={"conversationId": "chat"})
        provider._history = AsyncMock(
            side_effect=[
                {"responses": [], "inflightResponses": [{"responseId": "private-id"}]},
                {"responses": [], "inflightResponses": []},
            ]
        )
        record = SimpleNamespace(
            origin=None,
            title="Chat",
            messages=(),
            project_id=None,
            metadata={},
        )

        try:
            with patch("providers.grok.adapter.conversation", return_value=record):
                active = await provider.get_chat("chat")
                idle = await provider.get_chat("chat")
        finally:
            await provider.close()
        self.assertEqual(active.metadata["generation"], {"active": True})
        self.assertNotIn("generation", idle.metadata)
        self.assertNotIn("private-id", repr(active.metadata))

    async def test_unknown_model_refreshes_catalog_and_fails_before_send(self):
        provider = GrokProvider(Path("unused"), "test")
        transport = GrokTransportFixture()
        provider.transport = transport
        request = SendRequest(
            provider_id="grok",
            account_id="test",
            chat_id="local",
            prompt="Question",
            conversation_id="chat",
            model="not-in-this-account",
        )
        try:
            with self.assertRaisesRegex(ProviderError, "unavailable"):
                [event async for event in provider.stream(request)]
        finally:
            await provider.close()

        self.assertEqual(
            sum(path == "/rest/modes" for _, path, _ in transport.requests),
            2,
        )
        self.assertEqual(transport.bodies, [])
        self.assertEqual(len(transport.requests), 4)

    def test_grok_exposes_bounded_document_attachment_policy(self):
        provider = GrokProvider(Path("unused"), "test")

        policy = provider.attachment_policy()

        self.assertIn(Capability.ATTACHMENTS, provider.capabilities)
        self.assertEqual(policy["max_bytes"], 3 * 1024 * 1024)
        self.assertEqual(policy["max_total_bytes"], 10 * 1024 * 1024)
        self.assertNotIn(".png", policy["allowed_extensions"])

    async def test_grok_chat_upload_returns_metadata_for_file_attachment(self):
        content = b"%PDF-1.7 test"
        metadata = {
            "assetId": "asset-1",
            "name": "brief.pdf",
            "mimeType": "application/pdf",
            "sizeBytes": len(content),
            "fileSource": "SELF_UPLOAD_FILE_SOURCE",
        }
        transport = GrokTransport(Path("unused"))
        transport.request = AsyncMock(return_value={"fileMetadata": metadata})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "brief.pdf"
            path.write_bytes(content)
            try:
                uploaded = await transport.upload_file(path)
            finally:
                await transport.close()

        self.assertEqual(
            uploaded,
            {
                "id": "asset-1",
                "name": "brief.pdf",
                "mime_type": "application/pdf",
                "size_bytes": len(content),
                "grok_file": metadata,
            },
        )
        call = transport.request.await_args
        self.assertEqual(call.args, ("POST", "/rest/app-chat/upload-file-v2/complete"))
        self.assertEqual(
            call.kwargs["json"],
            {
                "directUpload": {
                    "fileData": base64.b64encode(content).decode("ascii"),
                    "fileName": "brief.pdf",
                    "fileMimeType": "application/pdf",
                    "fileSource": "SELF_UPLOAD_FILE_SOURCE",
                }
            },
        )

    async def test_grok_send_forwards_uploaded_metadata_as_file_attachments(self):
        provider = GrokProvider(Path("unused"), "test")
        transport = GrokTransportFixture()
        provider.transport = transport
        metadata = {
            "assetId": "asset-1",
            "name": "brief.pdf",
            "mimeType": "application/pdf",
        }
        request = SendRequest(
            provider_id="grok",
            account_id="test",
            chat_id="local",
            prompt="Summarize this file",
            conversation_id="chat",
            model="fast",
            attachments=({"id": "asset-1", "size_bytes": 13, "grok_file": metadata},),
        )
        try:
            [event async for event in provider.stream(request)]
        finally:
            await provider.close()

        self.assertEqual(transport.bodies[0]["fileAttachments"], [metadata])
        self.assertEqual(transport.bodies[0]["imageAttachments"], [])

    async def test_project_updates_only_send_changed_editable_fields(self):
        for changes, expected in (
            ({"name": " Renamed "}, {"name": "Renamed"}),
            ({"instructions": ""}, {"customPersonality": ""}),
            (
                {"name": "Updated", "instructions": "New"},
                {"name": "Updated", "customPersonality": "New"},
            ),
        ):
            with self.subTest(changes=changes):
                transport = SimpleNamespace(request=AsyncMock(return_value={"updated": True}))
                result = await GrokProjectEditor(transport).write("update", "workspace", **changes)
                self.assertEqual(result, {"updated": True})
                transport.request.assert_awaited_once_with(
                    "PUT", "/rest/workspaces/workspace", json=expected
                )

    async def test_project_text_source_uses_grok_workspace_upload(self):
        transport = SimpleNamespace(request=AsyncMock(return_value={"assetId": "source-1"}))
        editor = GrokProjectEditor(transport)

        await editor.write(
            "add_source",
            "workspace",
            kind="text",
            name="Instructions",
            text="Use verified sources",
        )

        call = transport.request.await_args
        self.assertEqual(
            call.args,
            ("POST", "/rest/workspaces/workspace/files/upload"),
        )
        self.assertEqual(
            call.kwargs["json"],
            {
                "fileName": "Instructions",
                "mimeType": "text/plain",
                "content": base64.b64encode(b"Use verified sources").decode("ascii"),
            },
        )

    async def test_project_mutations_verify_remote_state_and_preserve_unedited_fields(self):
        provider = GrokProvider(Path("unused"), "test")
        workspace = {
            "workspaceId": "workspace",
            "name": "Project",
            "customPersonality": "Old",
            "preferredModel": "keep",
            "isPublic": False,
        }
        files = []
        path = "/rest/workspaces/workspace"

        def request(method, target, **options):
            if method == "PUT" and target == path:
                workspace.update(options["json"])
                return dict(workspace)
            if method == "GET" and target == path:
                return dict(workspace)
            if method == "GET" and target == path + "/files":
                return {"files": list(files)}
            if method == "POST" and target == path + "/files/upload":
                files.append({"path": "/notes.txt", "name": "notes.txt", "assetId": "asset"})
                return {"file": files[-1]}
            if method == "DELETE" and target == path + "/files":
                self.assertEqual(options["params"]["path"], "/notes.txt")
                files.clear()
                return None
            raise AssertionError((method, target))

        provider.transport.request = AsyncMock(side_effect=request)
        try:
            current = await provider.get_project("workspace")
            provider.transport.request.reset_mock()
            updated = await provider.update_project(replace(current, instructions="New"))
            self.assertEqual(updated.instructions, "New")
            self.assertEqual(provider.transport.request.await_count, 5)
            self.assertEqual(workspace["preferredModel"], "keep")
            self.assertFalse(workspace["isPublic"])

            added = await provider.add_project_resource(
                "workspace", "text", name="notes.txt", text="Notes"
            )
            self.assertEqual(added.resources[0].id, "/notes.txt")
            with self.assertRaisesRegex(ValueError, "zaten var"):
                await provider.add_project_resource(
                    "workspace", "text", name="notes.txt", text="Overwrite"
                )
            provider.transport.request.reset_mock()
            removed = await provider.remove_project_resource("workspace", "/notes.txt")
            self.assertEqual(removed.resources, ())
            self.assertEqual(provider.transport.request.await_count, 5)
        finally:
            await provider.close()

    async def test_project_file_source_uses_bounded_workspace_upload(self):
        transport = SimpleNamespace(request=AsyncMock(return_value={"assetId": "source-1"}))
        editor = GrokProjectEditor(transport)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "notes.md"
            path.write_bytes(b"# Project notes\n")

            with patch(
                "providers.grok.transport.mimetypes.guess_type",
                return_value=("text/plain", None),
            ):
                await editor.write("add_source", "workspace", kind="file", path=path)

        call = transport.request.await_args
        self.assertEqual(call.args, ("POST", "/rest/workspaces/workspace/files/upload"))
        self.assertEqual(call.kwargs["json"]["fileName"], "notes.md")
        self.assertEqual(call.kwargs["json"]["mimeType"], "text/markdown")
        self.assertEqual(base64.b64decode(call.kwargs["json"]["content"]), b"# Project notes\n")

    async def test_project_source_removal_handles_asset_and_path_ids(self):
        for resource_id, source, expected in (
            (
                "asset-1",
                {"assetId": "asset-1", "name": "notes.md"},
                (("DELETE", "/rest/workspaces/workspace/assets/asset-1"), {}),
            ),
            (
                "docs/notes.md",
                {"path": "docs/notes.md", "name": "notes.md"},
                (
                    ("DELETE", "/rest/workspaces/workspace/files"),
                    {"params": {"path": "docs/notes.md", "recursive": "false"}},
                ),
            ),
            (
                "docs/notes.md",
                {"path": "docs/notes.md", "assetId": "asset-1", "name": "notes.md"},
                (
                    ("DELETE", "/rest/workspaces/workspace/files"),
                    {"params": {"path": "docs/notes.md", "recursive": "false"}},
                ),
            ),
        ):
            transport = SimpleNamespace(request=AsyncMock(return_value={"deleted": True}))
            editor = GrokProjectEditor(transport)

            result = await editor.write(
                "remove_source",
                "workspace",
                resource_id=resource_id,
                resource=Resource(resource_id, source["name"], metadata=source),
            )

            self.assertEqual(result, {"deleted": True})
            self.assertEqual(transport.request.await_count, 1)
            delete = transport.request.await_args
            self.assertEqual(delete.args, expected[0])
            self.assertEqual(delete.kwargs, expected[1])

    async def test_project_create_delete_and_chat_move_use_workspace_contracts(self):
        provider = GrokProvider(Path("unused"), "test")
        provider.transport.request = AsyncMock(
            side_effect=[
                {"workspaceId": "new", "name": "Project", "customPersonality": "Instructions"},
                None,
                {"conversationId": "chat", "title": "Chat", "workspaceId": "old"},
                None,
                None,
                {"conversationId": "chat", "title": "Chat", "workspaceId": "new"},
            ]
        )

        try:
            project = await provider.create_project(" Project ", "Instructions")
            await provider.delete_project("new")
            await provider.move_chat("chat", "new")
        finally:
            await provider.close()

        self.assertEqual((project.name, project.instructions), ("Project", "Instructions"))
        calls = provider.transport.request.await_args_list
        self.assertEqual(calls[0].args, ("POST", "/rest/workspaces"))
        self.assertEqual(
            calls[0].kwargs["json"],
            {"name": "Project", "customPersonality": "Instructions"},
        )
        self.assertEqual(calls[1].args, ("DELETE", "/rest/workspaces/new"))
        self.assertEqual(
            [call.args[:2] for call in calls[2:]],
            [
                ("GET", "/rest/app-chat/conversations/chat"),
                ("POST", "/rest/workspaces/new/conversations"),
                ("DELETE", "/rest/workspaces/old/conversations/chat"),
                ("GET", "/rest/app-chat/conversations/chat"),
            ],
        )
        self.assertEqual(calls[3].kwargs["json"], {"conversationId": "chat"})

    async def test_chat_move_detaches_rolls_back_and_skips_unchanged_membership(self):
        old = {"conversationId": "chat", "workspaceId": "old"}
        for target, replies, expected in (
            ("old", [old], ["GET"]),
            (None, [old, None, {"conversationId": "chat"}], ["GET", "DELETE", "GET"]),
            (
                "new",
                [old, None, ProviderError("remove failed"), None],
                ["GET", "POST", "DELETE", "DELETE"],
            ),
        ):
            with self.subTest(target=target):
                provider = GrokProvider(Path("unused"), "test")
                provider.transport.request = AsyncMock(side_effect=replies)
                try:
                    if target == "new":
                        with self.assertRaisesRegex(ProviderError, "remove failed"):
                            await provider.move_chat("chat", target)
                        rollback = provider.transport.request.await_args
                        self.assertEqual(
                            rollback.args, ("DELETE", "/rest/workspaces/new/conversations/chat")
                        )
                    else:
                        await provider.move_chat("chat", target)
                    self.assertEqual(
                        [call.args[0] for call in provider.transport.request.await_args_list],
                        expected,
                    )
                finally:
                    await provider.close()

    async def test_project_rejects_invalid_file_listing_before_confirming_mutations(self):
        provider = GrokProvider(Path("unused"), "test")
        provider.transport.request = AsyncMock(
            side_effect=[
                {"workspaceId": "workspace", "name": "Project"},
                {"files": "invalid"},
            ]
        )
        try:
            with self.assertRaisesRegex(ProviderError, "file response"):
                await provider.remove_project_resource("workspace", "source")
            self.assertTrue(
                all(call.args[0] == "GET" for call in provider.transport.request.await_args_list)
            )
        finally:
            await provider.close()

    def test_grok_cookie_selection_keeps_secure_session_and_ignores_noise(self):
        cookies = [
            {
                "name": "sso-rw",
                "value": "session",
                "domain": ".grok.com",
                "path": "/",
                "expires": -1,
                "secure": True,
            },
            {"name": "grok_device_id", "value": "device", "domain": ".grok.com", "secure": True},
            {"name": "__cf_bm", "value": "noise", "domain": ".grok.com", "secure": True},
            {"name": "sso-rw", "value": "foreign", "domain": ".example.com", "secure": True},
            {"name": "session-token", "value": "insecure", "domain": ".grok.com", "secure": False},
        ]
        self.assertEqual(select_cookies(cookies, "grok"), {"sso-rw": "session"})

    async def test_grok_stream_ignores_sse_comments_and_merges_multiline_data(self):
        class Response:
            def __init__(self, chunks):
                self.chunks = iter(chunks)

            async def aiter_content(self):
                for chunk in self.chunks:
                    yield chunk

        chunks = [
            b": ping\r\n\r\n",
            b"event: message\r\n",
            b'data: {"result":\r\n',
            b'data: {"token": "hi"}}\r\n\r\n',
            b": keepalive\r\n\r\n",
            b'{"result": {"token": "bye"}}\r\n',
        ]

        class StreamContext:
            async def __aenter__(self):
                return Response(chunks)

            async def __aexit__(self, *args):
                return False

        transport = GrokTransport(Path("unused"))
        transport._ensure_authenticated = AsyncMock()
        client = SimpleNamespace(stream=lambda *args, **kwargs: StreamContext())
        transport.session = SimpleNamespace(
            connect=lambda: client,
            url=lambda path: "https://grok.com" + path,
            check=lambda response: None,
            persist=AsyncMock(),
        )
        events = [event async for event in transport.events("/responses", {})]
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0], {"result": {"token": "hi"}})
        self.assertEqual(events[1], {"result": {"token": "bye"}})

    def test_history_preserves_user_file_attachments(self):
        history = {
            "conversation": {"conversationId": "chat", "title": "Chat"},
            "responses": [
                {
                    "responseId": "u1",
                    "sender": "human",
                    "message": "Check this",
                    "fileAttachments": [
                        {
                            "assetId": "file-1",
                            "fileName": "report.pdf",
                            "fileMimeType": "application/pdf",
                        }
                    ],
                },
                {
                    "responseId": "a1",
                    "sender": "assistant",
                    "parentResponseId": "u1",
                    "message": "Reviewed",
                },
            ],
        }
        chat = conversation({"id": "chat"}, "account", history)
        self.assertEqual(len(chat.messages), 2)
        user_msg = chat.messages[0]
        self.assertEqual(user_msg.content[0].text, "Check this")
        self.assertEqual(user_msg.content[1].kind, "resource")
        self.assertEqual(user_msg.content[1].resource.id, "file-1")
        self.assertEqual(user_msg.content[1].resource.name, "report.pdf")
        self.assertEqual(user_msg.content[1].resource.media_type, "application/pdf")

    async def test_stream_allows_concurrent_models_lookup_without_gate_contention(self):
        provider = GrokProvider(Path("unused"), "test")
        transport = GrokTransportFixture()
        provider.transport = transport
        started_event = asyncio.Event()
        finish_event = asyncio.Event()

        async def slow_events(path, body):
            started_event.set()
            await finish_event.wait()
            yield {
                "result": {
                    "conversation": {"conversationId": "chat"},
                    "response": {
                        "userResponse": {"responseId": "user-1"},
                        "token": "Done",
                        "done": True,
                        "modelResponse": {
                            "responseId": "answer-1",
                            "model": "grok-3",
                            "message": "Done",
                            "partial": False,
                        },
                    },
                }
            }

        transport.events = slow_events
        request = SendRequest(
            provider_id="grok",
            account_id="test",
            chat_id="local",
            prompt="Hello",
            conversation_id="chat",
            model="fast",
        )

        async def run_stream():
            return [e async for e in provider.stream(request)]

        stream_task = asyncio.create_task(run_stream())
        try:
            await started_event.wait()
            catalog = await provider.models(force=True)
            self.assertIsNotNone(catalog)
            self.assertEqual(catalog.default_model, "fast")
        finally:
            finish_event.set()
            await stream_task
            await provider.close()

    async def test_move_chat_retries_eventual_consistency_before_reporting_success(self):
        provider = GrokProvider(Path("unused"), "test")
        calls = 0

        async def request(method, path, **kwargs):
            nonlocal calls
            if method == "GET" and path == "/rest/app-chat/conversations/chat":
                calls += 1
                return {"id": "chat", "workspaceId": "ws-1" if calls == 1 else "ws-2"}
            if method == "POST":
                return {}
            if method == "DELETE":
                return {}
            return {}

        provider.transport.request = AsyncMock(side_effect=request)
        try:
            await provider.move_chat("chat", "ws-2")
            self.assertEqual(calls, 2)
        finally:
            await provider.close()
