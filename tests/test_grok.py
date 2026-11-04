"""Grok continuation and browser-cookie contracts."""

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from providers.api import (
    MessageAction,
    MessageMetadata,
    ProviderError,
    ResponseComplete,
    SendRequest,
    TextDelta,
)
from providers.errors import AccountPaused
from providers.grok import GrokProvider
from providers.grok.transport import GrokTransport
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
        self.turn = 0

    async def request(self, method, path, **options):
        self.requests.append((method, path, options))
        if path == "/rest/models":
            return {
                "models": [
                    {
                        "modelId": "grok-test",
                        "name": "Model",
                        "modeName": "Grok Test",
                        "modelMode": "MODEL_MODE_AUTO",
                        "tags": [],
                    },
                    {
                        "modelId": "grok-420",
                        "name": "Model",
                        "modeName": "Grok 420",
                        "modelMode": "MODEL_MODE_GROK_420",
                    },
                    {"modelId": "grok-default", "name": "Model"},
                    {
                        "modelId": "imagine-agent-mode",
                        "name": "Imagine Agent",
                        "modelMode": "MODEL_MODE_IMAGINE",
                    },
                    {
                        "modelId": "grok-plugins-4p5-excel",
                        "name": "Model",
                        "modelMode": "MODEL_MODE_AUTO",
                    },
                    {
                        "modelId": "grok-companion",
                        "name": "Model",
                        "modelMode": "MODEL_MODE_AUTO",
                    },
                    {"modelId": "build", "name": "Model", "modelMode": "MODEL_MODE_BUILD"},
                ],
                "unavailableModels": [
                    {"modelId": "grok-unavailable", "name": "Model", "modelMode": "MODEL_MODE_AUTO"}
                ],
                "defaultProModel": "grok-default",
                "defaultProMode": "MODEL_MODE_FAST",
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

        async def send(prompt="Question", *, action=None, message_id=None, model="grok-test"):
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
            self.assertEqual(body["modelMode"], "MODEL_MODE_AUTO")
            self.assertNotIn("modelName", body)
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

    async def test_model_catalog_only_exposes_account_chat_models_with_clear_names(self):
        provider = GrokProvider(Path("unused"), "test")
        transport = GrokTransportFixture()
        provider.transport = transport
        try:
            catalog = await provider.models()
        finally:
            await provider.close()

        self.assertEqual(
            {model.id for model in catalog.chat_models},
            {"grok-test", "grok-420", "grok-default"},
        )
        self.assertEqual(catalog.default_model, "grok-default")
        self.assertEqual(catalog.chat_model("grok-test").title, "Grok Test")
        default = catalog.chat_model("grok-default")
        self.assertEqual(default.title, "Grok Default")
        self.assertEqual(default.metadata["defaultMode"], "MODEL_MODE_FAST")
        self.assertEqual(
            catalog.chat_model("grok-420").metadata["modelMode"],
            "MODEL_MODE_GROK_420",
        )

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
            sum(path == "/rest/models" for _, path, _ in transport.requests),
            2,
        )
        self.assertEqual(transport.bodies, [])
        self.assertEqual(len(transport.requests), 2)

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
