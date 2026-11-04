"""Grok continuation and browser-cookie contracts."""

import unittest
from pathlib import Path
from types import SimpleNamespace

from providers.api import (
    MessageAction,
    MessageMetadata,
    ProviderError,
    ResponseComplete,
    SendRequest,
    TextDelta,
)
from providers.grok import GrokProvider
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
        if body.get("requestType") != "regenerate":
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
    async def test_continuation_cache_skips_history_and_retry_keeps_user_parent(self):
        provider = GrokProvider(Path("unused"), "test")
        transport = GrokTransportFixture()
        provider.transport = transport

        async def send(prompt="Question", *, action=None, message_id=None):
            request = SendRequest(
                provider_id="grok",
                account_id="test",
                chat_id="local",
                prompt=prompt,
                conversation_id="chat",
                model="grok-test",
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
        self.assertEqual(transport.bodies[2]["message"], "")
        self.assertEqual(transport.bodies[2]["requestType"], "regenerate")
        self.assertEqual(
            [path.rsplit("/", 1)[-1] for _, path, _ in transport.requests],
            ["response-node", "responses", "response-node"],
        )
        self.assertEqual(len(transport.bodies), 3)
        for events in (first, second, retried):
            self.assertIn(TextDelta("Answer"), events)
            self.assertIn(ResponseComplete(), events)
            metadata = next(event for event in events if isinstance(event, MessageMetadata))
            self.assertEqual(metadata.actions, (MessageAction.RETRY.value,))
            self.assertEqual(metadata.sources[0]["url"], "https://example.com")

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
