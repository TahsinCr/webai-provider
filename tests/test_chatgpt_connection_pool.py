import asyncio
import threading
import unittest
from typing import ClassVar
from unittest.mock import patch

from tests.provider_fakes import provider_account
from webaiprovider.providers.chatgpt.provider import AsyncChatGPTProvider
from webaiprovider.resources.message.protocol import (
    ConversationBound,
    ProviderRequest,
    TextDelta,
)


class PersistentClient:
    instances: ClassVar[list["PersistentClient"]] = []

    def __init__(self, session):
        self.session = session
        self.closed = threading.Event()
        self.temporary_conversations = None
        type(self).instances.append(self)

    def send_prompt(self, conversation_id, prompt, on_event, **_options):
        on_event({"type": "delta", "text": "OK"})
        if prompt == "cancel me":
            self.closed.wait(2)
        return "temporary-chat", {"messages": []}, set()

    def close(self):
        self.closed.set()


class ChatGPTConnectionPoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_stream_reuses_prepared_client_and_discards_it_after_cancel(self):
        PersistentClient.instances.clear()
        with patch("webaiprovider.providers.chatgpt.provider.ChatGPTClient", PersistentClient):
            provider = AsyncChatGPTProvider(
                provider_account("chatgpt"), session=object(), client_factory=PersistentClient
            )

        with provider._client_scope() as prepared_client:
            pass
        self.assertEqual(len(PersistentClient.instances), 1)

        request = ProviderRequest("hello", temporary=True)
        events = [event async for event in provider._stream_request(request)]
        self.assertTrue(any(isinstance(event, ConversationBound) for event in events))
        self.assertIs(provider._http_client, prepared_client)
        self.assertFalse(prepared_client.closed.is_set())

        cancelled = provider._stream_request(ProviderRequest("cancel me", temporary=True))
        first = await asyncio.wait_for(anext(cancelled), timeout=1)
        self.assertIsInstance(first, TextDelta)
        await asyncio.wait_for(cancelled.aclose(), timeout=2)
        self.assertTrue(prepared_client.closed.is_set())
        self.assertIsNone(provider._http_client)
        self.assertEqual(len(PersistentClient.instances), 1)
        await provider.close()
