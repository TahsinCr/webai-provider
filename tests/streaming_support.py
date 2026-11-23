from __future__ import annotations

from collections.abc import AsyncIterator, Iterable

from webaiprovider import AsyncWebAIProvider, WebAIAccount
from webaiprovider.resources.message.protocol import ProviderRequest, ProviderStreamEvent


class StreamTestProvider(AsyncWebAIProvider):
    def __init__(self, provider_id: str, account_id: str, events: Iterable[object]):
        self._events = tuple(events)
        self.requests: list[ProviderRequest] = []
        super().__init__(WebAIAccount(account_id, provider_id, {}))

    async def _stream_provider_events(
        self, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamEvent]:
        self.requests.append(request)
        for event in self._events:
            yield event  # type: ignore[misc]


async def collect_provider_stream(provider, operation):
    async with operation._send_scope:
        return [event async for event in provider._execute_stream(operation)]
