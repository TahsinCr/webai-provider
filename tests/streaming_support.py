from __future__ import annotations

from collections.abc import AsyncIterator, Iterable

from webaiprovider import Account, AsyncProvider
from webaiprovider.resources.message.protocol import ProviderRequest, ProviderStreamEvent


class StreamTestProvider(AsyncProvider):
    def __init__(self, provider_id: str, account_id: str, events: Iterable[object]):
        self._events = tuple(events)
        self.requests: list[ProviderRequest] = []
        super().__init__(Account(account_id, provider_id, {}))

    async def _stream_provider_events(
        self, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamEvent]:
        self.requests.append(request)
        for event in self._events:
            yield event  # type: ignore[misc]
