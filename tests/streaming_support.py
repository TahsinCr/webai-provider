from __future__ import annotations

from collections.abc import Iterable

from webaiprovider import Account, AsyncProvider
from webaiprovider.async_provider import _ProviderRequest


class StreamTestProvider(AsyncProvider):
    def __init__(self, provider_id: str, account_id: str, events: Iterable[object]):
        super().__init__(Account(account_id, provider_id, {}))
        self._events = tuple(events)
        self.requests: list[_ProviderRequest] = []

    async def _stream_request(self, request: _ProviderRequest):
        self.requests.append(request)
        for event in self._events:
            yield event
