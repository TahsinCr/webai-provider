from __future__ import annotations

from collections.abc import Iterable

from webaiprovider.providers.base import ProviderAdapter, _ProviderRequest


class StreamTestAdapter(ProviderAdapter):
    def __init__(self, provider_id: str, account_id: str, events: Iterable[object]):
        self.id = provider_id
        self.account_id = account_id
        self.capabilities = frozenset()
        self._events = tuple(events)
        self.requests: list[_ProviderRequest] = []

    async def prepare(self) -> None:
        pass

    async def _stream_request(self, request: _ProviderRequest):
        self.requests.append(request)
        for event in self._events:
            yield event
