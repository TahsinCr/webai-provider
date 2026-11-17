from __future__ import annotations

from collections.abc import Iterable

from webaiprovider import (
    Account,
    AsyncProvider,
    BaseMessageAdapter,
    BaseProviderAdapter,
    BaseSessionAdapter,
)
from webaiprovider.resources.message.protocol import ProviderRequest


class _StreamTestMessageAdapter(BaseMessageAdapter):
    def __init__(self, owner: StreamTestProvider) -> None:
        self._owner = owner

    async def stream(self, request: ProviderRequest):
        self._owner.requests.append(request)
        for event in self._owner._events:
            yield event


class StreamTestProvider(AsyncProvider):
    def __init__(self, provider_id: str, account_id: str, events: Iterable[object]):
        self._events = tuple(events)
        self.requests: list[ProviderRequest] = []
        super().__init__(
            Account(account_id, provider_id, {}),
            adapter=BaseProviderAdapter(
                provider_id=provider_id,
                account_id=account_id,
                session=BaseSessionAdapter(),
                messages=_StreamTestMessageAdapter(self),
            ),
        )
