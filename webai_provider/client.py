"""Host-independent account routing and lifecycle for the public Python API."""

import asyncio
from collections.abc import AsyncIterator

from .api import Capability, ProviderEvent, SendRequest
from .base import ProviderAdapter
from .errors import FeatureUnavailable
from .registry import create_provider


class ProviderClient:
    """An async resource scope for independent account adapters.

    account() exposes the full ProviderAdapter contract: authentication, models, history,
    projects, sources, uploads and downloads. stream() routes a SendRequest using
    both provider_id and account_id; chat/project remote IDs remain provider-owned.
    Closing the client cancels its active streams and closes every owned adapter.
    If an adapter close fails, the client stays terminal and a later close() retries
    only that adapter. Use one instance per asyncio event loop; application
    persistence is external.
    """

    def __init__(self):
        self._accounts: dict[tuple[str, str], ProviderAdapter] = {}
        self._streams: dict[asyncio.Task, int] = {}
        self._iterators = set()
        self._closed = False
        self._closing = None

    def add(self, adapter: ProviderAdapter) -> ProviderAdapter:
        if self._closed:
            raise RuntimeError("provider client is closed")
        key = (adapter.id, adapter.account_id)
        if not all(key) or key in self._accounts:
            raise ValueError("provider/account identity must be nonempty and unique")
        self._accounts[key] = adapter
        return adapter

    def connect(
        self, provider_id: str, account_id: str = "default", **configuration
    ) -> ProviderAdapter:
        """Configure an account lazily; no browser/session request is made here."""
        if self._closed:
            raise RuntimeError("provider client is closed")
        if (provider_id, account_id) in self._accounts:
            raise ValueError("provider/account identity must be unique")
        return self.add(create_provider(provider_id, account_id, **configuration))

    def account(self, provider_id: str, account_id: str = "default") -> ProviderAdapter:
        if self._closed:
            raise RuntimeError("provider client is closed")
        return self._accounts[provider_id, account_id]

    async def prepare(
        self, provider_id: str, account_id: str = "default", *, for_send: bool = False
    ) -> ProviderAdapter:
        """Warm one account before use; opt in to provider-specific send preparation."""
        adapter = self.account(provider_id, account_id)
        await adapter.prepare()
        if for_send:
            prepare_for_send = getattr(adapter, "prepare_for_send", None)
            if callable(prepare_for_send):
                await prepare_for_send()
        return adapter

    @property
    def accounts(self) -> tuple[tuple[str, str], ...]:
        return tuple(self._accounts)

    async def stream(self, request: SendRequest) -> AsyncIterator[ProviderEvent]:
        adapter = self.account(request.provider_id, request.account_id)
        if Capability.CHAT not in adapter.capabilities:
            raise FeatureUnavailable(Capability.CHAT)
        if request.action and request.action.capability not in adapter.capabilities:
            raise FeatureUnavailable(request.action.capability)
        if request.branch and Capability.CHAT_BRANCH not in adapter.capabilities:
            raise FeatureUnavailable(Capability.CHAT_BRANCH)
        task = asyncio.current_task()
        self._streams[task] = self._streams.get(task, 0) + 1
        iterator = adapter.stream(request)
        self._iterators.add(iterator)
        try:
            async for event in iterator:
                yield event
        finally:
            try:
                await iterator.aclose()
            finally:
                self._iterators.discard(iterator)
                count = self._streams.get(task, 1) - 1
                if count:
                    self._streams[task] = count
                else:
                    self._streams.pop(task, None)

    async def close(self) -> None:
        self._closed = True
        retry_failed_close = (
            self._closing is not None
            and self._closing.done()
            and (self._closing.cancelled() or self._closing.exception() is not None)
        )
        if self._closing is None or retry_failed_close:
            caller = asyncio.current_task()

            async def finish():
                tasks = [task for task in self._streams if task is not caller]
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.gather(
                    *(iterator.aclose() for iterator in tuple(self._iterators)),
                    return_exceptions=True,
                )
                self._iterators.clear()
                self._streams.clear()
                adapters = tuple(self._accounts.items())
                results = await asyncio.gather(
                    *(adapter.close() for _, adapter in adapters), return_exceptions=True
                )
                failures = [
                    (key, adapter, result)
                    for (key, adapter), result in zip(adapters, results, strict=True)
                    if isinstance(result, BaseException)
                ]
                self._accounts = {key: adapter for key, adapter, _ in failures}
                if failures:
                    raise failures[0][2]

            self._closing = asyncio.create_task(finish())
        await asyncio.shield(self._closing)

    async def __aenter__(self):
        if self._closed:
            raise RuntimeError("provider client is closed")
        return self

    async def __aexit__(self, *_):
        await self.close()
