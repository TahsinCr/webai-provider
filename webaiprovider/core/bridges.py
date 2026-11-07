from __future__ import annotations

from collections.abc import Coroutine
from types import TracebackType
from typing import Any, TypeVar

from eventflow import LoopPortal

T = TypeVar("T")


class SyncPortalBridge:
    __slots__ = ("_portal",)

    def __init__(self, name: str = "ProviderSyncPortal") -> None:
        self._portal = LoopPortal(name=name)

    def start(self) -> None:
        if not self._portal.running:
            self._portal.start()

    def close(self) -> None:
        if self._portal.running:
            self._portal.close()

    def call(self, coro: Coroutine[Any, Any, T], *, timeout: float | None = None) -> T:
        if not self._portal.running:
            self._portal.start()
        return self._portal.call(coro, timeout=timeout)

    def cancel(self) -> None:
        if self._portal.running:
            self._portal.cancel()

    def __enter__(self) -> SyncPortalBridge:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
