from __future__ import annotations

import threading
from collections.abc import Coroutine
from types import TracebackType
from typing import Any, TypeVar

from eventflow import LoopPortal

T = TypeVar("T")

_SHARED_LOCK = threading.Lock()
_SHARED_PORTAL: LoopPortal | None = None
_SHARED_REFS = 0


class PortalBridge:
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

    def __enter__(self) -> PortalBridge:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


class SharedPortalBridge:
    """Reference-counted handle to a single process-wide LoopPortal thread."""

    __slots__ = ("_acquired", "_closed")

    def __init__(self) -> None:
        self._acquired = False
        self._closed = False

    def _acquire(self) -> LoopPortal:
        global _SHARED_PORTAL, _SHARED_REFS
        with _SHARED_LOCK:
            if self._closed:
                raise RuntimeError("The shared portal handle is closed")
            if _SHARED_PORTAL is None:
                _SHARED_PORTAL = LoopPortal(name="ProviderSyncPortal")
            if not _SHARED_PORTAL.running:
                _SHARED_PORTAL.start()
            if not self._acquired:
                _SHARED_REFS += 1
                self._acquired = True
            return _SHARED_PORTAL

    def start(self) -> None:
        self._acquire()

    def call(self, coro: Coroutine[Any, Any, T], *, timeout: float | None = None) -> T:
        portal = self._acquire()
        return portal.call(coro, timeout=timeout)

    def close(self) -> None:
        global _SHARED_PORTAL, _SHARED_REFS
        portal_to_close: LoopPortal | None = None
        with _SHARED_LOCK:
            if self._closed:
                return
            self._closed = True
            if self._acquired:
                self._acquired = False
                _SHARED_REFS = max(0, _SHARED_REFS - 1)
                if _SHARED_REFS == 0 and _SHARED_PORTAL is not None:
                    portal_to_close = _SHARED_PORTAL
                    _SHARED_PORTAL = None
        if portal_to_close is not None and portal_to_close.running:
            portal_to_close.close()

    def __enter__(self) -> SharedPortalBridge:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
