from __future__ import annotations

import threading
from collections.abc import Awaitable
from types import TracebackType
from typing import TypeVar

from eventflow import LoopPortal

T = TypeVar("T")

_SHARED_LOCK = threading.Lock()
_SHARED_PORTALS: dict[str, tuple[LoopPortal, int]] = {}


class PortalBridge:
    """Reference-counted handle to a shared LoopPortal thread keyed by portal name."""

    __slots__ = ("_name", "_acquired", "_closed")

    def __init__(self, name: str = "ProviderSyncPortal") -> None:
        self._name = name
        self._acquired = False
        self._closed = False

    def _acquire(self) -> LoopPortal:
        with _SHARED_LOCK:
            if self._closed:
                raise RuntimeError("The portal bridge is closed")
            entry = _SHARED_PORTALS.get(self._name)
            portal = entry[0] if entry is not None else LoopPortal(name=self._name)
            refs = entry[1] if entry is not None else 0
            if not portal.running:
                portal.start()
            if not self._acquired:
                refs += 1
                self._acquired = True
            _SHARED_PORTALS[self._name] = (portal, refs)
            return portal

    def start(self) -> None:
        self._acquire()

    def close(self) -> None:
        portal_to_close: LoopPortal | None = None
        with _SHARED_LOCK:
            if self._closed:
                return
            self._closed = True
            if self._acquired:
                self._acquired = False
                entry = _SHARED_PORTALS.get(self._name)
                if entry is not None:
                    portal, refs = entry
                    refs = max(0, refs - 1)
                    if refs == 0:
                        _SHARED_PORTALS.pop(self._name, None)
                        portal_to_close = portal
                    else:
                        _SHARED_PORTALS[self._name] = (portal, refs)
        if portal_to_close is not None and portal_to_close.running:
            portal_to_close.close()

    def call(self, coro: Awaitable[T], *, timeout: float | None = None) -> T:
        return self._acquire().call(coro, timeout=timeout)

    def cancel(self) -> None:
        with _SHARED_LOCK:
            entry = _SHARED_PORTALS.get(self._name)
            portal = entry[0] if entry is not None else None
        if portal is not None and portal.running:
            portal.cancel()

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
