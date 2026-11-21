from __future__ import annotations

import sys
import time
from collections.abc import Callable
from typing import Any, TypeVar

from eventflow import VersionedCache

K = TypeVar("K")
V = TypeVar("V")


def _default_size_calculator(key: Any, value: Any) -> int:
    size = sys.getsizeof(key, 32)
    pending = [value]
    seen = set()
    while pending:
        item = pending.pop()
        identity = id(item)
        if identity in seen:
            continue
        seen.add(identity)
        size += sys.getsizeof(item, 64)
        if isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, (list, tuple, set, frozenset)):
            pending.extend(item)
    return size


class BoundedCache(VersionedCache[K, V]):
    """Bounded memory cache for callers without externally versioned records."""

    __slots__ = ()

    def __init__(
        self,
        *,
        max_entries: int = 128,
        max_bytes: int = 1_000_000,
        ttl: float | None = 300.0,
        getsizeof: Callable[[K, V], int] = _default_size_calculator,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        super().__init__(
            max_entries=max_entries,
            max_bytes=max_bytes,
            ttl=ttl,
            getsizeof=getsizeof,
            clock=clock,
        )

    def put(self, key: K, value: V, *, version: int = 0, ttl: float | None = None) -> bool:
        return super().put(key, value, version=version, ttl=ttl)


class ContinuationCache(BoundedCache[str, V]):
    """Bounded cache for provider continuation metadata, scoped to one session."""

    __slots__ = ()

    def __init__(self, *, ttl: float) -> None:
        super().__init__(max_entries=128, max_bytes=128 * 1024, ttl=ttl, clock=time.monotonic)
