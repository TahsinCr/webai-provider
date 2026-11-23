from __future__ import annotations

import sys
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


@dataclass(frozen=True, slots=True)
class CacheStats:
    """Snapshot of bounded cache occupancy."""

    entries: int
    bytes: int


@dataclass(slots=True)
class _CacheEntry(Generic[V]):
    value: V
    size: int
    expires_at: float | None


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


class BoundedCache(Generic[K, V]):
    """Thread-safe LRU cache bounded by entry count, estimated byte size, and TTL."""

    __slots__ = (
        "_max_entries",
        "_max_bytes",
        "_ttl",
        "_getsizeof",
        "_clock",
        "_lock",
        "_entries",
        "_bytes",
    )

    def __init__(
        self,
        *,
        max_entries: int = 128,
        max_bytes: int = 1_000_000,
        ttl: float | None = 300.0,
        getsizeof: Callable[[K, V], int] = _default_size_calculator,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be >= 1")
        if max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")
        if ttl is not None and ttl <= 0:
            raise ValueError("ttl must be > 0 or None")
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._ttl = ttl
        self._getsizeof = getsizeof
        self._clock = clock
        self._lock = threading.RLock()
        self._entries: OrderedDict[K, _CacheEntry[V]] = OrderedDict()
        self._bytes = 0

    def _evict_expired_locked(self, now: float) -> None:
        expired_keys = [
            key
            for key, entry in self._entries.items()
            if entry.expires_at is not None and entry.expires_at <= now
        ]
        for key in expired_keys:
            removed = self._entries.pop(key, None)
            if removed is not None:
                self._bytes -= removed.size

    def get(self, key: K) -> V | None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_at is not None and entry.expires_at <= self._clock():
                self._entries.pop(key, None)
                self._bytes -= entry.size
                return None
            self._entries.move_to_end(key)
            return entry.value

    def put(self, key: K, value: V, *, ttl: float | None = None) -> bool:
        effective_ttl = self._ttl if ttl is None else ttl
        if effective_ttl is not None and effective_ttl <= 0:
            self.remove(key)
            return False
        size = max(1, int(self._getsizeof(key, value)))
        if size > self._max_bytes:
            self.remove(key)
            return False
        now = self._clock()
        expires_at = None if effective_ttl is None else now + effective_ttl
        with self._lock:
            existing = self._entries.pop(key, None)
            if existing is not None:
                self._bytes -= existing.size
            if len(self._entries) >= self._max_entries or self._bytes + size > self._max_bytes:
                self._evict_expired_locked(now)
            while self._entries and (
                len(self._entries) >= self._max_entries or self._bytes + size > self._max_bytes
            ):
                _, oldest = self._entries.popitem(last=False)
                self._bytes -= oldest.size
            self._entries[key] = _CacheEntry(value=value, size=size, expires_at=expires_at)
            self._bytes += size
            return True

    def remove(self, key: K) -> V | None:
        with self._lock:
            entry = self._entries.pop(key, None)
            if entry is None:
                return None
            self._bytes -= entry.size
            return entry.value

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._bytes = 0

    def stats(self) -> CacheStats:
        with self._lock:
            return CacheStats(entries=len(self._entries), bytes=self._bytes)


class ContinuationCache(BoundedCache[str, V]):
    """Bounded cache for provider continuation metadata, scoped to one session."""

    __slots__ = ()

    def __init__(self, *, ttl: float) -> None:
        super().__init__(max_entries=128, max_bytes=128 * 1024, ttl=ttl, clock=time.monotonic)
