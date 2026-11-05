from __future__ import annotations

import sys
from collections.abc import Callable
from typing import Any, Generic, TypeVar

from eventflow import VersionedCache

K = TypeVar("K")
V = TypeVar("V")


def _default_size_calculator(key: Any, value: Any) -> int:
    size = sys.getsizeof(key, 32)
    if isinstance(value, str):
        return size + sys.getsizeof(value)
    if isinstance(value, bytes):
        return size + len(value)
    if isinstance(value, tuple) and len(value) == 3 and isinstance(value[2], int):
        return size + value[2]
    return size + sys.getsizeof(value, 64)


class MultiDimensionBoundedCache(Generic[K, V]):
    __slots__ = ("_cache", "_sequence", "_default_ttl")

    def __init__(
        self,
        max_entries: int = 128,
        max_bytes: int = 1_000_000,
        default_ttl: float | None = 300.0,
        getsizeof: Callable[[K, V], int] | None = None,
    ) -> None:
        self._sequence: int = 0
        self._default_ttl = default_ttl
        calculator = getsizeof if getsizeof is not None else _default_size_calculator
        self._cache: VersionedCache[K, V] = VersionedCache(
            max_entries=max_entries,
            max_bytes=max_bytes,
            ttl=default_ttl,
            getsizeof=calculator,
        )

    def get(self, key: K, default: V | None = None, *, version: int | None = None) -> V | None:
        val = self._cache.get(key, version=version)
        return val if val is not None else default

    def put(
        self,
        key: K,
        value: V,
        *,
        version: int | None = None,
        ttl: float | None = None,
    ) -> bool:
        if version is None:
            self._sequence += 1
            effective_version = self._sequence
        else:
            effective_version = version
            if version > self._sequence:
                self._sequence = version

        effective_ttl = ttl if ttl is not None else self._default_ttl
        return self._cache.put(key, value, version=effective_version, ttl=effective_ttl)

    def remove(self, key: K) -> bool:
        return self._cache.remove(key)

    def pop(self, key: K, default: Any = None) -> V | None:
        val = self._cache.get(key)
        if val is None:
            return default
        self._cache.remove(key)
        return val

    def clear(self) -> None:
        self._cache.clear()

    def stats(self) -> Any:
        return self._cache.stats()

    @property
    def current_bytes(self) -> int:
        return self._cache.stats().bytes

    def __contains__(self, key: K) -> bool:
        return self._cache.get(key) is not None

    def __len__(self) -> int:
        return self._cache.stats().entries

    def __bool__(self) -> bool:
        return self._cache.stats().entries > 0

    def __getitem__(self, key: K) -> V:
        val = self._cache.get(key)
        if val is None:
            raise KeyError(key)
        return val

    def __setitem__(self, key: K, value: V) -> None:
        self.put(key, value)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, dict):
            return len(self) == len(other) and all(self.get(k) == v for k, v in other.items())
        return super().__eq__(other)
