from __future__ import annotations

import asyncio
import unittest

from webaiprovider.core.cache import BoundedCache
from webaiprovider.core.concurrency import gather_bounded


class CoreConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_bounded_gather_reuses_workers_and_preserves_input_order(self) -> None:
        tasks = set()

        async def read(index: int) -> int:
            tasks.add(asyncio.current_task())
            if index % 2 == 0:
                await asyncio.sleep(0)
            return index

        self.assertEqual(await gather_bounded(range(32), read, limit=4), list(range(32)))
        self.assertEqual(len(tasks), 4)


class CoreCacheTests(unittest.TestCase):
    def test_cache_evicts_when_capacity_exceeded(self) -> None:
        cache: BoundedCache[str, str] = BoundedCache(
            max_entries=2,
            max_bytes=1000,
        )

        cache.put("k1", "v1")
        cache.put("k2", "value-2")
        cache.put("k3", "v3")

        self.assertIsNone(cache.get("k1"))
        self.assertEqual(cache.get("k2"), "value-2")
        self.assertEqual(cache.get("k3"), "v3")

    def test_cache_evicts_when_byte_limit_exceeded(self) -> None:
        cache: BoundedCache[str, str] = BoundedCache(
            max_entries=10,
            max_bytes=60,
            getsizeof=lambda _key, value: len(value),
        )

        cache.put("k1", "a" * 25)
        cache.put("k2", "b" * 25)
        cache.put("k3", "c" * 25)

        self.assertIsNone(cache.get("k1"))
        self.assertEqual(cache.get("k3"), "c" * 25)

    def test_cache_removal_and_clear(self) -> None:
        cache: BoundedCache[str, str] = BoundedCache(
            max_entries=5,
            max_bytes=1000,
        )
        cache.put("k1", "val1")
        cache.put("k2", "val2")
        self.assertGreater(cache.stats().bytes, 0)
        self.assertEqual(cache.remove("k1"), "val1")
        self.assertIsNone(cache.remove("k1"))
        self.assertEqual(cache.remove("k2"), "val2")

        cache.clear()
        self.assertEqual(cache.stats().entries, 0)


if __name__ == "__main__":
    unittest.main()
