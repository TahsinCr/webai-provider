from __future__ import annotations

import asyncio
import unittest

from webaiprovider.core.bridges import PortalBridge
from webaiprovider.core.cache import MultiDimensionBoundedCache
from webaiprovider.core.concurrency import ConcurrencyCoordinator, gather_bounded
from webaiprovider.core.scope import ProviderResourceScope
from webaiprovider.core.telemetry import (
    ProviderTelemetry,
    correlation_context,
    current_correlation_id,
)


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

    async def test_keyed_limiter_allows_concurrency_within_limit(self) -> None:
        coordinator = ConcurrencyCoordinator(max_concurrency_per_key=2)
        key = ("chatgpt", "acc_1")

        async with coordinator.acquire(key), coordinator.acquire(key):
            stats = coordinator.stats()
            self.assertIn("concurrency", stats)

        await coordinator.aclose()

    async def test_single_flight_deduplicates_concurrent_executions(self) -> None:
        coordinator = ConcurrencyCoordinator()
        executions = 0

        async def worker() -> str:
            nonlocal executions
            executions += 1
            await asyncio.sleep(0.01)
            return "computed_result"

        results = await asyncio.gather(
            coordinator.deduplicate("models_catalog", worker),
            coordinator.deduplicate("models_catalog", worker),
            coordinator.deduplicate("models_catalog", worker),
        )

        self.assertEqual(executions, 1)
        self.assertEqual(results, ["computed_result", "computed_result", "computed_result"])
        await coordinator.aclose()


class CoreCacheTests(unittest.TestCase):
    def test_cache_stores_and_retrieves_with_versioning(self) -> None:
        cache: MultiDimensionBoundedCache[str, str] = MultiDimensionBoundedCache(
            max_entries=3,
            max_bytes=1000,
            default_ttl=60.0,
        )

        cache.put("turn_1", "response text 1")
        self.assertEqual(cache.get("turn_1"), "response text 1")

        cache.put("turn_1", "response text 1 updated", version=2)
        self.assertEqual(cache.get("turn_1", version=2), "response text 1 updated")
        self.assertIsNone(cache.get("turn_1", version=3))

    def test_cache_evicts_when_capacity_exceeded(self) -> None:
        cache: MultiDimensionBoundedCache[str, str] = MultiDimensionBoundedCache(
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
        cache: MultiDimensionBoundedCache[str, str] = MultiDimensionBoundedCache(
            max_entries=10,
            max_bytes=60,
        )

        cache.put("k1", "a" * 25)
        cache.put("k2", "b" * 25)
        cache.put("k3", "c" * 25)

        self.assertIsNone(cache.get("k1"))

    def test_cache_removal_and_clear(self) -> None:
        cache: MultiDimensionBoundedCache[str, str] = MultiDimensionBoundedCache(
            max_entries=5,
            max_bytes=1000,
        )
        cache.put("k1", "val1")
        cache.put("k2", "val2")
        self.assertGreater(cache.current_bytes, 0)
        self.assertTrue(cache.remove("k1"))
        self.assertFalse(cache.remove("k1"))
        self.assertEqual(cache.pop("k2"), "val2")
        self.assertIsNone(cache.pop("missing"))

        cache.clear()
        self.assertEqual(cache.stats().entries, 0)


class CoreScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_resource_scope_executes_lifo_cleanups(self) -> None:
        order: list[str] = []

        async with ProviderResourceScope() as scope:
            scope.defer(lambda: order.append("socket_closed"))
            scope.defer(lambda: order.append("temp_file_deleted"))

        self.assertEqual(order, ["temp_file_deleted", "socket_closed"])


class CorePortalBridgeTests(unittest.TestCase):
    def test_portal_bridge_runs_coroutine_synchronously(self) -> None:
        with PortalBridge(name="TestPortal") as bridge:

            async def add(a: int, b: int) -> int:
                await asyncio.sleep(0.001)
                return a + b

            result = bridge.call(add(15, 27))
            self.assertEqual(result, 42)


class CoreTelemetryTests(unittest.TestCase):
    def test_provider_telemetry_metrics_and_ttft(self) -> None:
        telemetry = ProviderTelemetry()

        telemetry.record_call()
        telemetry.record_ttft(0.12)
        telemetry.record_ttft(0.08)
        telemetry.record_ttft(0.20)
        telemetry.record_success(0.5)

        snap = telemetry.snapshot()
        self.assertEqual(snap["calls"], 1)
        self.assertEqual(snap["successful"], 1)
        self.assertEqual(snap["failed"], 0)
        self.assertAlmostEqual(snap["median_ttft"], 0.12)

    def test_correlation_context_isolation(self) -> None:
        self.assertIsNone(current_correlation_id())
        with correlation_context("test-corr-abc"):
            self.assertEqual(current_correlation_id(), "test-corr-abc")
        self.assertIsNone(current_correlation_id())


if __name__ == "__main__":
    unittest.main()
