from __future__ import annotations

import asyncio
import unittest

from webai_provider.core.bridges import SyncPortalBridge
from webai_provider.core.cache import MultiDimensionBoundedCache
from webai_provider.core.concurrency import ConcurrencyCoordinator
from webai_provider.core.resilience import ProviderResilienceRegistry
from webai_provider.core.scope import ProviderResourceScope
from webai_provider.core.state import OperationState, OperationStateMachine
from webai_provider.core.telemetry import (
    ProviderTelemetry,
    correlation_context,
    current_correlation_id,
)
from webai_provider.errors import RateLimited


class CoreConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_keyed_limiter_allows_concurrency_within_limit(self) -> None:
        coordinator = ConcurrencyCoordinator(max_concurrency_per_key=2)
        key = ("chatgpt", "acc_1")

        async with coordinator.acquire(key):
            async with coordinator.acquire(key):
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

    async def test_run_keyed_executes_within_limit(self) -> None:
        coordinator = ConcurrencyCoordinator(max_concurrency_per_key=2)
        key = "acc_worker"

        async def worker(val: int) -> int:
            return val * 2

        res = await coordinator.run_keyed(key, worker, 21)
        self.assertEqual(res, 42)
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
        cache.put("k2", "v2")
        cache.put("k3", "v3")

        self.assertIsNone(cache.get("k1"))
        self.assertEqual(cache.get("k2"), "v2")
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

    def test_cache_mapping_and_dictionary_compatibility(self) -> None:
        cache: MultiDimensionBoundedCache[str, str] = MultiDimensionBoundedCache(
            max_entries=5,
            max_bytes=1000,
        )
        self.assertEqual(cache, {})
        self.assertFalse(cache)
        self.assertEqual(len(cache), 0)

        cache["k1"] = "val1"
        self.assertIn("k1", cache)
        self.assertNotIn("k2", cache)
        self.assertTrue(cache)
        self.assertEqual(len(cache), 1)
        self.assertEqual(cache["k1"], "val1")
        self.assertGreater(cache.current_bytes, 0)

        self.assertEqual(cache.pop("k1"), "val1")
        self.assertNotIn("k1", cache)
        self.assertEqual(cache, {})

        cache["k2"] = "val2"
        cache.clear()
        self.assertEqual(len(cache), 0)
        with self.assertRaises(KeyError):
            _ = cache["missing"]


class CoreResilienceTests(unittest.IsolatedAsyncioTestCase):
    async def test_rate_limit_blocking_and_recovery(self) -> None:
        registry = ProviderResilienceRegistry()
        key = "grok:acc_test"

        self.assertFalse(registry.is_rate_limited(key))
        registry.register_rate_limit(key, 0.02)
        self.assertTrue(registry.is_rate_limited(key))
        self.assertGreater(registry.rate_limit_remaining(key), 0.0)

        await asyncio.sleep(0.08)
        self.assertFalse(registry.is_rate_limited(key))

    async def test_execute_protected_auto_catches_rate_limit(self) -> None:
        registry = ProviderResilienceRegistry()
        key = "chatgpt:acc_fail"

        async def failing_call() -> None:
            raise RateLimited(retry_after=1.0)

        with self.assertRaises(RateLimited):
            await registry.execute_protected(key, failing_call)

        self.assertTrue(registry.is_rate_limited(key))

        with self.assertRaises(RateLimited):
            await registry.execute_protected(key, failing_call)

    async def test_rate_limited_does_not_trip_circuit_breaker(self) -> None:
        registry = ProviderResilienceRegistry(failure_threshold=2)
        key = "grok:acc_rl"

        async def rate_limited_call() -> None:
            raise RateLimited(retry_after=0.02)

        for _ in range(5):
            with self.assertRaises(RateLimited):
                await registry.execute_protected(key, rate_limited_call)
            await asyncio.sleep(0.05)

        self.assertEqual(registry.circuit_state(), "closed")


class CoreScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_resource_scope_executes_lifo_cleanups(self) -> None:
        order: list[str] = []

        async with ProviderResourceScope() as scope:
            scope.defer(lambda: order.append("socket_closed"))
            scope.defer(lambda: order.append("temp_file_deleted"))

        self.assertEqual(order, ["temp_file_deleted", "socket_closed"])

    async def test_resource_scope_spawns_and_cancels_background_task(self) -> None:
        cancelled = False

        async def worker() -> None:
            nonlocal cancelled
            try:
                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                cancelled = True
                raise

        scope = ProviderResourceScope()
        scope.spawn(worker())
        await asyncio.sleep(0.01)
        await scope.aclose()

        self.assertTrue(cancelled)


class CorePortalBridgeTests(unittest.TestCase):
    def test_portal_bridge_runs_coroutine_synchronously(self) -> None:
        with SyncPortalBridge(name="TestPortal") as bridge:

            async def add(a: int, b: int) -> int:
                await asyncio.sleep(0.001)
                return a + b

            result = bridge.call(add(15, 27))
            self.assertEqual(result, 42)


class CoreStateTests(unittest.TestCase):
    def test_operation_state_machine_valid_transitions(self) -> None:
        sm = OperationStateMachine()
        self.assertEqual(sm.state, OperationState.PENDING.value)
        self.assertFalse(sm.is_terminal)

        sm.transition(OperationState.PREPARING)
        self.assertEqual(sm.state, OperationState.PREPARING.value)

        sm.transition(OperationState.SENDING)
        self.assertEqual(sm.state, OperationState.SENDING.value)

        sm.transition(OperationState.STREAMING)
        self.assertEqual(sm.state, OperationState.STREAMING.value)

        sm.transition(OperationState.COMPLETED)
        self.assertEqual(sm.state, OperationState.COMPLETED.value)
        self.assertTrue(sm.is_terminal)

    def test_operation_state_machine_invalid_transition(self) -> None:
        sm = OperationStateMachine()
        self.assertFalse(sm.can_transition(OperationState.STREAMING))


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
