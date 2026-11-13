from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from eventflow import CircuitBreaker, CooldownRegistry

from ..errors import RateLimited

T = TypeVar("T")


def _is_infrastructure_failure(exc: BaseException) -> bool:
    if isinstance(exc, (RateLimited, asyncio.CancelledError)):
        return False
    return getattr(exc, "code", None) not in {"rate_limited", "account_paused"}


class ProviderResilienceRegistry:
    __slots__ = ("_cooldowns", "_circuit_breaker")

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        max_cooldown_entries: int = 4096,
    ) -> None:
        self._cooldowns = CooldownRegistry(max_entries=max_cooldown_entries)
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            should_count=_is_infrastructure_failure,
        )

    def register_rate_limit(self, key: str, retry_after: float) -> None:
        if retry_after > 0:
            self._cooldowns.block(key, retry_after)

    def is_rate_limited(self, key: str) -> bool:
        return self._cooldowns.is_blocked(key)

    def rate_limit_remaining(self, key: str) -> float:
        return self._cooldowns.remaining(key)

    def circuit_state(self) -> str:
        return self._circuit_breaker.state

    async def execute_protected(
        self,
        key: str,
        operation: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        if self._cooldowns.is_blocked(key):
            remaining = self._cooldowns.remaining(key)
            raise RateLimited(retry_after=float(remaining) or 1.0)

        try:
            return await self._circuit_breaker.call_async(operation, *args, **kwargs)
        except RateLimited as exc:
            retry_after = getattr(exc, "retry_after", None)
            if retry_after and isinstance(retry_after, (int, float)) and retry_after > 0:
                self.register_rate_limit(key, float(retry_after))
            raise
