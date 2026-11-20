from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable, Sequence
from typing import Any

from .hooks import OperationHooks
from .types import HookDispatchResult, OperationContext

_log = logging.getLogger(__name__)


class AsyncOperationHooks(OperationHooks):
    """Dispatch shared hook registrations on the asynchronous execution path."""

    @staticmethod
    async def _invoke(callback: Callable[..., Any], arg: Any) -> Any:
        result = callback(arg)
        if inspect.isawaitable(result):
            return await result
        return result

    async def emit(self, event: Any) -> None:
        event_name = getattr(event, "event", None) or str(event)
        entries = list(self._handlers.get(event_name, ())) + list(self._handlers.get("*", ()))
        for entry in entries:
            try:
                await self._invoke(entry, event)
            except Exception as error:
                _log.warning(
                    "Operation hook failed: event=%s error_type=%s",
                    event_name,
                    type(error).__name__,
                )

    def _entries_for(self, stage: str, operation_name: str) -> list[Callable[..., Any]]:
        stage_map = self._lifecycle.get(stage)
        if stage_map is None:
            return []
        exact = stage_map.get(operation_name)
        wildcard = stage_map.get("*")
        if not exact and not wildcard:
            return []
        result: list[Callable[..., Any]] = []
        if exact:
            result.extend(exact)
        if wildcard:
            result.extend(wildcard)
        return result

    @staticmethod
    async def run_before(
        hooks_chain: Sequence[AsyncOperationHooks],
        operation_name: str,
        context: OperationContext,
    ) -> OperationContext:
        for hooks in hooks_chain:
            for entry in hooks._entries_for("before", operation_name):
                updated = await hooks._invoke(entry, context)
                if isinstance(updated, OperationContext):
                    context = updated
        return context

    @staticmethod
    async def run_lifecycle(
        hooks_chain: Sequence[AsyncOperationHooks],
        stage: str,
        operation_name: str,
        context: OperationContext,
    ) -> HookDispatchResult:
        matched = 0
        delivered = 0
        errors: list[Exception] = []
        for hooks in hooks_chain:
            for entry in hooks._entries_for(stage, operation_name):
                matched += 1
                try:
                    await hooks._invoke(entry, context)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    errors.append(error)
                    _log.warning(
                        "Operation lifecycle hook failed: operation=%s stage=%s error_type=%s",
                        operation_name,
                        stage,
                        type(error).__name__,
                    )
                else:
                    delivered += 1
        result = HookDispatchResult(
            matched=matched,
            delivered=delivered,
            failed=len(errors),
            errors=tuple(errors),
        )
        results = context.metadata.setdefault("hook_results", {})
        if isinstance(results, dict):
            results[stage] = result
        return result
