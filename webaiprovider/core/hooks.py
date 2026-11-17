from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from .types import HookDispatchResult, OperationContext

_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _HookEntry:
    callback: Callable[..., Any]
    off_loop: bool = False


class OperationHooks:
    """Unified before/success/error/cancelled/finally and stream-stage hook registry."""

    def __init__(self) -> None:
        self._handlers: dict[str, list[_HookEntry]] = {}
        self._lifecycle: dict[str, dict[str, list[_HookEntry]]] = {}

    def on(
        self,
        event_name: str,
        handler: Callable[..., Any] | None = None,
        *,
        off_loop: bool = False,
    ) -> Any:
        def register(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._handlers.setdefault(event_name, []).append(_HookEntry(fn, off_loop=off_loop))
            return fn

        if handler is not None:
            return register(handler)
        return register

    def add(
        self,
        event_name: str,
        handler: Callable[..., Any] | None = None,
        *,
        off_loop: bool = False,
    ) -> Any:
        return self.on(event_name, handler, off_loop=off_loop)

    def _register_stage(
        self,
        stage: str,
        event_or_handler: str | Callable[..., Any] | None = None,
        handler: Callable[..., Any] | None = None,
        *,
        off_loop: bool = False,
    ) -> Any:
        if callable(event_or_handler) and handler is None:
            self._lifecycle.setdefault(stage, {}).setdefault("*", []).append(
                _HookEntry(event_or_handler, off_loop=off_loop)
            )
            return event_or_handler
        event_name = event_or_handler if isinstance(event_or_handler, str) else "*"

        def register(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._lifecycle.setdefault(stage, {}).setdefault(event_name, []).append(
                _HookEntry(fn, off_loop=off_loop)
            )
            return fn

        if handler is not None:
            return register(handler)
        return register

    def before(
        self,
        event_or_handler: str | Callable[..., Any] | None = None,
        handler: Callable[..., Any] | None = None,
    ) -> Any:
        return self._register_stage("before", event_or_handler, handler)

    def on_success(
        self,
        event_or_handler: str | Callable[..., Any] | None = None,
        handler: Callable[..., Any] | None = None,
    ) -> Any:
        return self._register_stage("on_success", event_or_handler, handler)

    def on_error(
        self,
        event_or_handler: str | Callable[..., Any] | None = None,
        handler: Callable[..., Any] | None = None,
    ) -> Any:
        return self._register_stage("on_error", event_or_handler, handler)

    def on_cancelled(
        self,
        event_or_handler: str | Callable[..., Any] | None = None,
        handler: Callable[..., Any] | None = None,
    ) -> Any:
        return self._register_stage("on_cancelled", event_or_handler, handler)

    def on_finally(
        self,
        event_or_handler: str | Callable[..., Any] | None = None,
        handler: Callable[..., Any] | None = None,
    ) -> Any:
        return self._register_stage("on_finally", event_or_handler, handler)

    @staticmethod
    async def _invoke_entry(entry: _HookEntry, arg: Any) -> Any:
        if entry.off_loop and not inspect.iscoroutinefunction(entry.callback):
            result = await asyncio.to_thread(entry.callback, arg)
        else:
            result = entry.callback(arg)
        if inspect.isawaitable(result):
            return await result
        return result

    async def emit(self, event: Any) -> None:
        event_name = getattr(event, "event", str(event))
        entries = list(self._handlers.get(event_name, ())) + list(self._handlers.get("*", ()))
        for entry in entries:
            try:
                await self._invoke_entry(entry, event)
            except Exception as error:
                _log.warning(
                    "Operation hook failed: event=%s error_type=%s",
                    event_name,
                    type(error).__name__,
                )

    def _entries_for(self, stage: str, operation_name: str) -> list[_HookEntry]:
        stage_map = self._lifecycle.get(stage)
        if stage_map is None:
            return []
        exact = stage_map.get(operation_name)
        wildcard = stage_map.get("*")
        if not exact and not wildcard:
            return []
        result: list[_HookEntry] = []
        if exact:
            result.extend(exact)
        if wildcard:
            result.extend(wildcard)
        return result

    @staticmethod
    async def run_before(
        hooks_chain: Sequence[OperationHooks],
        operation_name: str,
        context: OperationContext,
    ) -> OperationContext:
        for hooks in hooks_chain:
            for entry in hooks._entries_for("before", operation_name):
                updated = await hooks._invoke_entry(entry, context)
                if isinstance(updated, OperationContext):
                    context = updated
        return context

    @staticmethod
    async def run_lifecycle(
        hooks_chain: Sequence[OperationHooks],
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
                    await hooks._invoke_entry(entry, context)
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


class SyncOperationHooks:
    """Thread-safe synchronous decorator/registration facade over OperationHooks."""

    def __init__(self, target: OperationHooks) -> None:
        self._target = target

    def on(self, event_name: str, handler: Callable[..., Any] | None = None) -> Any:
        return self._target.on(event_name, handler, off_loop=True)

    def add(self, event_name: str, handler: Callable[..., Any] | None = None) -> Any:
        return self._target.add(event_name, handler, off_loop=True)

    def before(
        self,
        event_or_handler: str | Callable[..., Any] | None = None,
        handler: Callable[..., Any] | None = None,
    ) -> Any:
        return self._target._register_stage("before", event_or_handler, handler, off_loop=True)

    def on_success(
        self,
        event_or_handler: str | Callable[..., Any] | None = None,
        handler: Callable[..., Any] | None = None,
    ) -> Any:
        return self._target._register_stage("on_success", event_or_handler, handler, off_loop=True)

    def on_error(
        self,
        event_or_handler: str | Callable[..., Any] | None = None,
        handler: Callable[..., Any] | None = None,
    ) -> Any:
        return self._target._register_stage("on_error", event_or_handler, handler, off_loop=True)

    def on_cancelled(
        self,
        event_or_handler: str | Callable[..., Any] | None = None,
        handler: Callable[..., Any] | None = None,
    ) -> Any:
        return self._target._register_stage(
            "on_cancelled", event_or_handler, handler, off_loop=True
        )

    def on_finally(
        self,
        event_or_handler: str | Callable[..., Any] | None = None,
        handler: Callable[..., Any] | None = None,
    ) -> Any:
        return self._target._register_stage("on_finally", event_or_handler, handler, off_loop=True)
