from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Any

from .types import HookDispatchResult, OperationContext

_log = logging.getLogger(__name__)


class OperationHooks:
    """Register synchronous operation callbacks."""

    def __init__(self, target: OperationHooks | None = None) -> None:
        self._handlers: dict[str, list[Callable[..., Any]]] = (
            {} if target is None else target._handlers
        )
        self._lifecycle: dict[str, dict[str, list[Callable[..., Any]]]] = (
            {} if target is None else target._lifecycle
        )

    def on(
        self,
        event_name: str,
        handler: Callable[..., Any] | None = None,
    ) -> Any:
        def register(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._handlers.setdefault(event_name, []).append(fn)
            return fn

        if handler is not None:
            return register(handler)
        return register

    def add(
        self,
        event_name: str,
        handler: Callable[..., Any] | None = None,
    ) -> Any:
        return self.on(event_name, handler)

    def _register_stage(
        self,
        stage: str,
        event_or_handler: str | Callable[..., Any] | None = None,
        handler: Callable[..., Any] | None = None,
    ) -> Any:
        if callable(event_or_handler) and handler is None:
            self._lifecycle.setdefault(stage, {}).setdefault("*", []).append(event_or_handler)
            return event_or_handler
        event_name = event_or_handler if isinstance(event_or_handler, str) else "*"

        def register(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._lifecycle.setdefault(stage, {}).setdefault(event_name, []).append(fn)
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

    def emit(self, event: Any) -> None:
        event_name = getattr(event, "event", None) or str(event)
        entries = list(self._handlers.get(event_name, ())) + list(self._handlers.get("*", ()))
        for entry in entries:
            try:
                entry(event)
            except Exception as error:
                _log.warning(
                    "Operation hook failed: event=%s error_type=%s",
                    event_name,
                    type(error).__name__,
                )

    @staticmethod
    def run_before(
        hooks_chain: Sequence[OperationHooks],
        operation_name: str,
        context: OperationContext,
    ) -> OperationContext:
        for hooks in hooks_chain:
            for entry in hooks._entries_for("before", operation_name):
                updated = entry(context)
                if isinstance(updated, OperationContext):
                    context = updated
        return context

    @staticmethod
    def run_lifecycle(
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
                    entry(context)
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
