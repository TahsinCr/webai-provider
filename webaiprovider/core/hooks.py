from __future__ import annotations

from collections.abc import Callable
from typing import Any


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
