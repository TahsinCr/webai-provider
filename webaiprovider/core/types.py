from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class HookDispatchResult:
    """Bounded summary of observer hook execution for an operation stage."""

    matched: int = 0
    delivered: int = 0
    failed: int = 0
    errors: tuple[Exception, ...] = ()


@dataclass(slots=True)
class OperationContext:
    """Sanitized mutable execution context passed to hooks and pipelines."""

    name: str
    operation_id: str
    provider_id: str
    account_id: str
    resource_id: str | None = None
    operation: Any = None
    prompt: str | None = None
    model: Any = None
    effort: str | None = None
    attachments: tuple[Any, ...] = ()
    parameters: dict[str, Any] = field(default_factory=dict)
    request: Any = None
    result: Any = None
    error: BaseException | None = None
    cancelled: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)
