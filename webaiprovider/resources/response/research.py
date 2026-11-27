from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ResearchStatus(str, Enum):
    """Lifecycle status for a multi-stage deep research task."""

    PLANNING = "planning"
    WAITING_FOR_APPROVAL = "waiting_for_approval"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ResearchPlan:
    """Structured execution plan proposed before deep research begins."""

    plan_id: str | None = None
    title: str = ""
    steps: tuple[str, ...] = ()
    countdown_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class ResearchState:
    """Provider-agnostic snapshot of an in-flight or completed deep research session."""

    status: ResearchStatus
    session_id: str | None = None
    plan: ResearchPlan | None = None
    conversation_id: str | None = None
    message_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
