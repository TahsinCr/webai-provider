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


def with_research_status(
    chat: Any,
    response: Any,
    state: ResearchState,
    status: ResearchStatus,
) -> Any:
    """Return a shallow Response copy with an updated ResearchStatus."""
    from .response import Response

    updated = ResearchState(
        status=status,
        session_id=state.session_id,
        plan=state.plan,
        conversation_id=state.conversation_id,
        message_id=state.message_id,
        metadata=state.metadata,
    )
    return Response(
        parts=response.parts,
        message=response.message,
        id=response.id,
        remote_id=response.remote_id,
        model=response.model,
        sources=response.sources,
        artifacts=response.artifacts,
        research=updated,
        chat=chat,
    )


def build_research_response(
    chat: Any,
    response: Any,
    state: ResearchState,
    result: Mapping[str, Any],
    *,
    artifact_type: type[Any] | None = None,
    source_type: type[Any] | None = None,
) -> Any:
    """Build a completed Response containing a Deep Research Markdown artifact and sources."""
    from .artifact import Artifact
    from .parts import TextPart, ThinkingPart
    from .response import Response
    from .source import Source

    resolved_artifact_type = artifact_type or Artifact
    resolved_source_type = source_type or Source
    title = str(result.get("title") or "Deep Research Report")
    markdown = str(result.get("markdown") or "")
    thoughts = str(result.get("thoughts") or "")
    parts = []
    if thoughts:
        parts.append(ThinkingPart("research:thoughts", thoughts))
    parts.append(TextPart("research:summary", title))
    artifact = resolved_artifact_type(
        id=f"research:{state.session_id or state.conversation_id or 'report'}",
        kind="document",
        name=f"{title}.md",
        mime_type="text/markdown",
        size=len(markdown.encode("utf-8")),
        content=markdown,
        title=title,
    )
    sources = tuple(
        resolved_source_type(
            id=item.get("url") or f"src-{idx}",
            url=str(item.get("url") or ""),
            title=str(item.get("title") or ""),
            snippet=str(item.get("snippet") or ""),
            cited=item.get("cited"),
        )
        for idx, item in enumerate(result.get("sources") or ())
    )
    completed_state = ResearchState(
        status=ResearchStatus.COMPLETED,
        session_id=state.session_id,
        plan=state.plan,
        conversation_id=state.conversation_id,
        message_id=state.message_id,
        metadata=state.metadata,
    )
    return Response(
        parts=parts,
        message=response.message,
        id=response.id,
        remote_id=response.remote_id,
        model=response.model,
        sources=sources,
        artifacts=[artifact],
        research=completed_state,
        chat=chat,
    )
