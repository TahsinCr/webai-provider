"""Gemini Deep Research tool, source selection, and hNvQHb/MUAZcd polling helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar

from ...resources.response.research import (
    ResearchPlan,
    ResearchState,
    ResearchStatus,
    build_research_response,
)
from ...resources.tools.types import DeepResearchTool, ImageGenerationTool, Tool, WebSearchTool
from .image import GEMINI_IMAGE_TOOL_FIELDS


class DeepResearchSources(str, Enum):
    """Selectable research groundings for Gemini Deep Research."""

    WEB = "web"
    CHAT = "chat"
    WEB_AND_CHAT = "web_and_chat"


@dataclass(frozen=True, slots=True)
class GeminiDeepResearchTool(DeepResearchTool):
    """Gemini-specific Deep Research tool with explicit source selection."""

    sources: DeepResearchSources = DeepResearchSources.WEB
    provider_type: ClassVar[str | None] = "gemini"

    def __post_init__(self) -> None:
        super().__post_init__()
        if not isinstance(self.sources, DeepResearchSources):
            raise TypeError("sources must be a DeepResearchSources enum value")


def gemini_tool_fields(tool: Tool | None) -> dict[int, Any] | None:
    """Map a validated Tool instance to Gemini StreamGenerate JSPB body fields."""
    if tool is None:
        return None
    if isinstance(tool, WebSearchTool):
        return {49: 13}
    if isinstance(tool, ImageGenerationTool):
        return dict(GEMINI_IMAGE_TOOL_FIELDS)
    if isinstance(tool, (DeepResearchTool, GeminiDeepResearchTool)):
        return {49: 1}
    return None


def build_start_research_fields(
    conversation_id: str,
    turn_token: str | None = None,
    *,
    prompt: str = "Araştırmayı başlatın",
) -> dict[int, Any]:
    """Build StreamGenerate JSPB body fields for starting a planned Gemini Deep Research."""
    ctx: list[Any] = [conversation_id]
    if turn_token:
        ctx.extend([None] * 8 + [turn_token])
    return {
        0: [prompt, 0, None, None, None, None, 0],
        71: ctx,
        79: 3,
        80: 1,
        91: 1,
        97: [0, 1],
        98: 1,
    }


_STEP_LINE_RE = re.compile(r"^\s*(?:\(\d+\)|\d+\.|[-*])\s+(.+)$")


def parse_gemini_research_plan(
    text: str,
    *,
    raw_plan: Any = None,
    conversation_id: str | None = None,
    message_id: str | None = None,
    turn_token: str | None = None,
) -> ResearchState:
    """Build a ResearchState from Gemini's initial planning response."""
    title = ""
    steps: list[str] = []
    if isinstance(raw_plan, list) and raw_plan:
        if isinstance(raw_plan[0], str) and raw_plan[0].strip():
            title = raw_plan[0].strip()
        sections = raw_plan[1] if len(raw_plan) > 1 and isinstance(raw_plan[1], list) else ()
        for section in sections:
            if not isinstance(section, list) or len(section) < 3:
                continue
            body = section[2]
            if not isinstance(body, str):
                continue
            for line in body.splitlines():
                stripped = line.strip()
                if not stripped:
                    continue
                match = _STEP_LINE_RE.match(stripped)
                steps.append(match.group(1).strip() if match else stripped)
    if not steps:
        lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
        for line in lines:
            match = _STEP_LINE_RE.match(line)
            if match:
                steps.append(match.group(1).strip())
            elif not title and not line.endswith(":"):
                title = line.lstrip("# ").strip()
    plan = ResearchPlan(
        plan_id=conversation_id,
        title=title or "Deep Research Plan",
        steps=tuple(steps),
    )
    return ResearchState(
        status=ResearchStatus.WAITING_FOR_APPROVAL,
        session_id=conversation_id,
        plan=plan,
        conversation_id=conversation_id,
        message_id=message_id,
        metadata={"turn_token": turn_token} if turn_token else {},
    )


def parse_gemini_research_hnvqhb(turns_payload: Any) -> dict[str, Any] | None:
    """Extract final Deep Research report, thoughts, and sources from hNvQHb turns."""
    if not isinstance(turns_payload, list):
        return None
    thoughts: list[str] = []
    sources: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    report_title = "Deep Research Report"
    report_markdown: str | None = None

    def visit(node: Any) -> None:
        nonlocal report_title, report_markdown
        if isinstance(node, list):
            # Immersive entry report pattern: [im_id, None, title, "agency-placeholder-task-id", "# Markdown..."]
            if (
                len(node) >= 5
                and isinstance(node[2], str)
                and node[3] == "agency-placeholder-task-id"
                and isinstance(node[4], str)
                and node[4].strip()
            ):
                report_title = node[2].strip() or report_title
                report_markdown = node[4]
            # Thinking step pattern with agency-placeholder-task-id
            if (
                len(node) >= 2
                and node[0] == "agency-placeholder-task-id"
                and isinstance(node[1], str)
            ):
                step_text = node[1].strip()
                if step_text and step_text not in thoughts:
                    thoughts.append(step_text)
            for item in node:
                visit(item)
        elif isinstance(node, dict):
            url = node.get("url") or node.get("uri")
            if (
                isinstance(url, str)
                and url.startswith(("http://", "https://"))
                and url not in seen_urls
            ):
                seen_urls.add(url)
                sources.append(
                    {
                        "url": url,
                        "title": str(node.get("title") or url),
                        "cited": bool(node.get("cited", True)),
                    }
                )
            for val in node.values():
                visit(val)

    visit(turns_payload)
    if report_markdown is None:
        return None
    return {
        "title": report_title,
        "markdown": report_markdown,
        "thoughts": "\n".join(thoughts),
        "sources": tuple(sources),
    }


def build_gemini_research_response(
    chat: Any,
    response: Any,
    state: ResearchState,
    result: dict[str, Any],
    *,
    artifact_type: type[Any] | None = None,
    source_type: type[Any] | None = None,
) -> Any:
    """Build a completed Response containing the Gemini Deep Research Markdown artifact."""
    return build_research_response(
        chat,
        response,
        state,
        result,
        artifact_type=artifact_type,
        source_type=source_type,
    )
