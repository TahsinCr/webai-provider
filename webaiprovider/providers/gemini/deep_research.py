"""Gemini Deep Research tool, source selection, and hNvQHb/MUAZcd polling helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar

from ...resources.response.artifact import Artifact
from ...resources.response.parts import TextPart, ThinkingPart
from ...resources.response.research import ResearchPlan, ResearchState, ResearchStatus
from ...resources.response.response import Response
from ...resources.response.source import Source
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
    """Map a validated Tool instance to Gemini processSession req0 1.8.5.1 protobuf fields."""
    if tool is None:
        return None
    if isinstance(tool, WebSearchTool):
        return {50: 13}
    if isinstance(tool, ImageGenerationTool):
        return dict(GEMINI_IMAGE_TOOL_FIELDS)
    if isinstance(tool, GeminiDeepResearchTool):
        sources = tool.sources
    elif isinstance(tool, DeepResearchTool):
        sources = DeepResearchSources.WEB_AND_CHAT
    else:
        return None

    fields: dict[int, Any] = {50: 1}
    if sources is DeepResearchSources.WEB:
        fields[55] = [1]
        fields[56] = [1]
    elif sources is DeepResearchSources.WEB_AND_CHAT:
        fields[55] = [1, 7]
        fields[56] = [1, 12]
    elif sources is DeepResearchSources.CHAT:
        fields[55] = [12]
    return fields


def build_start_research_fields(
    conversation_id: str,
    turn_token: str | None = None,
    *,
    prompt: str = "Araştırmayı başlatın",
) -> dict[int, Any]:
    """Build processSession req0 1.8.5.1 fields for starting a planned Gemini Deep Research."""
    ctx: dict[int, Any] = {1: conversation_id}
    if turn_token:
        ctx[10] = turn_token
    return {
        1: prompt,
        72: ctx,
        80: 3,
        81: 1,
        92: 1,
        98: bytes.fromhex("08001001"),
        99: 1,
    }


_STEP_LINE_RE = re.compile(r"^\s*(?:\(\d+\)|\d+\.|[-*])\s+(.+)$")


def parse_gemini_research_plan(
    text: str,
    *,
    conversation_id: str | None = None,
    message_id: str | None = None,
    turn_token: str | None = None,
) -> ResearchState:
    """Build a ResearchState from Gemini's initial planning response."""
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    title = ""
    steps: list[str] = []
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
    response: Response,
    state: ResearchState,
    result: dict[str, Any],
    *,
    artifact_type: type[Artifact] = Artifact,
    source_type: type[Source] = Source,
) -> Response:
    """Build a completed Response containing the Gemini Deep Research Markdown artifact."""
    title = str(result.get("title") or "Deep Research Report")
    markdown = str(result.get("markdown") or "")
    thoughts = str(result.get("thoughts") or "")
    parts = []
    if thoughts:
        parts.append(ThinkingPart("research:thoughts", thoughts))
    parts.append(TextPart("research:summary", title))
    artifact = artifact_type(
        id=f"research:{state.conversation_id or state.session_id or 'report'}",
        kind="document",
        name=f"{title}.md",
        mime_type="text/markdown",
        size=len(markdown.encode("utf-8")),
        content=markdown,
        title=title,
    )
    sources = tuple(
        source_type(
            id=item.get("url") or f"src-{idx}",
            url=str(item.get("url") or ""),
            title=str(item.get("title") or ""),
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
