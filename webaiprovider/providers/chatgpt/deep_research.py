"""ChatGPT Deep Research payload hooks, venus_plan parser, and call_mcp polling."""

from __future__ import annotations

from typing import Any

from ...resources.response.artifact import Artifact
from ...resources.response.parts import TextPart, ThinkingPart
from ...resources.response.research import ResearchPlan, ResearchState, ResearchStatus
from ...resources.response.response import Response
from ...resources.response.source import Source

CALL_MCP_PATH = "/backend-api/ecosystem/call_mcp"
DEEP_RESEARCH_APP_URI = "connectors://connector_openai_deep_research"
DEEP_RESEARCH_HINT = "plugin:connector_openai_deep_research"


def apply_deep_research_payload(body: dict[str, Any], prompt: str) -> None:
    """Attach Deep Research system_hints, response contract, and connector mention."""
    body["system_hints"] = [DEEP_RESEARCH_HINT]
    body["model_response_contracts"] = [{"id": "web.mica.v1"}]
    messages = body.get("messages")
    if isinstance(messages, list) and messages and isinstance(messages[0], dict):
        msg = messages[0]
        metadata = msg.setdefault("metadata", {})
        if isinstance(metadata, dict):
            metadata["system_hints"] = [DEEP_RESEARCH_HINT]
        content = msg.get("content")
        if isinstance(content, dict) and isinstance(content.get("parts"), list):
            parts = content["parts"]
            mention_text = (
                f"&#x20;[$derin-ara-t-rma](app://connector_openai_deep_research) {prompt}"
            )
            if parts and isinstance(parts[-1], str):
                parts[-1] = mention_text
            else:
                parts.append(mention_text)


def parse_venus_plan_message(
    message_obj: dict[str, Any],
    *,
    conversation_id: str | None = None,
) -> ResearchState | None:
    """Extract ResearchState from a ChatGPT Deep Research venus_plan tool response."""
    if not isinstance(message_obj, dict):
        return None
    metadata = message_obj.get("metadata")
    if not isinstance(metadata, dict):
        return None
    sdk = metadata.get("chatgpt_sdk")
    if not isinstance(sdk, dict):
        return None
    tool_meta = sdk.get("tool_response_metadata")
    if not isinstance(tool_meta, dict):
        return None
    session_id = tool_meta.get("async_task_conversation_id")
    venus_plan = tool_meta.get("venus_plan")
    if not isinstance(session_id, str) or not isinstance(venus_plan, dict):
        return None
    raw_steps = venus_plan.get("steps") or ()
    steps = tuple(
        str(step.get("title") or step.get("text") or step) if isinstance(step, dict) else str(step)
        for step in raw_steps
    )
    widget_state = tool_meta.get("venus_widget_state") or {}
    raw_status = widget_state.get("status") if isinstance(widget_state, dict) else None
    status = (
        ResearchStatus.WAITING_FOR_APPROVAL
        if raw_status == "waiting_for_user_response_on_plan"
        else ResearchStatus.RUNNING
    )
    countdown = widget_state.get("countdown_seconds") if isinstance(widget_state, dict) else None
    plan = ResearchPlan(
        plan_id=str(venus_plan.get("plan_id") or session_id),
        title=str(venus_plan.get("title") or "Deep Research Plan"),
        steps=steps,
        countdown_seconds=float(countdown) if isinstance(countdown, (int, float)) else 51.0,
    )
    return ResearchState(
        status=status,
        session_id=session_id,
        plan=plan,
        conversation_id=conversation_id,
        message_id=message_obj.get("id"),
        metadata={"websocket_url": tool_meta.get("websocket_url")},
    )


def call_mcp_payload(
    action: str,
    session_id: str,
    *,
    conversation_id: str,
    message_id: str | None = None,
) -> dict[str, Any]:
    """Build POST /backend-api/ecosystem/call_mcp payload for Deep Research control/polling."""
    payload: dict[str, Any] = {
        "app_uri": DEEP_RESEARCH_APP_URI,
        "conversation_id": conversation_id,
        "launcher_host": "chatgpt",
        "method": "tools/call",
        "params": {
            "name": action,
            "arguments": {"session_id": session_id},
        },
    }
    if message_id:
        payload["message_id"] = message_id
    return payload


def parse_deep_research_get_state(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Parse call_mcp('get_state') payload; return completed report dict or None if running."""
    if not isinstance(payload, dict):
        return None
    meta = payload.get("_meta")
    if not isinstance(meta, dict):
        result = payload.get("result")
        meta = result.get("_meta") if isinstance(result, dict) else None
    if not isinstance(meta, dict):
        return None
    widget_msgs = meta.get("deep_research_widget_messages")
    if not isinstance(widget_msgs, list):
        return None

    thoughts: list[str] = []
    markdown: str | None = None
    title = "Deep Research Report"
    for msg in widget_msgs:
        if not isinstance(msg, dict):
            continue
        channel = msg.get("channel")
        content = msg.get("content") or {}
        if not isinstance(content, dict):
            continue
        ctype = content.get("content_type")
        if ctype == "thoughts":
            for item in content.get("thoughts") or ():
                if isinstance(item, dict):
                    text = item.get("summary") or item.get("content")
                    if isinstance(text, str) and text.strip():
                        thoughts.append(text.strip())
        elif channel == "final" or ctype == "text":
            parts = content.get("parts") or ()
            joined = "\n".join(str(p) for p in parts if isinstance(p, str) and p.strip())
            if joined:
                markdown = joined
                first_line = joined.splitlines()[0].lstrip("# ").strip()
                if first_line:
                    title = first_line[:200]

    if markdown is None:
        return None

    sources: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for search in meta.get("source_searches") or ():
        if not isinstance(search, dict):
            continue
        for entry in search.get("results") or search.get("sources") or ():
            if not isinstance(entry, dict):
                continue
            url = entry.get("url")
            if isinstance(url, str) and url not in seen_urls:
                seen_urls.add(url)
                sources.append(
                    {
                        "url": url,
                        "title": str(entry.get("title") or url),
                        "snippet": str(entry.get("snippet") or ""),
                        "cited": True,
                    }
                )

    return {
        "title": title,
        "markdown": markdown,
        "thoughts": "\n".join(thoughts),
        "sources": tuple(sources),
    }


def build_chatgpt_research_response(
    chat: Any,
    response: Response,
    state: ResearchState,
    result: dict[str, Any],
    *,
    artifact_type: type[Artifact] = Artifact,
    source_type: type[Source] = Source,
) -> Response:
    """Build a completed Response containing the ChatGPT Deep Research Markdown artifact."""
    title = str(result.get("title") or "Deep Research Report")
    markdown = str(result.get("markdown") or "")
    thoughts = str(result.get("thoughts") or "")
    parts = []
    if thoughts:
        parts.append(ThinkingPart("research:thoughts", thoughts))
    parts.append(TextPart("research:summary", title))
    artifact = artifact_type(
        id=f"research:{state.session_id or state.conversation_id or 'report'}",
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
