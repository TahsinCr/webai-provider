import json
import re
from collections.abc import Callable, Iterable
from itertools import chain, islice
from typing import Any

from ...resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    ProviderRequest,
    ResearchPlanReceived,
)
from ...resources.response.artifact import FileTargetRule, files_from
from ...resources.response.parts import ResponsePart, TextPart, ThinkingPart
from ...resources.response.source import sources_from
from .deep_research import parse_venus_plan_message
from .image import extract_chatgpt_image_artifacts

_FILE_SERVICE_RE = re.compile(r"file[-_][A-Za-z0-9_-]+")
_CHATGPT_SANDBOX_RE = re.compile(r"\[([^\]\n]{1,200})\]\(((?:sandbox:)?/mnt/data/[^\s)]+)\)")

CHATGPT_FILE_RULE = FileTargetRule(
    parse_target=lambda _target, parsed: (
        (
            parsed.scheme in ("file-service", "sediment")
            and _FILE_SERVICE_RE.fullmatch(parsed.netloc) is not None
            and not parsed.path
        )
        or (
            parsed.scheme in ("", "sandbox")
            and parsed.path.startswith("/mnt/data/")
            and not parsed.netloc
        )
    ),
    matches_identity=lambda identity, target: not target and identity.startswith("sandbox:/mnt/"),
    markdown_link_re=_CHATGPT_SANDBOX_RE,
    markdown_hint="/mnt/data/",
)


def message_files(*messages):
    attachments = []
    for message in messages:
        metadata = message.get("metadata") or {}
        for item in islice(
            chain(metadata.get("attachments") or (), message.get("attachments") or ()), 128
        ):
            if not isinstance(item, dict):
                continue
            identifier = item.get("id") or item.get("file_id")
            if isinstance(identifier, str):
                attachments.append(
                    dict(item, url="file-service://" + identifier, message_id=message.get("id"))
                )
    return files_from({"files": attachments}, *messages, rule=CHATGPT_FILE_RULE)


def message_parts(content: Any) -> list[Any]:
    if not isinstance(content, dict):
        return []
    content_type = content.get("content_type")
    if content_type == "thoughts":
        thoughts = content.get("thoughts") or []
        return [
            item.get("summary") or item.get("content", "")
            for item in thoughts
            if isinstance(item, dict)
        ]
    if content_type in ("reasoning_recap", "reasoning_summary"):
        return [content.get("content") or content.get("text") or ""]
    parts = content.get("parts", [])
    return parts if isinstance(parts, list) else []


def text_from_parts(parts: Iterable[Any]) -> str:
    rendered: list[str] = []
    for part in parts:
        if isinstance(part, str):
            rendered.append(part)
        elif isinstance(part, dict):
            text = part.get("text")
            if isinstance(text, str):
                rendered.append(text)
            else:
                rendered.append(json.dumps(part, ensure_ascii=False, sort_keys=True))
        elif part is not None:
            rendered.append(str(part))
    return "\n".join(rendered)


def emit_post_send_events(
    remote_id: str,
    completed: dict[str, Any],
    previous: Iterable[str] | None,
    request: ProviderRequest,
    publish: Callable[[tuple[str, object]], None],
    *,
    emit_bound: bool = True,
) -> None:
    response_messages = completed.get("messages", [])
    previous_ids = set(previous or ())
    turn_start = len(response_messages)
    while turn_start:
        item = response_messages[turn_start - 1]
        if (item.get("author") or {}).get("role") == "user":
            break
        turn_start -= 1
    user = response_messages[turn_start - 1] if turn_start else {}
    final = None
    parts: list[ResponsePart] = []
    tool_images: list[dict[str, Any]] = []
    research_state = None
    for item in response_messages[turn_start:]:
        if not item.get("id") or item["id"] in previous_ids:
            continue
        role = (item.get("author") or {}).get("role")
        if role == "tool":
            tool_images.extend(extract_chatgpt_image_artifacts(item))
            if research_state is None:
                research_state = parse_venus_plan_message(item, conversation_id=remote_id)
            continue
        if (
            item.get("recipient") not in (None, "all")
            or (item.get("metadata") or {}).get("is_visually_hidden_from_conversation")
            or role != "assistant"
        ):
            continue
        content = item.get("content") or {}
        thinking = item.get("channel") == "analysis" or content.get("content_type") in (
            "thoughts",
            "reasoning_recap",
            "reasoning_summary",
        )
        if not thinking:
            final = item
        text = text_from_parts(message_parts(content))
        if text:
            part_type = ThinkingPart if thinking else TextPart
            parts.append(part_type(f"{'thinking' if thinking else 'text'}:{item['id']}", text))
    if research_state is not None and research_state.plan is not None:
        publish(("event", ResearchPlanReceived(plan=research_state.plan, state=research_state)))
    if final or tool_images or research_state is not None:
        metadata = (final or {}).get("metadata") or {}
        combined_files = tuple(tool_images) + message_files(*response_messages[turn_start:])
        publish(
            (
                "event",
                MessageMetadata(
                    (final or {}).get("id"),
                    user.get("id"),
                    metadata.get("model_slug") or request.model,
                    metadata.get("reasoning_effort") or request.effort,
                    sources_from(final) if final else (),
                    files=combined_files,
                    parts=tuple(parts),
                    research=research_state,
                ),
            )
        )
    elif parts:
        publish(("event", MessageMetadata(parts=tuple(parts))))
    if emit_bound:
        publish(("event", ConversationBound(remote_id)))
