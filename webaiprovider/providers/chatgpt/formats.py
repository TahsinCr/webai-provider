import json
import re
from collections.abc import Callable, Iterable
from itertools import chain, islice
from typing import Any

from ...resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    ProviderRequest,
)
from ...resources.response.artifact import FileTargetRule, files_from
from ...resources.response.parts import ResponsePart, TextPart, ThinkingPart
from ...resources.response.source import sources_from

_FILE_SERVICE_RE = re.compile(r"file[-_][A-Za-z0-9_-]+")
_CHATGPT_SANDBOX_RE = re.compile(r"\[([^\]\n]{1,200})\]\(((?:sandbox:)?/mnt/data/[^\s)]+)\)")

CHATGPT_FILE_RULE = FileTargetRule(
    parse_target=lambda _target, parsed: (
        (
            parsed.scheme == "file-service"
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
    for item in response_messages[turn_start:]:
        if (
            not item.get("id")
            or item["id"] in previous_ids
            or item.get("recipient") not in (None, "all")
            or (item.get("metadata") or {}).get("is_visually_hidden_from_conversation")
            or (item.get("author") or {}).get("role") != "assistant"
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
    if final:
        metadata = final.get("metadata") or {}
        publish(
            (
                "event",
                MessageMetadata(
                    final.get("id"),
                    user.get("id"),
                    metadata.get("model_slug") or request.model,
                    metadata.get("reasoning_effort") or request.effort,
                    sources_from(final),
                    files=message_files(*response_messages[turn_start:]),
                    parts=tuple(parts),
                ),
            )
        )
    elif parts:
        publish(("event", MessageMetadata(parts=tuple(parts))))
    if emit_bound:
        publish(("event", ConversationBound(remote_id)))
