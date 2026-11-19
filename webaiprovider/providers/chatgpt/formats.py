import json
import re
from collections.abc import Iterable
from itertools import chain, islice
from typing import Any

from ...resources.response.artifact import FileTargetRule, files_from, register_file_target_rule

_FILE_SERVICE_RE = re.compile(r"file[-_][A-Za-z0-9_-]+")
_CHATGPT_SANDBOX_RE = re.compile(r"\[([^\]\n]{1,200})\]\(((?:sandbox:)?/mnt/data/[^\s)]+)\)")

register_file_target_rule(
    FileTargetRule(
        name="chatgpt",
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
        matches_identity=lambda identity, target: (
            not target and identity.startswith("sandbox:/mnt/")
        ),
        markdown_link_re=_CHATGPT_SANDBOX_RE,
        markdown_hint="/mnt/data/",
    )
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
    return files_from({"files": attachments}, *messages)


def message_parts(content: Any) -> list[Any]:
    if not isinstance(content, dict):
        return []
    if content.get("content_type") == "thoughts":
        thoughts = content.get("thoughts") or []
        return [
            item.get("summary") or item.get("content", "")
            for item in thoughts
            if isinstance(item, dict)
        ]
    if content.get("content_type") in ("reasoning_recap", "reasoning_summary"):
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
