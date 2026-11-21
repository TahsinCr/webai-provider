from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import SplitResult, unquote, urlsplit

from ...collections import Collection
from .source import _clean_text, _has_control_chars

MAX_FILE_REFERENCES = 128
_MEDIA_TYPE_RE = re.compile(r"[\w!#$&^.+-]+/[\w!#$&^.+-]+")
_FILE_TARGET_KEYS = (
    "download_url",
    "remote_url",
    "file_path",
    "path",
    "url",
    "asset_pointer",
)
_FILE_RESOURCE_KEYS = (
    "files",
    "resources",
    "attachments",
    "artifacts",
    "resource",
    "file",
    "output_files",
    "files_metadata",
)
_FILE_NESTED_KEYS = ("metadata", "native", "native_message", "content", "parts", "value")


@dataclass(frozen=True, slots=True)
class FileTargetRule:
    """Provider-specific file reference validator and Markdown link extractor."""

    parse_target: Callable[[str, SplitResult], bool] | None = None
    matches_identity: Callable[[str, str | None], bool] | None = None
    markdown_link_re: re.Pattern[str] | None = None
    markdown_hint: str | None = None


def _parse_file_target(target: str, rule: FileTargetRule | None) -> SplitResult | None:
    if len(target) > 8192 or _has_control_chars(target):
        return None
    try:
        parsed = urlsplit(target)
    except ValueError:
        return None
    if parsed.scheme in ("http", "https"):
        if not parsed.hostname or parsed.username or parsed.password:
            return None
        return parsed
    if rule and rule.parse_target and rule.parse_target(target, parsed):
        return parsed
    return None


def _extract_file(
    value: Any,
    seen: set[str],
    owner: str | None,
    rule: FileTargetRule | None,
) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    raw_metadata = value.get("metadata")
    native = raw_metadata.get("native") if isinstance(raw_metadata, dict) else None
    merged = native | value if isinstance(native, dict) else value
    target = next(
        (
            merged[key]
            for key in _FILE_TARGET_KEYS
            if isinstance(merged.get(key), str) and merged[key]
        ),
        None,
    )
    identity = merged.get("id")
    if (
        isinstance(identity, str)
        and rule
        and rule.matches_identity
        and rule.matches_identity(identity, target)
    ):
        target = identity
    if not target or target in seen:
        return None
    parsed = _parse_file_target(target, rule)
    if parsed is None:
        return None
    seen.add(target)
    name = (
        merged.get("name")
        or merged.get("file_name")
        or merged.get("filename")
        or unquote(parsed.path).rsplit("/", 1)[-1]
        or parsed.netloc
        or "File"
    )
    item: dict[str, Any] = {"url": target, "name": _clean_text(name, 200)}
    message_id = merged.get("message_id") or owner
    if isinstance(message_id, str) and 0 < len(message_id) <= 256:
        item["message_id"] = message_id
    media = merged.get("media_type") or merged.get("mime_type") or merged.get("content_type")
    if isinstance(media, str) and _MEDIA_TYPE_RE.fullmatch(media):
        item["media_type"] = media[:120]
    size = merged.get("size_bytes", merged.get("size"))
    if isinstance(size, int) and not isinstance(size, bool) and size >= 0:
        item["size_bytes"] = size
    return item


def files_from(
    *roots: Any,
    rule: FileTargetRule | None = None,
    seen: set[str] | None = None,
) -> tuple[dict[str, Any], ...]:
    """Extract new file references, sharing the deduplication budget when requested."""
    files: list[dict[str, Any]] = []
    known = set() if seen is None else seen
    remaining = 2048

    def add(value: Any, owner: str | None = None) -> None:
        if len(known) >= MAX_FILE_REFERENCES:
            return
        entry = _extract_file(value, known, owner, rule)
        if entry is not None:
            files.append(entry)

    def visit(value: Any, depth: int = 0, resource: bool = False, owner: str | None = None) -> None:
        nonlocal remaining
        if depth > 7 or remaining <= 0 or len(known) >= MAX_FILE_REFERENCES:
            return
        remaining -= 1
        if isinstance(value, (list, tuple)):
            for item in value[:MAX_FILE_REFERENCES]:
                visit(item, depth + 1, resource, owner)
        elif isinstance(value, str):
            if (
                rule
                and rule.markdown_link_re
                and (rule.markdown_hint is None or rule.markdown_hint in value)
            ):
                for match in rule.markdown_link_re.finditer(value):
                    add({"name": match[1], "url": match[2]}, owner)
                    if len(known) >= MAX_FILE_REFERENCES:
                        break
        elif isinstance(value, dict):
            if resource or any(
                value.get(key) for key in ("file_path", "download_url", "asset_pointer")
            ):
                add(value, owner)
            if depth == 7:
                return
            child = value.get("text")
            if child is not None:
                visit(child, depth + 1, owner=owner)
            else:
                remaining -= 1
            for keys, is_resource in ((_FILE_RESOURCE_KEYS, True), (_FILE_NESTED_KEYS, False)):
                for key in keys:
                    if remaining <= 0 or len(known) >= MAX_FILE_REFERENCES:
                        return
                    child = value.get(key)
                    if child is None:
                        remaining -= 1
                    else:
                        visit(child, depth + 1, is_resource, owner)

    for root in roots:
        owner = (
            root.get("id")
            if isinstance(root, dict) and (root.get("role") or root.get("author"))
            else None
        )
        visit(root, owner=owner)
    return tuple(files)


@dataclass(frozen=True)
class Artifact:
    """Generated file or rich output associated with an assistant response."""

    id: str
    kind: str
    name: str
    mime_type: str = "application/octet-stream"
    size: int | None = None
    download_url: str | None = None
    content: bytes | str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


class ArtifactsCollection(Collection[Artifact]):
    """Synchronous read-only local collection of response artifacts."""
