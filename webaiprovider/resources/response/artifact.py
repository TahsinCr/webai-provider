from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import SplitResult, unquote, urlsplit

from ...collections import Collection
from .source import _clean_text, _has_control_chars

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
    """Provider-registered file reference validator and markdown link extractor."""

    name: str
    parse_target: Callable[[str, SplitResult], bool]
    matches_identity: Callable[[str, str | None], bool] | None = None
    markdown_link_re: re.Pattern[str] | None = None


_FILE_TARGET_RULES: dict[str, FileTargetRule] = {}


def register_file_target_rule(rule: FileTargetRule) -> None:
    """Register or replace a provider-specific file target normalization rule."""
    _FILE_TARGET_RULES[rule.name] = rule


def _parse_file_target(target: str, rules: Iterable[FileTargetRule]) -> SplitResult | None:
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
    for rule in rules:
        if rule.parse_target(target, parsed):
            return parsed
    return None


def _extract_file(
    value: Any,
    seen: set[str],
    owner: str | None,
    rules: tuple[FileTargetRule, ...],
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
    if isinstance(identity, str) and any(
        rule.matches_identity is not None and rule.matches_identity(identity, target)
        for rule in rules
    ):
        target = identity
    if not target or target in seen:
        return None
    parsed = _parse_file_target(target, rules)
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
    rules: Iterable[FileTargetRule] | None = None,
) -> tuple[dict[str, Any], ...]:
    """Extract validated file/artifact references from provider payloads."""
    active_rules = tuple(rules) if rules is not None else tuple(_FILE_TARGET_RULES.values())
    files: list[dict[str, Any]] = []
    seen: set[str] = set()
    remaining = 2048

    def add(value: Any, owner: str | None = None) -> None:
        if len(files) >= 128:
            return
        entry = _extract_file(value, seen, owner, active_rules)
        if entry is not None:
            files.append(entry)

    def visit(value: Any, depth: int = 0, resource: bool = False, owner: str | None = None) -> None:
        nonlocal remaining
        if depth > 7 or remaining <= 0 or len(files) >= 128:
            return
        remaining -= 1
        if isinstance(value, (list, tuple)):
            for item in value[:128]:
                visit(item, depth + 1, resource, owner)
        elif isinstance(value, str):
            for rule in active_rules:
                if rule.markdown_link_re is None:
                    continue
                for match in rule.markdown_link_re.finditer(value):
                    add({"name": match[1], "url": match[2]}, owner)
        elif isinstance(value, dict):
            if resource or any(
                value.get(key) for key in ("file_path", "download_url", "asset_pointer")
            ):
                add(value, owner)
            visit(value.get("text"), depth + 1, owner=owner)
            for key in _FILE_RESOURCE_KEYS:
                visit(value.get(key), depth + 1, True, owner)
            for key in _FILE_NESTED_KEYS:
                visit(value.get(key), depth + 1, owner=owner)

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
