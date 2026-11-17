from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any
from urllib.parse import SplitResult, unquote, urlsplit

_CTRL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f]")
_MEDIA_TYPE_RE = re.compile(r"[\w!#$&^.+-]+/[\w!#$&^.+-]+")
_SOURCE_COLLECTION_KEYS = (
    "citations",
    "sources",
    "content_references",
    "annotations",
    "items",
    "grouped_webpages",
    "webSearchResults",
    "web_search_results",
)
_SOURCE_NESTED_KEYS = (
    "metadata",
    "native",
    "content",
    "candidate",
    "grounding_metadata",
    "groundingMetadata",
)
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


def _clean_text(value: Any, limit: int) -> str:
    return " ".join(_CTRL_CHAR_RE.sub("", str(value)).split())[:limit]


def _has_control_chars(text: str) -> bool:
    return _CTRL_CHAR_RE.search(text) is not None


def _is_valid_http_url(url: str) -> bool:
    if len(url) > 8192 or _has_control_chars(url):
        return False
    try:
        parsed = urlsplit(url)
    except ValueError:
        return False
    return (
        parsed.scheme in ("http", "https")
        and bool(parsed.hostname)
        and not parsed.username
        and not parsed.password
    )


def _extract_source(value: Any, seen: set[Any]) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    source = value.get("web") or value.get("source") or value.get("url_citation") or value
    if not isinstance(source, dict):
        return None
    url = source.get("url") or source.get("uri") or source.get("href")
    file_id = source.get("file_id")
    if not url and isinstance(file_id, str) and file_id and len(file_id) <= 256:
        identity = ("file", file_id)
        if identity in seen or _has_control_chars(file_id):
            return None
        seen.add(identity)
        label = source.get("title") or source.get("filename") or source.get("name") or file_id
        return {"title": _clean_text(label, 200), "file_id": file_id}
    if not isinstance(url, str) or url in seen or not _is_valid_http_url(url):
        return None
    seen.add(url)
    label = source.get("title") or source.get("name") or value.get("title") or url
    item: dict[str, Any] = {"title": _clean_text(label, 200), "url": url}
    description = (
        source.get("description")
        or source.get("snippet")
        or value.get("description")
        or value.get("snippet")
    )
    if isinstance(description, str):
        clean_description = _clean_text(description, 1200)
        if clean_description:
            item["description"] = clean_description
    return item


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


def sources_from(*roots: Any) -> tuple[dict[str, Any], ...]:
    sources: list[dict[str, Any]] = []
    seen: set[Any] = set()
    remaining_nodes = 512

    def add(value: Any) -> None:
        if len(sources) >= 128:
            return
        entry = _extract_source(value, seen)
        if entry is not None:
            sources.append(entry)

    def visit(value: Any, depth: int = 0) -> None:
        nonlocal remaining_nodes
        if depth > 4 or remaining_nodes <= 0:
            return
        remaining_nodes -= 1
        if isinstance(value, (list, tuple)):
            for item in value[:128]:
                visit(item, depth + 1)
            return
        if not isinstance(value, dict):
            return
        for key in _SOURCE_COLLECTION_KEYS:
            entries = value.get(key)
            if isinstance(entries, (list, tuple)):
                for item in entries[:128]:
                    add(item)
                    visit(item, depth + 1)
        groups = value.get("search_result_groups")
        if isinstance(groups, (list, tuple)):
            for group in groups[:128]:
                if remaining_nodes <= 0 or len(sources) >= 128:
                    break
                remaining_nodes -= 1
                if not isinstance(group, dict):
                    continue
                entries = group.get("entries")
                if not isinstance(entries, (list, tuple)):
                    continue
                for item in entries[:128]:
                    if remaining_nodes <= 0 or len(sources) >= 128:
                        break
                    remaining_nodes -= 1
                    add(item)
        for key in _SOURCE_NESTED_KEYS:
            visit(value.get(key), depth + 1)
        for key in ("grounding_chunks", "groundingChunks"):
            entries = value.get(key)
            if isinstance(entries, (list, tuple)):
                for item in entries[:128]:
                    add(item)

    for root in roots:
        visit(root)
    return tuple(sources)


def files_from(
    *roots: Any,
    rules: Iterable[FileTargetRule] | None = None,
) -> tuple[dict[str, Any], ...]:
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
