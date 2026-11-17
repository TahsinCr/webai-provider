from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from ...collections import Collection

_CTRL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f]")
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


def sources_from(*roots: Any) -> tuple[dict[str, Any], ...]:
    """Extract and sanitize web or file source citations from provider payloads."""
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


@dataclass(frozen=True)
class Source:
    """Web search or document citation attached to an assistant response."""

    id: str
    url: str
    title: str = ""
    snippet: str = ""
    kind: str = "web"
    metadata: Mapping[str, Any] = field(default_factory=dict)


class SourcesCollection(Collection[Source]):
    """Synchronous read-only local collection of response citations."""

    @classmethod
    def _extract_name(cls, item: Any) -> str:
        if isinstance(item, Source):
            return item.title or item.url or item.id
        return super()._extract_name(item)
