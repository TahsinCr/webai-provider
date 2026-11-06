from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Citation:
    url: str
    title: str = ""
    snippet: str = ""
    start_index: int | None = None
    end_index: int | None = None


def extract_citations(payload: dict[str, Any] | list[Any]) -> list[Citation]:
    citations: list[Citation] = []
    seen_urls: set[str] = set()

    def add_citation(
        url: str,
        title: str = "",
        snippet: str = "",
        start: int | None = None,
        end: int | None = None,
    ):
        clean_url = url.strip()
        if not clean_url or clean_url in seen_urls:
            return
        seen_urls.add(clean_url)
        citations.append(
            Citation(
                url=clean_url,
                title=title.strip(),
                snippet=snippet.strip(),
                start_index=start,
                end_index=end,
            )
        )

    if isinstance(payload, dict):
        results = (
            payload.get("webSearchResults")
            or payload.get("sources")
            or payload.get("citations")
            or []
        )
        if isinstance(results, list):
            for item in results:
                if isinstance(item, dict):
                    url = item.get("url") or item.get("link") or ""
                    title = item.get("title") or item.get("name") or ""
                    snippet = item.get("snippet") or item.get("snippetText") or ""
                    start = item.get("startIndex") or item.get("start")
                    end = item.get("endIndex") or item.get("end")
                    add_citation(url, title, snippet, start, end)
        metadata = payload.get("metadata")
        if isinstance(metadata, dict) and metadata:
            citations.extend(extract_citations(metadata))
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                citations.extend(extract_citations(item))

    return citations


def sources_from(node: dict[str, Any]) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    seen: set[str] = set()
    raw = (
        node.get("metadata", {}).get("citations")
        if isinstance(node.get("metadata"), dict)
        else None
    )
    if not isinstance(raw, list):
        raw = node.get("sources") or node.get("webSearchResults") or []

    for item in raw:
        if not isinstance(item, dict):
            continue
        url = (item.get("url") or item.get("link") or "").strip()
        title = (item.get("title") or item.get("name") or "").strip()
        key = url or title
        if not key or key in seen:
            continue
        seen.add(key)
        sources.append({"url": url, "title": title})
    return sources
