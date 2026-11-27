"""Gemini image generation protocol fields and artifact extraction helpers."""

from __future__ import annotations

import re
from typing import Any

from .media import PLACEHOLDER, candidate_files, valid_media_url
from .proto import ProtoView

GEMINI_IMAGE_TOOL_FIELDS: dict[int, Any] = {
    18: bytes.fromhex("0a020802"),
    50: 0,
}

_GG_IMAGE_URL_RE = re.compile(r"https://lh3\.googleusercontent\.com/gg/[^\s\"'<>)\]]+")


def strip_image_placeholders(text: str) -> str:
    """Remove Gemini image placeholder URLs so image-only turns yield empty text."""
    if not text:
        return ""
    cleaned = PLACEHOLDER.sub("", text)
    return cleaned.strip()


def extract_gemini_image_artifacts(candidate: Any) -> list[dict[str, Any]]:
    """Extract native JPEG/PNG image artifacts from a Gemini hNvQHb candidate."""
    artifacts: list[dict[str, Any]] = []
    for index, item in enumerate(candidate_files(candidate)):
        url = item.get("url")
        if not isinstance(url, str) or not valid_media_url(url):
            continue
        artifacts.append(
            {
                "id": item.get("alias") or f"gemini-image-{index}",
                "kind": "image",
                "name": item.get("name") or f"image-{index + 1}.jpg",
                "media_type": item.get("media_type") or "image/jpeg",
                "remote_url": url,
            }
        )
    return artifacts


def extract_proto_image_files(view: ProtoView) -> list[dict[str, Any]]:
    """Extract generated image URLs from a processSession protobuf packet."""
    seen: set[str] = set()
    found: list[dict[str, Any]] = []

    def scan_bytes(raw: bytes) -> None:
        text = raw.decode("utf-8", errors="ignore")
        for match in _GG_IMAGE_URL_RE.finditer(text):
            url = match.group(0)
            if url not in seen and valid_media_url(url):
                seen.add(url)
                idx = len(found) + 1
                found.append(
                    {
                        "alias": f"gemini-image:{idx}",
                        "name": f"image-{idx}.jpg",
                        "url": url,
                        "media_type": "image/jpeg",
                        "kind": "image",
                    }
                )

    block = view.at(2, 7, 1, 4)
    if block is not None:
        scan_bytes(block._raw)
    return found
