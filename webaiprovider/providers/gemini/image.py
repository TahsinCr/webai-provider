"""Gemini image generation protocol fields and artifact extraction helpers."""

from __future__ import annotations

from typing import Any

from .media import PLACEHOLDER, candidate_files, valid_media_url

GEMINI_IMAGE_TOOL_FIELDS: dict[int, Any] = {
    17: [[2]],
    49: 0,
}


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
