"""ChatGPT image generation payload hooks and image_asset_pointer artifact extraction."""

from __future__ import annotations

from typing import Any


def apply_image_tool_payload(body: dict[str, Any]) -> None:
    """Attach picture_v2 system_hints for explicit ChatGPT image generation."""
    body["system_hints"] = ["picture_v2"]
    messages = body.get("messages")
    if isinstance(messages, list) and messages and isinstance(messages[0], dict):
        metadata = messages[0].setdefault("metadata", {})
        if isinstance(metadata, dict):
            metadata["system_hints"] = ["picture_v2"]


def extract_chatgpt_image_artifacts(message_obj: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract generated PNG image_asset_pointer artifacts from a ChatGPT tool/assistant turn."""
    if not isinstance(message_obj, dict):
        return []
    content = message_obj.get("content")
    if not isinstance(content, dict) or content.get("content_type") != "multimodal_text":
        return []
    parts = content.get("parts")
    if not isinstance(parts, list):
        return []
    found: list[dict[str, Any]] = []
    for index, part in enumerate(parts):
        if not isinstance(part, dict) or part.get("content_type") != "image_asset_pointer":
            continue
        pointer = part.get("asset_pointer")
        if not isinstance(pointer, str) or not pointer.startswith(
            ("sediment://", "file-service://")
        ):
            continue
        file_id = pointer.split("://", 1)[1]
        mime = part.get("mime_type") or "image/png"
        ext = "png" if "png" in mime else "jpg"
        entry: dict[str, Any] = {
            "id": file_id,
            "kind": "image",
            "name": f"{file_id}.{ext}",
            "media_type": mime,
            "remote_url": pointer,
            "message_id": message_obj.get("id"),
        }
        if isinstance(part.get("size_bytes"), int):
            entry["size_bytes"] = part["size_bytes"]
        if isinstance(part.get("width"), int):
            entry["width"] = part["width"]
        if isinstance(part.get("height"), int):
            entry["height"] = part["height"]
        found.append(entry)
    return found
