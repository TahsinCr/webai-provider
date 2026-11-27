"""Grok image generation payload and generatedImageUrls artifact helpers."""

from __future__ import annotations

from ...resources.tools.types import ImageGenerationTool, Tool, WebSearchTool


def apply_grok_tool_payload(body: dict[str, object], tool: Tool | None) -> None:
    """Apply explicit WebSearchTool or ImageGenerationTool flags to Grok send payload."""
    if isinstance(tool, WebSearchTool):
        body["disableSearch"] = False
    elif isinstance(tool, ImageGenerationTool):
        body["enableImageGeneration"] = True
