from __future__ import annotations

import math
from dataclasses import dataclass
from typing import ClassVar

from ...base import Capability


@dataclass(frozen=True, slots=True)
class Tool:
    """Base contract for explicit per-request provider tools."""

    capability: ClassVar[Capability | None] = None
    provider_type: ClassVar[str | None] = None


@dataclass(frozen=True, slots=True)
class WebSearchTool(Tool):
    """Explicitly enable web search for a request."""

    capability: ClassVar[Capability | None] = Capability.WEB_SEARCH


@dataclass(frozen=True, slots=True)
class ImageGenerationTool(Tool):
    """Explicitly trigger image generation for a request."""

    capability: ClassVar[Capability | None] = Capability.IMAGE_GENERATION


@dataclass(frozen=True, slots=True)
class DeepResearchTool(Tool):
    """Explicitly trigger deep research for a request."""

    auto_start: bool = False
    capability: ClassVar[Capability | None] = Capability.DEEP_RESEARCH

    def __post_init__(self) -> None:
        if not isinstance(self.auto_start, bool):
            raise TypeError("auto_start must be a bool")


def validate_tool_instance(tool: object) -> Tool | None:
    """Validate that a supplied tool argument is None or a Tool instance."""
    if tool is None:
        return None
    if not isinstance(tool, Tool):
        raise TypeError(f"tool must be a Tool instance or None, got {type(tool).__name__}")
    return tool


def validate_tool_timeout(timeout: object) -> float | None:
    """Validate that tool_timeout is None or a positive finite number of seconds."""
    if timeout is None:
        return None
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise TypeError("tool_timeout must be a positive number of seconds or None")
    value = float(timeout)
    if not math.isfinite(value) or value <= 0.0:
        raise ValueError("tool_timeout must be a positive finite number of seconds")
    return value
