from __future__ import annotations

from ..contracts import ModelRef
from .base import StaticModelCatalog


class ClaudeModels(StaticModelCatalog):
    provider = "claude"

    AUTO = ModelRef("claude", "auto", "Auto")
    CLAUDE_3_7_SONNET = ModelRef(
        "claude", "claude-3-7-sonnet", "Claude 3.7 Sonnet", capabilities=("low", "medium", "high")
    )
    CLAUDE_3_5_SONNET = ModelRef("claude", "claude-3-5-sonnet", "Claude 3.5 Sonnet")
    CLAUDE_3_5_HAIKU = ModelRef("claude", "claude-3-5-haiku", "Claude 3.5 Haiku")
    CLAUDE_3_OPUS = ModelRef("claude", "claude-3-opus", "Claude 3 Opus")
