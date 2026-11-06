from __future__ import annotations

from ..contracts import ModelRef


class ClaudeModels:
    CLAUDE_3_5_SONNET = ModelRef("claude", "claude-3-5-sonnet", "Claude 3.5 Sonnet")
    CLAUDE_3_5_HAIKU = ModelRef("claude", "claude-3-5-haiku", "Claude 3.5 Haiku")
    CLAUDE_3_OPUS = ModelRef("claude", "claude-3-opus", "Claude 3 Opus")
    AUTO = ModelRef("claude", "auto", "Auto")
