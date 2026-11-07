from __future__ import annotations

from ..contracts import ModelRef
from .base import StaticModelCatalog, effort_refs


class ClaudeModels(StaticModelCatalog):
    provider = "claude"

    OPUS_4_5 = ModelRef("claude", "claude-opus-4-5-20251101", "Claude Opus 4.5")
    OPUS_4_6 = ModelRef("claude", "claude-opus-4-6", "Claude Opus 4.6")
    OPUS_4_7 = ModelRef("claude", "claude-opus-4-7", "Claude Opus 4.7")
    OPUS_4_8 = ModelRef("claude", "claude-opus-4-8", "Claude Opus 4.8")
    OPUS_5 = ModelRef(
        "claude",
        "claude-opus-5",
        "Claude Opus 5",
        efforts=effort_refs("claude", "claude-opus-5", ("low", "medium", "high", "xhigh", "max")),
    )
    OPUS_5_5 = ModelRef(
        "claude",
        "claude-opus-5-5",
        "Claude Opus 5.5",
        efforts=effort_refs("claude", "claude-opus-5-5", ("low", "medium", "high", "xhigh", "max")),
    )
    FABLE_5_1 = ModelRef(
        "claude",
        "claude-fable-5-1",
        "Claude Fable 5.1",
        efforts=effort_refs(
            "claude", "claude-fable-5-1", ("low", "medium", "high", "xhigh", "max")
        ),
        default_effort_id="medium",
    )
    FABLE_5 = ModelRef(
        "claude",
        "claude-fable-5",
        "Claude Fable 5",
        efforts=effort_refs("claude", "claude-fable-5", ("low", "medium", "high", "xhigh", "max")),
        default_effort_id="medium",
    )
    SONNET_5_5 = ModelRef(
        "claude",
        "claude-sonnet-5-5",
        "Claude Sonnet 5.5",
        efforts=effort_refs(
            "claude", "claude-sonnet-5-5", ("low", "medium", "high", "xhigh", "max")
        ),
        default_effort_id="medium",
    )
    HAIKU_5_5 = ModelRef(
        "claude",
        "claude-haiku-5-5",
        "Claude Haiku 5.5",
        efforts=effort_refs(
            "claude", "claude-haiku-5-5", ("low", "medium", "high", "xhigh", "max")
        ),
        default_effort_id="medium",
    )
    HAIKU_4_5 = ModelRef("claude", "claude-haiku-4-5-20251001", "Claude Haiku 4.5")
    SONNET_4_5 = ModelRef("claude", "claude-sonnet-4-5-20250929", "Claude Sonnet 4.5")
    SONNET_5 = ModelRef(
        "claude",
        "claude-sonnet-5",
        "Claude Sonnet 5",
        efforts=effort_refs(
            "claude",
            "claude-sonnet-5",
            (
                ("low:auto", "Low Auto"),
                ("low:off", "Low Off"),
                ("medium:auto", "Medium Auto"),
                ("medium:off", "Medium Off"),
                ("high:auto", "High Auto"),
                ("high:off", "High Off"),
                ("xhigh:auto", "Extra High Auto"),
                ("xhigh:off", "Extra High Off"),
                ("max:auto", "Max Auto"),
                ("max:off", "Max Off"),
            ),
        ),
        default_effort_id="medium:auto",
    )
    SONNET_4_6 = ModelRef(
        "claude",
        "claude-sonnet-4-6",
        "Claude Sonnet 4.6",
        efforts=effort_refs(
            "claude",
            "claude-sonnet-4-6",
            (
                ("low:auto", "Low Auto"),
                ("low:off", "Low Off"),
                ("medium:auto", "Medium Auto"),
                ("medium:off", "Medium Off"),
                ("high:auto", "High Auto"),
                ("high:off", "High Off"),
                ("max:auto", "Max Auto"),
                ("max:off", "Max Off"),
            ),
        ),
        default_effort_id="medium:auto",
    )
