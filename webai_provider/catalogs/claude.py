from __future__ import annotations

from ..contracts import ModelRef
from .base import StaticModelCatalog


class ClaudeModels(StaticModelCatalog):
    provider = "claude"

    SONNET_5_5 = ModelRef(
        "claude",
        "claude-sonnet-5-5",
        "Claude Sonnet 5.5",
        capabilities=("low", "medium", "high", "xhigh", "max"),
    )
    HAIKU_5_5 = ModelRef(
        "claude",
        "claude-haiku-5-5",
        "Claude Haiku 5.5",
        capabilities=("low", "medium", "high", "xhigh", "max"),
    )
    SONNET_5 = ModelRef(
        "claude",
        "claude-sonnet-5",
        "Claude Sonnet 5",
        capabilities=(
            "low:auto",
            "low:off",
            "medium:auto",
            "medium:off",
            "high:auto",
            "high:off",
            "xhigh:auto",
            "xhigh:off",
            "max:auto",
            "max:off",
        ),
    )
    SONNET_4_6 = ModelRef(
        "claude",
        "claude-sonnet-4-6",
        "Claude Sonnet 4.6",
        capabilities=(
            "low:auto",
            "low:off",
            "medium:auto",
            "medium:off",
            "high:auto",
            "high:off",
            "max:auto",
            "max:off",
        ),
    )
