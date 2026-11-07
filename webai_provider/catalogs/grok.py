from __future__ import annotations

from ..contracts import ModelRef
from .base import StaticModelCatalog


class GrokModels(StaticModelCatalog):
    provider = "grok"

    AUTO = ModelRef("grok", "auto", "Auto")
    GROK_3 = ModelRef("grok", "grok-3", "Grok 3")
    GROK_3_MINI = ModelRef("grok", "grok-3-mini", "Grok 3 Mini")
    GROK_3_THINK = ModelRef(
        "grok", "grok-3-think", "Grok 3 (Think)", capabilities=("low", "medium", "high")
    )
    GROK_3_DEEPSEARCH = ModelRef("grok", "grok-3-deepsearch", "Grok 3 (DeepSearch)")
    GROK_2 = ModelRef("grok", "grok-2", "Grok 2")
    GROK_2_MINI = ModelRef("grok", "grok-2-mini", "Grok 2 Mini")
