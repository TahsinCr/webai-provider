from __future__ import annotations

from ..contracts import ModelRef
from .base import StaticModelCatalog


class GrokModels(StaticModelCatalog):
    provider = "grok"

    AUTO = ModelRef("grok", "auto", "Grok Auto", metadata={"modelMode": "MODEL_MODE_AUTO"})
    GROK_3 = ModelRef(
        "grok",
        "fast",
        "Grok 3",
        metadata={"modelName": "grok-3", "modelMode": "MODEL_MODE_FAST"},
    )
    GROK_4_7_EXPERT = ModelRef(
        "grok", "expert", "Grok 4.7 Expert", metadata={"modelMode": "MODEL_MODE_EXPERT"}
    )
    GROK_4_7_HEAVY = ModelRef(
        "grok", "heavy", "Grok 4.7 Heavy", metadata={"modelMode": "MODEL_MODE_HEAVY"}
    )
