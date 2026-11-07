from __future__ import annotations

from typing import ClassVar

from ..contracts import EffortSet, ModelRef
from .base import StaticModelCatalog


class GrokModels(StaticModelCatalog):
    provider = "grok"

    AUTO: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "grok", "auto", "Grok Auto", efforts=EffortSet, metadata={"modelMode": "MODEL_MODE_AUTO"}
    )
    GROK_3: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "grok",
        "fast",
        "Grok 3",
        efforts=EffortSet,
        metadata={"modelName": "grok-3", "modelMode": "MODEL_MODE_FAST"},
    )
    GROK_4_7_EXPERT: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "grok",
        "expert",
        "Grok 4.7 Expert",
        efforts=EffortSet,
        metadata={"modelMode": "MODEL_MODE_EXPERT"},
    )
    GROK_4_7_HEAVY: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "grok",
        "heavy",
        "Grok 4.7 Heavy",
        efforts=EffortSet,
        metadata={"modelMode": "MODEL_MODE_HEAVY"},
    )
