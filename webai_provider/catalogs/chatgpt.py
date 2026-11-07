from typing import ClassVar

from ..contracts import EffortRef, EffortSet, ModelRef
from .base import StaticModelCatalog


class GPT56ThinkingEfforts(EffortSet):
    MEDIUM: ClassVar[EffortRef] = EffortRef(
        "chatgpt", "gpt-5-6-thinking", "standard", "Medium", rank=0
    )
    HIGH: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-5-6-thinking", "extended", "High", rank=1)


class GPT6ThinkingEfforts(EffortSet):
    MEDIUM: ClassVar[EffortRef] = EffortRef(
        "chatgpt", "gpt-6-thinking", "standard", "Medium", rank=0
    )
    HIGH: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-6-thinking", "extended", "High", rank=1)


class GPT6AstraEfforts(EffortSet):
    LOW: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-6-astra", "low", "Low", rank=0)
    MEDIUM: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-6-astra", "medium", "Medium", rank=1)
    HIGH: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-6-astra", "high", "High", rank=2)
    XHIGH: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-6-astra", "xhigh", "Extra High", rank=3)
    MAX: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-6-astra", "max", "Max", rank=4)


class ChatGPTModels(StaticModelCatalog):
    provider = "chatgpt"

    GPT_5_6_INSTANT: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "chatgpt", "gpt-5-6-instant", "GPT-5.6 Instant", efforts=EffortSet
    )
    GPT_5_6_THINKING: ClassVar[ModelRef[GPT56ThinkingEfforts]] = ModelRef(
        "chatgpt",
        "gpt-5-6-thinking",
        "GPT-5.6 Thinking",
        efforts=GPT56ThinkingEfforts,
    )
    GPT_6_AUTO: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "chatgpt", "gpt-6", "GPT-6 Auto", efforts=EffortSet
    )
    GPT_6_INSTANT: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "chatgpt", "gpt-6-instant", "GPT-6 Instant", efforts=EffortSet
    )
    GPT_6_THINKING: ClassVar[ModelRef[GPT6ThinkingEfforts]] = ModelRef(
        "chatgpt", "gpt-6-thinking", "GPT-6 Thinking", efforts=GPT6ThinkingEfforts
    )
    GPT_6_ASTRA: ClassVar[ModelRef[GPT6AstraEfforts]] = ModelRef(
        "chatgpt",
        "gpt-6-astra",
        "GPT-6 Astra",
        efforts=GPT6AstraEfforts,
    )
    GPT_6_MINI: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "chatgpt", "gpt-6-mini", "GPT-6 Mini", efforts=EffortSet
    )
    GPT_6_THINKING_MINI: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "chatgpt", "gpt-6-t-mini", "GPT-6 Thinking Mini", efforts=EffortSet
    )
