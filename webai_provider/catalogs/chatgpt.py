from typing import ClassVar

from ..contracts import EffortRef, ModelRef, ModelSelection
from .base import StaticModelCatalog


class GPT56ThinkingEfforts:
    MEDIUM: ClassVar[EffortRef] = EffortRef(
        "chatgpt", "gpt-5-6-thinking", "standard", "Medium", rank=0
    )
    HIGH: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-5-6-thinking", "extended", "High", rank=1)


class GPT6ThinkingEfforts:
    MEDIUM: ClassVar[EffortRef] = EffortRef(
        "chatgpt", "gpt-6-thinking", "standard", "Medium", rank=0
    )
    HIGH: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-6-thinking", "extended", "High", rank=1)


class GPT6AstraEfforts:
    LOW: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-6-astra", "low", "Low", rank=0)
    MEDIUM: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-6-astra", "medium", "Medium", rank=1)
    HIGH: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-6-astra", "high", "High", rank=2)
    XHIGH: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-6-astra", "xhigh", "Extra High", rank=3)
    MAX: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-6-astra", "max", "Max", rank=4)


class ChatGPTThinkingModelRef(ModelRef):
    Efforts: ClassVar[type[GPT56ThinkingEfforts] | type[GPT6ThinkingEfforts]]

    @property
    def MEDIUM(self) -> ModelSelection:
        return self.select("Medium")

    @property
    def HIGH(self) -> ModelSelection:
        return self.select("High")


class GPT56ThinkingModelRef(ChatGPTThinkingModelRef):
    Efforts: ClassVar[type[GPT56ThinkingEfforts]] = GPT56ThinkingEfforts


class GPT6ThinkingModelRef(ChatGPTThinkingModelRef):
    Efforts: ClassVar[type[GPT6ThinkingEfforts]] = GPT6ThinkingEfforts


class GPT6AstraModelRef(ModelRef):
    Efforts: ClassVar[type[GPT6AstraEfforts]] = GPT6AstraEfforts

    @property
    def LOW(self) -> ModelSelection:
        return self.select(GPT6AstraEfforts.LOW)

    @property
    def MEDIUM(self) -> ModelSelection:
        return self.select(GPT6AstraEfforts.MEDIUM)

    @property
    def HIGH(self) -> ModelSelection:
        return self.select(GPT6AstraEfforts.HIGH)

    @property
    def XHIGH(self) -> ModelSelection:
        return self.select(GPT6AstraEfforts.XHIGH)

    @property
    def MAX(self) -> ModelSelection:
        return self.select(GPT6AstraEfforts.MAX)


class ChatGPTModels(StaticModelCatalog):
    provider = "chatgpt"

    GPT_5_6_LUNA = ModelRef("chatgpt", "gpt-5-6-mini", "GPT-5.6 Luna")
    GPT_5_6_LUNA_THINKING = ModelRef("chatgpt", "gpt-5-6-t-mini", "GPT-5.6 Luna Thinking")
    GPT_5_6_INSTANT = ModelRef("chatgpt", "gpt-5-6-instant", "GPT-5.6 Instant")
    GPT_5_6_SOL = ModelRef("chatgpt", "gpt-5-6", "GPT-5.6 Sol")
    GPT_5_6_THINKING: ClassVar[GPT56ThinkingModelRef] = GPT56ThinkingModelRef(
        "chatgpt",
        "gpt-5-6-thinking",
        "GPT-5.6 Sol Thinking",
        efforts=(GPT56ThinkingEfforts.MEDIUM, GPT56ThinkingEfforts.HIGH),
    )
    GPT_6_AUTO = ModelRef("chatgpt", "gpt-6", "GPT-6 Auto")
    GPT_6_INSTANT = ModelRef("chatgpt", "gpt-6-instant", "GPT-6 Instant")
    GPT_6_THINKING: ClassVar[GPT6ThinkingModelRef] = GPT6ThinkingModelRef(
        "chatgpt",
        "gpt-6-thinking",
        "GPT-6 Thinking",
        efforts=(GPT6ThinkingEfforts.MEDIUM, GPT6ThinkingEfforts.HIGH),
    )
    GPT_6_ASTRA: ClassVar[GPT6AstraModelRef] = GPT6AstraModelRef(
        "chatgpt",
        "gpt-6-astra",
        "GPT-6 Astra",
        efforts=(
            GPT6AstraEfforts.LOW,
            GPT6AstraEfforts.MEDIUM,
            GPT6AstraEfforts.HIGH,
            GPT6AstraEfforts.XHIGH,
            GPT6AstraEfforts.MAX,
        ),
    )
    GPT_6_MINI = ModelRef("chatgpt", "gpt-6-mini", "GPT-6 Mini")
    GPT_6_THINKING_MINI = ModelRef("chatgpt", "gpt-6-t-mini", "GPT-6 Thinking Mini")
