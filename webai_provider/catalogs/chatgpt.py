from ..contracts import ModelRef
from .base import StaticModelCatalog, effort_refs


class ChatGPTModels(StaticModelCatalog):
    provider = "chatgpt"

    GPT_5_6_LUNA = ModelRef("chatgpt", "gpt-5-6-mini", "GPT-5.6 Luna")
    GPT_5_6_LUNA_THINKING = ModelRef("chatgpt", "gpt-5-6-t-mini", "GPT-5.6 Luna Thinking")
    GPT_5_6_INSTANT = ModelRef("chatgpt", "gpt-5-6-instant", "GPT-5.6 Instant")
    GPT_5_6_SOL = ModelRef("chatgpt", "gpt-5-6", "GPT-5.6 Sol")
    GPT_5_6_THINKING = ModelRef(
        "chatgpt",
        "gpt-5-6-thinking",
        "GPT-5.6 Sol Thinking",
        efforts=effort_refs(
            "chatgpt",
            "gpt-5-6-thinking",
            (("standard", "Medium"), ("extended", "High")),
        ),
    )
    GPT_6_AUTO = ModelRef("chatgpt", "gpt-6", "GPT-6 Auto")
    GPT_6_INSTANT = ModelRef("chatgpt", "gpt-6-instant", "GPT-6 Instant")
    GPT_6_THINKING = ModelRef(
        "chatgpt",
        "gpt-6-thinking",
        "GPT-6 Thinking",
        efforts=effort_refs(
            "chatgpt", "gpt-6-thinking", (("standard", "Medium"), ("extended", "High"))
        ),
    )
    GPT_6_MINI = ModelRef("chatgpt", "gpt-6-mini", "GPT-6 Mini")
    GPT_6_THINKING_MINI = ModelRef("chatgpt", "gpt-6-t-mini", "GPT-6 Thinking Mini")
