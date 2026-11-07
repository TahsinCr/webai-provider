from ..contracts import ModelRef
from .base import StaticModelCatalog


class ChatGPTModels(StaticModelCatalog):
    provider = "chatgpt"

    GPT_5_6_INSTANT = ModelRef("chatgpt", "gpt-5-6-instant", "GPT-5.6 Instant")
    GPT_5_6_THINKING = ModelRef(
        "chatgpt",
        "gpt-5-6-thinking",
        "GPT-5.6 Thinking",
        capabilities=("standard", "extended"),
    )
    GPT_5_6_THINKING_MINI = ModelRef("chatgpt", "gpt-5-6-t-mini", "GPT-5.6 Thinking mini")
    GPT_6_AUTO = ModelRef("chatgpt", "gpt-6", "GPT-6 Auto")
    GPT_6_INSTANT = ModelRef("chatgpt", "gpt-6-instant", "GPT-6 Instant")
    GPT_6_THINKING = ModelRef(
        "chatgpt",
        "gpt-6-thinking",
        "GPT-6 Thinking",
        capabilities=("standard", "extended"),
    )
    GPT_6_THINKING_MINI = ModelRef("chatgpt", "gpt-6-t-mini", "GPT-6 Thinking mini")
