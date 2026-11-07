from __future__ import annotations

from ..contracts import ModelRef
from .base import StaticModelCatalog


class ChatGPTModels(StaticModelCatalog):
    provider = "chatgpt"

    AUTO = ModelRef("chatgpt", "auto", "Auto")
    GPT_4O = ModelRef("chatgpt", "gpt-4o", "GPT-4o")
    GPT_4O_MINI = ModelRef("chatgpt", "gpt-4o-mini", "GPT-4o mini")
    GPT_4_5 = ModelRef("chatgpt", "gpt-4.5-preview", "GPT-4.5")
    O3_MINI = ModelRef("chatgpt", "o3-mini", "o3-mini", capabilities=("low", "medium", "high"))
    O1 = ModelRef("chatgpt", "o1", "o1", capabilities=("low", "medium", "high"))
    O1_MINI = ModelRef("chatgpt", "o1-mini", "o1-mini")
    O1_PREVIEW = ModelRef("chatgpt", "o1-preview", "o1-preview")
    CHATGPT_4O_LATEST = ModelRef("chatgpt", "chatgpt-4o-latest", "ChatGPT-4o Latest")
    GPT_4_TURBO = ModelRef("chatgpt", "gpt-4-turbo", "GPT-4 Turbo")
    GPT_4 = ModelRef("chatgpt", "gpt-4", "GPT-4")
