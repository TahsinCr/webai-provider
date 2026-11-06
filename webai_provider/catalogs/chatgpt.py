from __future__ import annotations

from ..contracts import ModelRef


class ChatGPTModels:
    GPT_4O = ModelRef("chatgpt", "gpt-4o", "GPT-4o")
    GPT_4O_MINI = ModelRef("chatgpt", "gpt-4o-mini", "GPT-4o mini")
    GPT_4_5 = ModelRef("chatgpt", "gpt-4-5", "GPT-4.5")
    O1 = ModelRef("chatgpt", "o1", "o1")
    O1_MINI = ModelRef("chatgpt", "o1-mini", "o1-mini")
    O3_MINI = ModelRef("chatgpt", "o3-mini", "o3-mini")
    AUTO = ModelRef("chatgpt", "auto", "Auto")
