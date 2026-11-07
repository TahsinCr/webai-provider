from __future__ import annotations

from enum import Enum


class ProviderType(str, Enum):
    CHATGPT = "chatgpt"
    CLAUDE = "claude"
    GEMINI = "gemini"
    GROK = "grok"

    @classmethod
    def parse(cls, value: ProviderType | str) -> ProviderType:
        try:
            return value if isinstance(value, cls) else cls(value.casefold())
        except (AttributeError, ValueError):
            supported = ", ".join(item.value for item in cls)
            raise ValueError(f"Unsupported provider type; expected one of: {supported}") from None
