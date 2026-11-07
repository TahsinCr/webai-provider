from __future__ import annotations

from ..contracts import ModelRef
from .base import StaticModelCatalog


class GeminiModels(StaticModelCatalog):
    provider = "gemini"

    AUTO = ModelRef("gemini", "auto", "Auto")
    GEMINI_2_0_FLASH = ModelRef("gemini", "gemini-2.0-flash", "Gemini 2.0 Flash")
    GEMINI_FLASH = ModelRef("gemini", "gemini-flash", "Gemini Flash")
    GEMINI_2_0_FLASH_THINKING = ModelRef(
        "gemini",
        "gemini-2.0-flash-thinking",
        "Gemini 2.0 Flash Thinking",
        capabilities=("low", "medium", "high"),
    )
    GEMINI_THINKING = ModelRef(
        "gemini",
        "gemini-thinking",
        "Gemini Thinking",
        capabilities=("low", "medium", "high"),
    )
    GEMINI_ADVANCED = ModelRef("gemini", "gemini-advanced", "Gemini Advanced")
    GEMINI_PRO = ModelRef("gemini", "gemini-pro", "Gemini Pro")
    GEMINI_FLASH_LITE = ModelRef("gemini", "gemini-flash-lite", "Gemini Flash-Lite")
