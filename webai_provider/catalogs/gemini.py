from __future__ import annotations

from ..contracts import ModelRef
from .base import StaticModelCatalog


class GeminiModels(StaticModelCatalog):
    provider = "gemini"

    AUTO = ModelRef("gemini", "auto", "Auto")
    GEMINI_2_5_PRO = ModelRef(
        "gemini", "gemini-2.5-pro", "Gemini 2.5 Pro", capabilities=("low", "medium", "high")
    )
    GEMINI_2_5_FLASH = ModelRef("gemini", "gemini-2.5-flash", "Gemini 2.5 Flash")
    GEMINI_2_0_FLASH = ModelRef("gemini", "gemini-2.0-flash", "Gemini 2.0 Flash")
    GEMINI_2_0_FLASH_THINKING = ModelRef(
        "gemini",
        "gemini-2.0-flash-thinking",
        "Gemini 2.0 Flash Thinking",
        capabilities=("low", "medium", "high"),
    )
    GEMINI_2_0_PRO_EXP = ModelRef("gemini", "gemini-2.0-pro-exp", "Gemini 2.0 Pro Experimental")
    GEMINI_1_5_PRO = ModelRef("gemini", "gemini-1.5-pro", "Gemini 1.5 Pro")
    GEMINI_1_5_FLASH = ModelRef("gemini", "gemini-1.5-flash", "Gemini 1.5 Flash")
