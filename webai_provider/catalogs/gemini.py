from __future__ import annotations

from ..contracts import ModelRef


class GeminiModels:
    GEMINI_2_5_FLASH = ModelRef("gemini", "gemini-2.5-flash", "Gemini 2.5 Flash")
    GEMINI_2_5_PRO = ModelRef("gemini", "gemini-2.5-pro", "Gemini 2.5 Pro")
    GEMINI_2_0_FLASH = ModelRef("gemini", "gemini-2.0-flash", "Gemini 2.0 Flash")
    GEMINI_FLASH_LITE = ModelRef("gemini", "gemini-flash-lite", "Gemini Flash-Lite")
    GEMINI_ARGON = ModelRef("gemini", "gemini-argon", "Gemini 4 Argon")
    AUTO = ModelRef("gemini", "auto", "Auto")
