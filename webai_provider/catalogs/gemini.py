from __future__ import annotations

from ..contracts import ModelRef
from .base import StaticModelCatalog, effort_refs


class GeminiModels(StaticModelCatalog):
    provider = "gemini"

    FLASH_LITE = ModelRef(
        "gemini",
        "gemini-flash-lite",
        "Gemini 3.5 Flash-Lite",
        efforts=effort_refs("gemini", "gemini-flash-lite", ("standard", "medium", "extended")),
    )
    FLASH = ModelRef(
        "gemini",
        "gemini-flash",
        "Gemini 3.8 Flash",
        efforts=effort_refs("gemini", "gemini-flash", ("standard", "medium", "extended")),
    )
    PRO = ModelRef(
        "gemini",
        "gemini-pro",
        "Gemini 3.1 Pro",
        efforts=effort_refs("gemini", "gemini-pro", ("standard", "medium", "extended")),
    )
    ARGON = ModelRef(
        "gemini",
        "gemini-argon",
        "Gemini 4 Argon",
        efforts=effort_refs("gemini", "gemini-argon", ("standard", "medium", "extended")),
    )
