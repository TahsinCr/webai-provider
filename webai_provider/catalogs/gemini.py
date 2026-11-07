from __future__ import annotations

from ..contracts import ModelRef
from .base import StaticModelCatalog


class GeminiModels(StaticModelCatalog):
    provider = "gemini"

    FLASH_LITE = ModelRef(
        "gemini",
        "gemini-flash-lite",
        "Gemini 3.5 Flash-Lite",
        capabilities=("standard", "medium", "extended"),
    )
    FLASH = ModelRef(
        "gemini",
        "gemini-flash",
        "Gemini 3.8 Flash",
        capabilities=("standard", "medium", "extended"),
    )
    PRO = ModelRef(
        "gemini", "gemini-pro", "Gemini 3.1 Pro", capabilities=("standard", "medium", "extended")
    )
    ARGON = ModelRef(
        "gemini", "gemini-argon", "Gemini 4 Argon", capabilities=("standard", "medium", "extended")
    )
