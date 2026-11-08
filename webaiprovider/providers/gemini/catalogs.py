from __future__ import annotations

from typing import ClassVar

from ...catalogs import StaticModelCatalog, effort_ref
from ...contracts import EffortRef, EffortSet, ModelRef


class GeminiFlashLiteEfforts(EffortSet):
    STANDARD: ClassVar[EffortRef] = effort_ref("gemini", "gemini-flash-lite", "standard")
    MEDIUM: ClassVar[EffortRef] = effort_ref("gemini", "gemini-flash-lite", "medium")
    EXTENDED: ClassVar[EffortRef] = effort_ref("gemini", "gemini-flash-lite", "extended")


class GeminiFlashEfforts(EffortSet):
    STANDARD: ClassVar[EffortRef] = effort_ref("gemini", "gemini-flash", "standard")
    MEDIUM: ClassVar[EffortRef] = effort_ref("gemini", "gemini-flash", "medium")
    EXTENDED: ClassVar[EffortRef] = effort_ref("gemini", "gemini-flash", "extended")


class GeminiProEfforts(EffortSet):
    STANDARD: ClassVar[EffortRef] = effort_ref("gemini", "gemini-pro", "standard")
    MEDIUM: ClassVar[EffortRef] = effort_ref("gemini", "gemini-pro", "medium")
    EXTENDED: ClassVar[EffortRef] = effort_ref("gemini", "gemini-pro", "extended")


class GeminiArgonEfforts(EffortSet):
    STANDARD: ClassVar[EffortRef] = effort_ref("gemini", "gemini-argon", "standard")
    MEDIUM: ClassVar[EffortRef] = effort_ref("gemini", "gemini-argon", "medium")
    EXTENDED: ClassVar[EffortRef] = effort_ref("gemini", "gemini-argon", "extended")


class GeminiModels(StaticModelCatalog):
    provider = "gemini"

    FLASH_LITE: ClassVar[ModelRef[GeminiFlashLiteEfforts]] = ModelRef(
        "gemini",
        "gemini-flash-lite",
        "Gemini 3.5 Flash-Lite",
        efforts=GeminiFlashLiteEfforts,
    )
    FLASH: ClassVar[ModelRef[GeminiFlashEfforts]] = ModelRef(
        "gemini", "gemini-flash", "Gemini 3.8 Flash", efforts=GeminiFlashEfforts
    )
    PRO: ClassVar[ModelRef[GeminiProEfforts]] = ModelRef(
        "gemini", "gemini-pro", "Gemini 3.1 Pro", efforts=GeminiProEfforts
    )
    ARGON: ClassVar[ModelRef[GeminiArgonEfforts]] = ModelRef(
        "gemini", "gemini-argon", "Gemini 4 Argon", efforts=GeminiArgonEfforts
    )
