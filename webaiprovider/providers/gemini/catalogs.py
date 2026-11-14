from __future__ import annotations

import re
from typing import ClassVar

from ...errors import AccountPaused, ProviderError
from ...resources.common import Choice
from ...resources.model.collections import (
    StaticModelCatalog,
    effort_ref,
)
from ...resources.model.model import (
    EffortRef,
    EffortSet,
    Model,
    ModelRef,
)
from .errors import GeminiCompatibilityError, GeminiError
from .rpc import nested, rpc_parts

THINKING_VALUES = {
    "THINKING_LEVEL_STANDARD": 1,
    "THINKING_LEVEL_EXTENDED": 2,
    "THINKING_LEVEL_DEEP_THINK": 3,
    "THINKING_LEVEL_MEDIUM": 4,
}
_LEADING_VERSION_RE = re.compile(r"^\d+(?:\.\d+)?\s+")


class GeminiFlashLiteEfforts(EffortSet):
    STANDARD: ClassVar[EffortRef] = effort_ref("gemini", "gemini-flash-lite", "standard")
    MEDIUM: ClassVar[EffortRef] = effort_ref("gemini", "gemini-flash-lite", "medium")
    EXTENDED: ClassVar[EffortRef] = effort_ref("gemini", "gemini-flash-lite", "extended")


class GeminiFlashEfforts(EffortSet):
    STANDARD: ClassVar[EffortRef] = effort_ref("gemini", "gemini-flash", "standard")
    MEDIUM: ClassVar[EffortRef] = effort_ref("gemini", "gemini-flash", "medium")
    EXTENDED: ClassVar[EffortRef] = effort_ref("gemini", "gemini-flash", "extended")


class GeminiHydrogenEfforts(EffortSet):
    STANDARD: ClassVar[EffortRef] = effort_ref("gemini", "gemini-hydrogen", "standard")
    MEDIUM: ClassVar[EffortRef] = effort_ref("gemini", "gemini-hydrogen", "medium")
    EXTENDED: ClassVar[EffortRef] = effort_ref("gemini", "gemini-hydrogen", "extended")


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
    HYDROGEN: ClassVar[ModelRef[GeminiHydrogenEfforts]] = ModelRef(
        "gemini", "gemini-hydrogen", "Gemini 4 Hydrogen", efforts=GeminiHydrogenEfforts
    )
    PRO: ClassVar[ModelRef[GeminiProEfforts]] = ModelRef(
        "gemini", "gemini-pro", "Gemini 3.1 Pro", efforts=GeminiProEfforts
    )
    ARGON: ClassVar[ModelRef[GeminiArgonEfforts]] = ModelRef(
        "gemini", "gemini-argon", "Gemini 4 Argon", efforts=GeminiArgonEfforts
    )


def parse_model_catalog(status, rpc_response) -> tuple[Model, ...]:
    if not isinstance(status, dict):
        raise ProviderError("Gemini model policy could not be retrieved")
    parts = list(rpc_parts(rpc_response, "otAQ7b"))
    if len(parts) != 1 or nested(parts[0], 14) == 1016:
        raise AccountPaused("Gemini account could not be verified; guest mode was not used")
    body = parts[0]
    rows = nested(body, 15)
    tiers, flags = nested(body, 16, default=[]), nested(body, 17, default=[])
    native = {item["modeCategory"]: item["id"] for item in status.get("bardModeConfigs") or []}
    policy = status.get("geminiModePolicy")
    if not isinstance(policy, dict) or not isinstance(policy.get("configs"), list):
        raise GeminiCompatibilityError("Gemini reasoning options contract has changed")
    policies = {}
    for item in policy["configs"]:
        config = item.get("modeConfig") or {}
        hosts = item.get("hosts") or []
        if (
            hosts
            and "MODE_PICKER_HOST_CHAT" not in hosts
            or config.get("isDisabled")
            or config.get("upsellConfig")
        ):
            continue
        remote = (
            config.get("modeVariantId")
            or config.get("id")
            or native.get(config.get("modeCategory"))
        )
        if remote:
            policies[remote] = item

    if not isinstance(rows, list):
        raise GeminiCompatibilityError("Gemini model catalog contract has changed")
    capacity, slot = (
        (1, 13)
        if 21 in tiers
        else (2, 13)
        if 22 in tiers
        else (4, 12)
        if 115 in flags
        else (3, 12)
        if 16 in tiers or 106 in flags
        else (2, 12)
        if 8 in tiers or 19 in flags
        else (1, 12)
    )
    models = []
    for row in rows:
        remote = nested(row, 0)
        if not isinstance(remote, str) or remote not in policies:
            continue
        item = policies[remote]
        config = item["modeConfig"]
        efforts, values = [], {}
        for option in item.get("thinkingLevelConfigs") or []:
            if option.get("isDisabled"):
                continue
            level = option.get("thinkingLevel")
            if level == "THINKING_LEVEL_UNSPECIFIED":
                continue
            if level not in THINKING_VALUES:
                raise GeminiCompatibilityError("Gemini returned an unknown thinking protocol.")
            identifier = level.removeprefix("THINKING_LEVEL_").lower()
            if identifier not in values:
                efforts.append(Choice(identifier, identifier))
                values[identifier] = THINKING_VALUES[level]
        implicit = item.get("implicitThinkingLevel") or ""
        default = implicit.removeprefix("THINKING_LEVEL_").lower()
        if default not in values:
            default = None
        category = nested(row, 1) or nested(row, 10) or remote
        title = config.get("titleForPicker") or config.get("title") or category
        number = nested(row, 17)
        if not isinstance(number, int):
            number = nested(row, 9, default=1)
        mode_category = str(config.get("modeCategory") or "").strip().upper()
        category_name = str(category).strip().lower()
        variant_name = f"{category_name} {str(title).strip().lower()}".replace("-", " ").replace(
            "_", " "
        )
        is_fast = (
            mode_category in {"FAST", "MODE_CATEGORY_FAST"} and "lite" not in variant_name.split()
        )
        category_slug = _LEADING_VERSION_RE.sub("", str(category).strip()).lower().replace(
            " ", "-"
        ) or str(category).lower().replace(" ", "-")
        models.append(
            Model(
                "gemini-" + category_slug,
                str(title),
                tuple(efforts),
                default_effort=default,
                metadata={
                    "model_id": remote,
                    "selector_id": None if is_fast else remote,
                    "capacity": capacity,
                    "capacity_slot": slot,
                    "number": None if is_fast else number,
                    "effort_values": values,
                    "description": nested(row, 12) or nested(row, 2),
                },
            )
        )
    if not models:
        raise GeminiError("Gemini returned no available models")
    return tuple(models)
