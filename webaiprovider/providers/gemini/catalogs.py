from __future__ import annotations

import re
from typing import ClassVar

from ...errors import AccountPaused, ProviderError
from ...resources.model.model import (
    EffortOption,
    EffortRef,
    EffortSet,
    Model,
    ModelRef,
)
from ...resources.model.types import StaticModelCatalog, effort_ref
from .errors import GeminiCompatibilityError, GeminiError
from .rpc import rpc_parts, rpc_value

THINKING_VALUES = {
    "THINKING_LEVEL_STANDARD": 1,
    "THINKING_LEVEL_EXTENDED": 2,
    "THINKING_LEVEL_DEEP_THINK": 3,
    "THINKING_LEVEL_MEDIUM": 4,
}
_EFFORT_ORDER = {
    "standard": 0,
    "medium": 1,
    "extended": 2,
    "deep_think": 3,
}
_LEADING_VERSION_RE = re.compile(r"^\d+(?:\.\d+)?\s+")
_REMOTE_MODEL_SLUGS = {
    "8c46e95b1a07cecc": "auto",
    "e6fa609c3fa255c0": "neon",
    "95a4c0abcba24364": "carbon",
}
_LOCALIZED_MODEL_SLUGS = {
    "otomatik": "auto",
    "hidrojen": "hydrogen",
    "karbon": "carbon",
}


def _canonical_model_slug(remote: str, category: str, variant_tokens: set[str]) -> str:
    if remote in _REMOTE_MODEL_SLUGS:
        return _REMOTE_MODEL_SLUGS[remote]
    if remote == "56fdd199312815e2" and "flash" not in variant_tokens:
        return "hydrogen"
    raw_slug = _LEADING_VERSION_RE.sub("", str(category).strip()).lower().replace(" ", "-") or str(
        category
    ).lower().replace(" ", "-")
    return _LOCALIZED_MODEL_SLUGS.get(raw_slug, raw_slug)


class GeminiHydrogenEfforts(EffortSet):
    STANDARD: ClassVar[EffortRef] = effort_ref("gemini", "gemini-hydrogen", "standard")
    MEDIUM: ClassVar[EffortRef] = effort_ref("gemini", "gemini-hydrogen", "medium")
    EXTENDED: ClassVar[EffortRef] = effort_ref("gemini", "gemini-hydrogen", "extended")


class GeminiNeonEfforts(EffortSet):
    STANDARD: ClassVar[EffortRef] = effort_ref("gemini", "gemini-neon", "standard")
    MEDIUM: ClassVar[EffortRef] = effort_ref("gemini", "gemini-neon", "medium")
    EXTENDED: ClassVar[EffortRef] = effort_ref("gemini", "gemini-neon", "extended")


class GeminiCarbonEfforts(EffortSet):
    STANDARD: ClassVar[EffortRef] = effort_ref("gemini", "gemini-carbon", "standard")
    MEDIUM: ClassVar[EffortRef] = effort_ref("gemini", "gemini-carbon", "medium")
    EXTENDED: ClassVar[EffortRef] = effort_ref("gemini", "gemini-carbon", "extended")


class GeminiModels(StaticModelCatalog):
    provider = "gemini"

    HYDROGEN: ClassVar[ModelRef[GeminiHydrogenEfforts]] = ModelRef(
        "gemini", "gemini-hydrogen", "Gemini 4 Hydrogen", efforts=GeminiHydrogenEfforts
    )
    NEON: ClassVar[ModelRef[GeminiNeonEfforts]] = ModelRef(
        "gemini", "gemini-neon", "Gemini 4 Neon", efforts=GeminiNeonEfforts
    )
    CARBON: ClassVar[ModelRef[GeminiCarbonEfforts]] = ModelRef(
        "gemini", "gemini-carbon", "Gemini 4 Carbon", efforts=GeminiCarbonEfforts
    )


def parse_model_catalog(status, rpc_response) -> tuple[Model, ...]:
    if not isinstance(status, dict):
        raise ProviderError("Gemini model policy could not be retrieved")
    parts = list(rpc_parts(rpc_response, "otAQ7b"))
    if len(parts) != 1 or rpc_value(parts[0], 14) == 1016:
        raise AccountPaused("Gemini account could not be verified; guest mode was not used")
    body = parts[0]
    rows = rpc_value(body, 15)
    tiers, flags = rpc_value(body, 16, default=[]), rpc_value(body, 17, default=[])
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
        remote = rpc_value(row, 0)
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
                efforts.append(EffortOption(identifier, identifier))
                values[identifier] = THINKING_VALUES[level]
        efforts.sort(key=lambda option: _EFFORT_ORDER.get(option.id, 99))
        implicit = item.get("implicitThinkingLevel") or ""
        default = implicit.removeprefix("THINKING_LEVEL_").lower()
        if default not in values:
            default = None
        category = rpc_value(row, 1) or rpc_value(row, 10) or remote
        title = config.get("titleForPicker") or config.get("title") or category
        number = rpc_value(row, 17)
        if not isinstance(number, int):
            number = rpc_value(row, 9, default=1)
        mode_category = str(config.get("modeCategory") or "").strip().upper()
        category_name = str(category).strip().lower()
        variant_name = f"{category_name} {str(title).strip().lower()}".replace("-", " ").replace(
            "_", " "
        )
        variant_tokens = set(variant_name.split())
        is_fast = mode_category in {"FAST", "MODE_CATEGORY_FAST"} and "lite" not in variant_tokens
        category_slug = _canonical_model_slug(remote, str(category), variant_tokens)
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
                    "description": rpc_value(row, 12) or rpc_value(row, 2),
                },
            )
        )
    if not models:
        raise GeminiError("Gemini returned no available models")
    return tuple(models)
