from typing import Any, ClassVar

from ...resources.model.model import (
    EffortOption,
    EffortRef,
    EffortSet,
    Model,
    ModelCatalog,
    ModelCatalogState,
    ModelRef,
)
from ...resources.model.types import StaticModelCatalog
from .errors import ChatGPTError


class GPT56ThinkingEfforts(EffortSet):
    MEDIUM: ClassVar[EffortRef] = EffortRef(
        "chatgpt", "gpt-5-6-thinking", "standard", "Medium", rank=0
    )
    HIGH: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-5-6-thinking", "extended", "High", rank=1)


class GPT6ThinkingEfforts(EffortSet):
    MEDIUM: ClassVar[EffortRef] = EffortRef(
        "chatgpt", "gpt-6-thinking", "standard", "Medium", rank=0
    )
    HIGH: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-6-thinking", "extended", "High", rank=1)


class GPT62ThinkingEfforts(EffortSet):
    MEDIUM: ClassVar[EffortRef] = EffortRef(
        "chatgpt", "gpt-6-2-thinking", "standard", "Medium", rank=0
    )
    HIGH: ClassVar[EffortRef] = EffortRef("chatgpt", "gpt-6-2-thinking", "extended", "High", rank=1)


class ChatGPTModels(StaticModelCatalog):
    provider = "chatgpt"

    GPT_5_6_INSTANT: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "chatgpt", "gpt-5-6-instant", "GPT-5.6 Instant", efforts=EffortSet
    )
    GPT_5_6_THINKING: ClassVar[ModelRef[GPT56ThinkingEfforts]] = ModelRef(
        "chatgpt",
        "gpt-5-6-thinking",
        "GPT-5.6 Thinking",
        efforts=GPT56ThinkingEfforts,
    )
    GPT_5_6_THINKING_MINI: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "chatgpt", "gpt-5-6-t-mini", "GPT-5.6 Thinking Mini", efforts=EffortSet
    )
    GPT_6_INSTANT: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "chatgpt", "gpt-6-instant", "GPT-6 Instant", efforts=EffortSet
    )
    GPT_6_THINKING: ClassVar[ModelRef[GPT6ThinkingEfforts]] = ModelRef(
        "chatgpt", "gpt-6-thinking", "GPT-6 Thinking", efforts=GPT6ThinkingEfforts
    )
    GPT_6_THINKING_MINI: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "chatgpt", "gpt-6-t-mini", "GPT-6 Thinking Mini", efforts=EffortSet
    )
    GPT_6_2_AUTO: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "chatgpt", "gpt-6-2", "GPT-6.2 Auto", efforts=EffortSet
    )
    GPT_6_2_INSTANT: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "chatgpt", "gpt-6-2-instant", "GPT-6.2 Instant", efforts=EffortSet
    )
    GPT_6_2_THINKING: ClassVar[ModelRef[GPT62ThinkingEfforts]] = ModelRef(
        "chatgpt",
        "gpt-6-2-thinking",
        "GPT-6.2 Thinking",
        efforts=GPT62ThinkingEfforts,
    )


def parse_model_catalog(raw: Any) -> ModelCatalog:
    if not isinstance(raw, dict) or not isinstance(raw.get("models"), list):
        raise ChatGPTError("Model catalog response changed: expected models list")
    models: dict[str, Model] = {}
    categories = raw.get("categories")
    if categories is not None and not isinstance(categories, list):
        raise ChatGPTError("Model picker response changed: expected categories list")
    picker: dict[str, dict[str, Any]] = {}
    for category in categories or []:
        if not isinstance(category, dict):
            raise ChatGPTError("Model picker category changed")
        supported = category.get("supported_models", [])
        if not isinstance(supported, list) or any(not isinstance(slug, str) for slug in supported):
            raise ChatGPTError("Model picker supported_models changed")
        for slug in supported + [category.get("default_model")]:
            if isinstance(slug, str) and slug:
                picker.setdefault(slug, category)
    for entry in raw["models"]:
        if not isinstance(entry, dict):
            raise ChatGPTError("Model catalog response contains an invalid model")
        slug = entry.get("slug") or entry.get("id")
        if not isinstance(slug, str) or not slug:
            raise ChatGPTError("Model catalog response contains a model without an ID")
        if (
            any(entry.get(key) is False for key in ("enabled", "is_enabled", "is_available"))
            or entry.get("disabled") is True
        ):
            continue
        efforts: dict[str, EffortOption] = {}
        values = entry.get("thinking_efforts", [])
        if not isinstance(values, list):
            raise ChatGPTError(f"Effort catalog response changed for {slug}")
        default_effort = entry.get("default_thinking_effort") or entry.get("default_effort")
        for value in values:
            item = {"thinking_effort": value} if isinstance(value, str) else value
            if not isinstance(item, dict):
                raise ChatGPTError(f"Invalid effort choice for {slug}")
            effort = item.get("thinking_effort") or item.get("id") or item.get("effort")
            if not isinstance(effort, str) or not effort:
                raise ChatGPTError(f"Effort choice has no ID for {slug}")
            title = (
                item.get("full_label")
                or item.get("title")
                or item.get("short_label")
                or item.get("name")
            )
            efforts[effort] = EffortOption(
                effort, title if isinstance(title, str) and title else effort
            )
            if item.get("is_default") is True or item.get("default") is True:
                default_effort = effort
        visible = not slug.casefold().endswith("-wm")
        visible = (
            visible
            and entry.get("is_hidden") is not True
            and entry.get("show_in_model_picker") is not False
        )
        if categories is not None:
            visible = visible and (
                slug in picker or (not categories and slug == raw.get("default_model_slug"))
            )
        category = picker.get(slug, {})
        version = category.get("model_version")
        lane = category.get("model_lane")
        label = {
            "auto": "Auto",
            "instant": "Instant",
            "thinking": "Thinking",
            "thinking_mini": "Thinking mini",
        }.get(lane, category.get("human_category_name"))
        chat_title = (
            f"GPT-{version} {label}"
            if isinstance(version, str) and isinstance(label, str)
            else None
        )
        models[slug] = Model(
            slug,
            str(entry.get("title") or slug),
            tuple(efforts.values()),
            default_effort if default_effort in efforts else None,
            dict(entry),
            visible,
            chat_title,
        )
    default = raw.get("default_model_slug") or raw.get("default_model")
    if not isinstance(default, str) or default not in models:
        default = next(
            (m.id for m in models.values() if m.metadata.get("is_default") is True), None
        )
    return ModelCatalog(ModelCatalogState.AVAILABLE, tuple(models.values()), default)
