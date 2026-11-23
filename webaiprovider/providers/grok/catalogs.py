from __future__ import annotations

from typing import ClassVar

from ...errors import WebAIProviderError
from ...resources.model.model import (
    EffortSet,
    Model,
    ModelCatalog,
    ModelCatalogState,
    ModelRef,
)
from ...resources.model.types import StaticModelCatalog

_MODEL_MODES = {
    "auto": "MODEL_MODE_AUTO",
    "fast": "MODEL_MODE_FAST",
    "expert": "MODEL_MODE_EXPERT",
    "heavy": "MODEL_MODE_HEAVY",
}


class GrokModels(StaticModelCatalog):
    provider = "grok"

    AUTO: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "grok", "auto", "Grok Auto", efforts=EffortSet, metadata={"modelMode": "MODEL_MODE_AUTO"}
    )
    GROK_3: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "grok",
        "fast",
        "Grok 3",
        efforts=EffortSet,
        metadata={"modelName": "grok-3", "modelMode": "MODEL_MODE_FAST"},
    )
    GROK_4_7_EXPERT: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "grok",
        "expert",
        "Grok 4.7 Expert",
        efforts=EffortSet,
        metadata={"modelMode": "MODEL_MODE_EXPERT"},
    )
    GROK_4_7_HEAVY: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "grok",
        "heavy",
        "Grok 4.7 Heavy",
        efforts=EffortSet,
        metadata={"modelMode": "MODEL_MODE_HEAVY"},
    )


def parse_model_catalog(data, details) -> ModelCatalog:
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("modes"), list)
        or not isinstance(details, dict)
        or not isinstance(details.get("models"), list)
    ):
        raise WebAIProviderError("Grok model catalog response contract changed")

    by_mode, by_id = {}, {}
    for item in details["models"]:
        if not isinstance(item, dict):
            continue
        identifier, mode = item.get("modelId"), item.get("modelMode")
        if isinstance(identifier, str) and identifier and isinstance(mode, str) and mode:
            by_mode.setdefault(mode, {})[identifier] = item
            by_id[identifier] = item
            by_id.setdefault(identifier.replace(".", ""), item)

    models = []
    for item in data["modes"]:
        if not isinstance(item, dict):
            continue
        model_id = item.get("id")
        if (
            not isinstance(model_id, str)
            or not model_id
            or model_id in ("build", "imagine", "voice")
        ):
            continue
        availability = item.get("availability")
        if (
            not isinstance(availability, dict)
            or not isinstance(availability.get("available"), dict)
            or any(
                availability.get(key) is not None
                for key in ("requiresUpgrade", "unavailable", "comingSoon")
            )
        ):
            continue
        title = item.get("title")
        title = title.strip() if isinstance(title, str) and title.strip() else model_id
        metadata = {
            key: item[key]
            for key in ("description", "badgeText", "iconHint", "tags")
            if item.get(key) is not None
        }
        metadata["modeName"] = title
        description = item.get("description")
        advertised = description.rsplit("·", 1)[-1].strip() if isinstance(description, str) else ""
        advertised = advertised if advertised.casefold().startswith("grok ") else ""
        model_mode = item.get("modelMode")
        if not isinstance(model_mode, str) or not model_mode:
            detail_entry = by_id.get(model_id) or by_id.get(model_id.replace(".", ""), {})
            model_mode = _MODEL_MODES.get(model_id.casefold()) or detail_entry.get("modelMode")
        if isinstance(model_mode, str) and model_mode:
            metadata["modelMode"] = model_mode
        matches = by_mode.get(model_mode, {})
        detail = matches.get(model_id) or matches.get(model_id.replace(".", ""))
        if detail is None and len(matches) == 1:
            detail = next(iter(matches.values()))
        if detail is not None:
            metadata["modelName"] = detail["modelId"]
            name = detail.get("name")
            title = (
                name.strip()
                if isinstance(name, str) and name.strip()
                else advertised or detail["modelId"].replace("-", " ").replace("_", " ").title()
            )
        elif advertised:
            title = advertised
        chat_title = (
            title
            if model_id.casefold().startswith("grok-") or title.casefold().startswith("grok")
            else f"Grok · {title}"
        )
        if metadata.get("modeName") and metadata["modeName"] != title:
            chat_title += " · " + metadata["modeName"]
        models.append(Model(model_id, title, metadata=metadata, chat_title=chat_title))

    default = data.get("defaultModeId")
    if not any(model.id == default for model in models):
        default = models[0].id if models else None
    return ModelCatalog(
        ModelCatalogState.AVAILABLE if models else ModelCatalogState.UNAVAILABLE,
        tuple(models),
        default,
    )
