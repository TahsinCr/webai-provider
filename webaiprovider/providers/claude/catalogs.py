from __future__ import annotations

from typing import ClassVar

from ...catalogs import StaticModelCatalog, effort_ref
from ...contracts import EffortRef, EffortSet, ModelRef
from ...models import Choice, FeatureState, Model, ModelCatalog
from .errors import ClaudeCompatibilityError


def _effort(model_id: str, effort_id: str, name: str | None = None) -> EffortRef:
    return effort_ref("claude", model_id, effort_id, name)


class ClaudeOpus5Efforts(EffortSet):
    LOW: ClassVar[EffortRef] = _effort("claude-opus-5", "low")
    MEDIUM: ClassVar[EffortRef] = _effort("claude-opus-5", "medium")
    HIGH: ClassVar[EffortRef] = _effort("claude-opus-5", "high")
    XHIGH: ClassVar[EffortRef] = _effort("claude-opus-5", "xhigh")
    MAX: ClassVar[EffortRef] = _effort("claude-opus-5", "max")


class ClaudeOpus55Efforts(EffortSet):
    LOW: ClassVar[EffortRef] = _effort("claude-opus-5-5", "low")
    MEDIUM: ClassVar[EffortRef] = _effort("claude-opus-5-5", "medium")
    HIGH: ClassVar[EffortRef] = _effort("claude-opus-5-5", "high")
    XHIGH: ClassVar[EffortRef] = _effort("claude-opus-5-5", "xhigh")
    MAX: ClassVar[EffortRef] = _effort("claude-opus-5-5", "max")


class ClaudeFable51Efforts(EffortSet):
    DEFAULT_EFFORT_ID: ClassVar[str | None] = "medium"
    LOW: ClassVar[EffortRef] = _effort("claude-fable-5-1", "low")
    MEDIUM: ClassVar[EffortRef] = _effort("claude-fable-5-1", "medium")
    HIGH: ClassVar[EffortRef] = _effort("claude-fable-5-1", "high")
    XHIGH: ClassVar[EffortRef] = _effort("claude-fable-5-1", "xhigh")
    MAX: ClassVar[EffortRef] = _effort("claude-fable-5-1", "max")


class ClaudeFable5Efforts(EffortSet):
    DEFAULT_EFFORT_ID: ClassVar[str | None] = "medium"
    LOW: ClassVar[EffortRef] = _effort("claude-fable-5", "low")
    MEDIUM: ClassVar[EffortRef] = _effort("claude-fable-5", "medium")
    HIGH: ClassVar[EffortRef] = _effort("claude-fable-5", "high")
    XHIGH: ClassVar[EffortRef] = _effort("claude-fable-5", "xhigh")
    MAX: ClassVar[EffortRef] = _effort("claude-fable-5", "max")


class ClaudeSonnet55Efforts(EffortSet):
    DEFAULT_EFFORT_ID: ClassVar[str | None] = "medium"
    LOW: ClassVar[EffortRef] = _effort("claude-sonnet-5-5", "low")
    MEDIUM: ClassVar[EffortRef] = _effort("claude-sonnet-5-5", "medium")
    HIGH: ClassVar[EffortRef] = _effort("claude-sonnet-5-5", "high")
    XHIGH: ClassVar[EffortRef] = _effort("claude-sonnet-5-5", "xhigh")
    MAX: ClassVar[EffortRef] = _effort("claude-sonnet-5-5", "max")


class ClaudeHaiku55Efforts(EffortSet):
    DEFAULT_EFFORT_ID: ClassVar[str | None] = "medium"
    LOW: ClassVar[EffortRef] = _effort("claude-haiku-5-5", "low")
    MEDIUM: ClassVar[EffortRef] = _effort("claude-haiku-5-5", "medium")
    HIGH: ClassVar[EffortRef] = _effort("claude-haiku-5-5", "high")
    XHIGH: ClassVar[EffortRef] = _effort("claude-haiku-5-5", "xhigh")
    MAX: ClassVar[EffortRef] = _effort("claude-haiku-5-5", "max")


class ClaudeSonnet5Efforts(EffortSet):
    DEFAULT_EFFORT_ID: ClassVar[str | None] = "medium:auto"
    LOW_AUTO: ClassVar[EffortRef] = _effort("claude-sonnet-5", "low:auto")
    LOW_OFF: ClassVar[EffortRef] = _effort("claude-sonnet-5", "low:off")
    MEDIUM_AUTO: ClassVar[EffortRef] = _effort("claude-sonnet-5", "medium:auto")
    MEDIUM_OFF: ClassVar[EffortRef] = _effort("claude-sonnet-5", "medium:off")
    HIGH_AUTO: ClassVar[EffortRef] = _effort("claude-sonnet-5", "high:auto")
    HIGH_OFF: ClassVar[EffortRef] = _effort("claude-sonnet-5", "high:off")
    XHIGH_AUTO: ClassVar[EffortRef] = _effort("claude-sonnet-5", "xhigh:auto", "Extra High Auto")
    XHIGH_OFF: ClassVar[EffortRef] = _effort("claude-sonnet-5", "xhigh:off", "Extra High Off")
    MAX_AUTO: ClassVar[EffortRef] = _effort("claude-sonnet-5", "max:auto")
    MAX_OFF: ClassVar[EffortRef] = _effort("claude-sonnet-5", "max:off")


class ClaudeSonnet46Efforts(EffortSet):
    DEFAULT_EFFORT_ID: ClassVar[str | None] = "medium:auto"
    LOW_AUTO: ClassVar[EffortRef] = _effort("claude-sonnet-4-6", "low:auto")
    LOW_OFF: ClassVar[EffortRef] = _effort("claude-sonnet-4-6", "low:off")
    MEDIUM_AUTO: ClassVar[EffortRef] = _effort("claude-sonnet-4-6", "medium:auto")
    MEDIUM_OFF: ClassVar[EffortRef] = _effort("claude-sonnet-4-6", "medium:off")
    HIGH_AUTO: ClassVar[EffortRef] = _effort("claude-sonnet-4-6", "high:auto")
    HIGH_OFF: ClassVar[EffortRef] = _effort("claude-sonnet-4-6", "high:off")
    MAX_AUTO: ClassVar[EffortRef] = _effort("claude-sonnet-4-6", "max:auto")
    MAX_OFF: ClassVar[EffortRef] = _effort("claude-sonnet-4-6", "max:off")


class ClaudeModels(StaticModelCatalog):
    provider = "claude"

    OPUS_4_5: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "claude", "claude-opus-4-5-20251101", "Claude Opus 4.5", efforts=EffortSet
    )
    OPUS_4_6: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "claude", "claude-opus-4-6", "Claude Opus 4.6", efforts=EffortSet
    )
    OPUS_4_7: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "claude", "claude-opus-4-7", "Claude Opus 4.7", efforts=EffortSet
    )
    OPUS_4_8: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "claude", "claude-opus-4-8", "Claude Opus 4.8", efforts=EffortSet
    )
    OPUS_5: ClassVar[ModelRef[ClaudeOpus5Efforts]] = ModelRef(
        "claude", "claude-opus-5", "Claude Opus 5", efforts=ClaudeOpus5Efforts
    )
    OPUS_5_5: ClassVar[ModelRef[ClaudeOpus55Efforts]] = ModelRef(
        "claude", "claude-opus-5-5", "Claude Opus 5.5", efforts=ClaudeOpus55Efforts
    )
    FABLE_5_1: ClassVar[ModelRef[ClaudeFable51Efforts]] = ModelRef(
        "claude",
        "claude-fable-5-1",
        "Claude Fable 5.1",
        efforts=ClaudeFable51Efforts,
    )
    FABLE_5: ClassVar[ModelRef[ClaudeFable5Efforts]] = ModelRef(
        "claude",
        "claude-fable-5",
        "Claude Fable 5",
        efforts=ClaudeFable5Efforts,
    )
    SONNET_5_5: ClassVar[ModelRef[ClaudeSonnet55Efforts]] = ModelRef(
        "claude",
        "claude-sonnet-5-5",
        "Claude Sonnet 5.5",
        efforts=ClaudeSonnet55Efforts,
    )
    HAIKU_5_5: ClassVar[ModelRef[ClaudeHaiku55Efforts]] = ModelRef(
        "claude",
        "claude-haiku-5-5",
        "Claude Haiku 5.5",
        efforts=ClaudeHaiku55Efforts,
    )
    HAIKU_4_5: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "claude", "claude-haiku-4-5-20251001", "Claude Haiku 4.5", efforts=EffortSet
    )
    SONNET_4_5: ClassVar[ModelRef[EffortSet]] = ModelRef(
        "claude", "claude-sonnet-4-5-20250929", "Claude Sonnet 4.5", efforts=EffortSet
    )
    SONNET_5: ClassVar[ModelRef[ClaudeSonnet5Efforts]] = ModelRef(
        "claude",
        "claude-sonnet-5",
        "Claude Sonnet 5",
        efforts=ClaudeSonnet5Efforts,
    )
    SONNET_4_6: ClassVar[ModelRef[ClaudeSonnet46Efforts]] = ModelRef(
        "claude",
        "claude-sonnet-4-6",
        "Claude Sonnet 4.6",
        efforts=ClaudeSonnet46Efforts,
    )


def parse_model_catalog(data) -> ModelCatalog:
    config = next((x for x in data.get("model_selector_config", []) if x.get("id") == "chat"), None)
    state = next((x for x in data.get("model_selector_state", []) if x.get("id") == "chat"), {})
    if not config or not isinstance(config.get("models"), list):
        raise ClaudeCompatibilityError("Claude account model-selection contract changed")
    states = {x["id"]: x.get("thinking", {}) for x in state.get("thinking_by_model", [])}
    if state.get("model"):
        states[state["model"]] = state.get("thinking", {})
    models = []
    for entry in config["models"]:
        if (
            not isinstance(entry.get("id"), str)
            or entry.get("disabled")
            or entry.get("disabled_reason")
            or entry.get("inactive")
            or entry.get("is_available") is False
        ):
            continue
        thinking = entry.get("thinking") or {}
        efforts = [x for x in thinking.get("effort_options", []) if not x.get("disabled")]
        modes = [x for x in thinking.get("mode_options", []) if not x.get("disabled")]
        selections, choices = {}, []
        selected = states.get(entry["id"], {})
        effort_ids = {x["id"] for x in efforts}
        mode_ids = {x["id"] for x in modes}
        default_effort = selected.get("effort")
        if default_effort not in effort_ids:
            default_effort = next(
                (x["id"] for x in efforts if x.get("recommended")),
                efforts[0]["id"] if efforts else None,
            )
        default_mode = selected.get("mode")
        if default_mode not in mode_ids:
            default_mode = modes[0]["id"] if modes else None
        for effort in efforts or [None]:
            for mode in modes or [None]:
                if effort is None and mode is None:
                    continue
                eid, mid = effort["id"] if effort else None, mode["id"] if mode else None
                identity = eid if eid and len(modes) <= 1 else f"{eid}:{mid}" if eid else mid
                title = effort.get("name", eid) if effort else mode.get("name", mid)
                if effort and len(modes) > 1:
                    title += " · " + mode.get("name", mid)
                payload = {}
                if eid:
                    payload["effort"] = eid
                if mid:
                    payload["thinking_mode"] = mid
                selections[identity] = payload
                choices.append(
                    {
                        "id": identity,
                        "title": title,
                        "selected": (eid == default_effort or not effort)
                        and (mid == default_mode or not mode),
                    }
                )
        models.append(
            Model(
                entry["id"],
                entry.get("name") or entry["id"],
                tuple(Choice(choice["id"], choice["title"]) for choice in choices),
                default_effort=next(
                    (choice["id"] for choice in choices if choice["selected"]), None
                ),
                metadata={"native": entry, "selections": selections},
            )
        )
    if not models:
        raise ClaudeCompatibilityError("Claude returned no selectable chat model for this account")
    preferred = state.get("model")
    default = preferred if any(x.id == preferred for x in models) else models[0].id
    return ModelCatalog(FeatureState.AVAILABLE, tuple(models), default)
