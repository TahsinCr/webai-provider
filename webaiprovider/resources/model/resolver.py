from __future__ import annotations

import math
from collections.abc import Sequence

from ...errors import AmbiguousMatchError, RemoteNotFound
from .model import EffortRef, ModelRef, ModelSelection
from .types import Models


class SelectionResolver:
    """Resolve Models tier enums, refs, selections, or string queries against candidates."""

    @classmethod
    def resolve(
        cls,
        target: Models | ModelRef | ModelSelection | EffortRef | str,
        candidates: Sequence[ModelRef] | Sequence[EffortRef],
        *,
        default_id: str | None = None,
    ) -> ModelRef | ModelSelection | EffortRef:
        if not candidates:
            raise RemoteNotFound("No available selection was found")
        if isinstance(target, ModelSelection):
            return cls._resolve_selection(target, candidates, default_id=default_id)
        if isinstance(target, EffortRef) and isinstance(candidates[0], ModelRef):
            return cls._resolve_model_for_effort(target, candidates)
        if isinstance(target, Models):
            return cls._resolve_enum_tier(target, candidates, default_id=default_id)
        target_id, query = cls._validate_target(target, candidates)
        return cls._match_candidate(target, target_id, query, candidates, default_id=default_id)

    @classmethod
    def _resolve_selection(
        cls,
        target: ModelSelection,
        candidates: Sequence[ModelRef] | Sequence[EffortRef],
        *,
        default_id: str | None,
    ) -> ModelSelection:
        model = cls.resolve(target.model, candidates, default_id=default_id)
        if not isinstance(model, ModelRef):
            raise RemoteNotFound("Model was not found in the provider catalog")
        effort = (
            cls.resolve(target.effort, model.efforts.list(), default_id=model.default_effort_id)
            if target.effort is not None
            else None
        )
        if effort is not None and not isinstance(effort, EffortRef):
            raise RemoteNotFound("Effort was not found in the provider catalog")
        return ModelSelection(model, effort)

    @classmethod
    def _resolve_model_for_effort(
        cls,
        target: EffortRef,
        candidates: Sequence[ModelRef] | Sequence[EffortRef],
    ) -> ModelSelection:
        model = next(
            (
                item
                for item in candidates
                if isinstance(item, ModelRef)
                and item.provider == target.provider
                and item.id == target.model_id
            ),
            None,
        )
        if model is None:
            raise RemoteNotFound(f"{target.provider} model is not available in this catalog")
        effort = cls.resolve(target, model.efforts.list(), default_id=model.default_effort_id)
        if not isinstance(effort, EffortRef):
            raise RemoteNotFound("Effort was not found in the provider catalog")
        return ModelSelection(model, effort)

    @staticmethod
    def _resolve_enum_tier(
        target: Models,
        candidates: Sequence[ModelRef] | Sequence[EffortRef],
        *,
        default_id: str | None,
    ) -> ModelRef | EffortRef:
        if target is Models.AUTO:
            selected = next((item for item in candidates if item.id == default_id), None)
            if selected is None:
                selected = next(
                    (item for item in candidates if getattr(item, "metadata", {}).get("default")),
                    None,
                )
            if selected is None:
                raise RemoteNotFound("Provider default model was not found in the catalog")
            return selected

        ordered = (
            sorted(candidates, key=lambda item: item.rank if item.rank is not None else 0)
            if all(item.rank is not None for item in candidates)
            else candidates
        )
        index = {
            Models.LOW: 0,
            Models.MEDIUM: math.floor(len(ordered) / 2),
            Models.HIGH: len(ordered) - 1,
        }[target]
        return ordered[index]

    @staticmethod
    def _validate_target(
        target: ModelRef | EffortRef | str,
        candidates: Sequence[ModelRef] | Sequence[EffortRef],
    ) -> tuple[str, str | None]:
        if isinstance(target, ModelRef):
            if not isinstance(candidates[0], ModelRef) or target.provider != candidates[0].provider:
                raise RemoteNotFound(f"{target.provider} model is not available in this catalog")
            return target.id, None
        if isinstance(target, EffortRef):
            if not isinstance(candidates[0], EffortRef):
                raise RemoteNotFound("Effort selection is not available in the model catalog")
            if (
                target.provider != candidates[0].provider
                or target.model_id != candidates[0].model_id
            ):
                raise RemoteNotFound(f"{target.provider} effort is not available in this catalog")
            return target.id, None
        if isinstance(target, str):
            return target, target.casefold()
        raise TypeError(f"Unsupported selection type: {type(target).__name__}")

    @staticmethod
    def _match_candidate(
        target: ModelRef | EffortRef | str,
        target_id: str,
        query: str | None,
        candidates: Sequence[ModelRef] | Sequence[EffortRef],
        *,
        default_id: str | None,
    ) -> ModelRef | EffortRef:
        exact_matches = [item for item in candidates if item.id == target_id]
        if not exact_matches and query is not None:
            exact_matches = [item for item in candidates if item.name.casefold() == query]
        if len(exact_matches) == 1:
            return exact_matches[0]
        if len(exact_matches) > 1:
            names = [item.id for item in exact_matches]
            raise AmbiguousMatchError(f"Multiple selections matched: {', '.join(names)}")
        if query is None:
            raise RemoteNotFound(f"Selection was not found: {target_id}")

        partial_matches = [
            item
            for item in candidates
            if query in item.id.casefold() or query in item.name.casefold()
        ]
        if len(partial_matches) == 1:
            return partial_matches[0]
        if len(partial_matches) > 1:
            preferred = next((item for item in candidates if item.id == default_id), None)
            if preferred is not None and (
                query in preferred.id.casefold() or query in preferred.name.casefold()
            ):
                return preferred
            names = [item.id for item in partial_matches]
            raise AmbiguousMatchError(
                f"Selection is ambiguous; multiple candidates were found: {', '.join(names)}"
            )
        raise RemoteNotFound(f"Selection was not found: {target}")
