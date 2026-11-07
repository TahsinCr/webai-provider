from __future__ import annotations

import math
from typing import Sequence

from ..contracts import EffortRef, ModelRef, ModelSelection
from ..errors import AmbiguousMatchError, RemoteNotFound
from .policies import Models


class SelectionResolver:
    @classmethod
    def resolve(
        cls,
        target: Models | ModelRef | ModelSelection | EffortRef | str,
        candidates: Sequence[ModelRef] | Sequence[EffortRef],
        *,
        default_id: str | None = None,
    ) -> ModelRef | ModelSelection | EffortRef:
        if not candidates:
            raise RemoteNotFound("Kullanılabilir seçim bulunamadı")

        if isinstance(target, ModelSelection):
            model = cls.resolve(target.model, candidates, default_id=default_id)
            if not isinstance(model, ModelRef):
                raise RemoteNotFound("Model sağlayıcı kataloğunda bulunamadı")
            effort = (
                cls.resolve(
                    target.effort,
                    model.efforts.list(),
                    default_id=model.default_effort_id,
                )
                if target.effort is not None
                else None
            )
            if effort is not None and not isinstance(effort, EffortRef):
                raise RemoteNotFound("Efor sağlayıcı kataloğunda bulunamadı")
            return ModelSelection(model, effort)

        if isinstance(target, EffortRef) and isinstance(candidates[0], ModelRef):
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
                raise RemoteNotFound(f"{target.provider} modeli bu katalogda bulunmuyor")
            effort = cls.resolve(
                target,
                model.efforts.list(),
                default_id=model.default_effort_id,
            )
            if not isinstance(effort, EffortRef):
                raise RemoteNotFound("Efor sağlayıcı kataloğunda bulunamadı")
            return ModelSelection(model, effort)

        if isinstance(target, Models):
            if target is Models.AUTO:
                selected = next((item for item in candidates if item.id == default_id), None)
                if selected is None:
                    selected = next(
                        (
                            item
                            for item in candidates
                            if getattr(item, "metadata", {}).get("default")
                        ),
                        None,
                    )
                if selected is None:
                    raise RemoteNotFound("Sağlayıcı varsayılan modeli kataloğunda bulunamadı")
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

        if isinstance(target, ModelRef):
            if not isinstance(candidates[0], ModelRef) or target.provider != candidates[0].provider:
                raise RemoteNotFound(f"{target.provider} modeli bu katalogda bulunmuyor")
        elif isinstance(target, EffortRef):
            if not isinstance(candidates[0], EffortRef):
                raise RemoteNotFound("Efor seçimi model kataloğunda kullanılamaz")
            if (
                target.provider != candidates[0].provider
                or target.model_id != candidates[0].model_id
            ):
                raise RemoteNotFound(f"{target.provider} eforu bu katalogda bulunmuyor")
        elif not isinstance(target, str):
            raise TypeError(f"Desteklenmeyen seçim türü: {type(target).__name__}")

        target_id = target.id if isinstance(target, (ModelRef, EffortRef)) else None
        exact_matches = [
            item
            for item in candidates
            if (target_id is not None and item.id == target_id)
            or (isinstance(target, str) and item.name.casefold() == target.casefold())
        ]
        if len(exact_matches) == 1:
            return exact_matches[0]
        if len(exact_matches) > 1:
            names = [item.id for item in exact_matches]
            raise AmbiguousMatchError(f"Birden fazla seçim eşleşti: {', '.join(names)}")

        if target_id is not None:
            raise RemoteNotFound(f"Seçim bulunamadı: {target_id}")
        if not isinstance(target, str):
            raise RemoteNotFound("Seçim bulunamadı")

        target_lower = target.casefold()
        partial_matches = [
            item
            for item in candidates
            if target_lower in item.id.casefold() or target_lower in item.name.casefold()
        ]
        if len(partial_matches) == 1:
            return partial_matches[0]
        if len(partial_matches) > 1:
            preferred = next((item for item in candidates if item.id == default_id), None)
            if preferred is not None and (
                target_lower in preferred.id.casefold() or target_lower in preferred.name.casefold()
            ):
                return preferred
            names = [item.id for item in partial_matches]
            raise AmbiguousMatchError(
                f"Seçim belirsiz, birden çok aday bulundu: {', '.join(names)}"
            )

        raise RemoteNotFound(f"Seçim bulunamadı: {target}")
