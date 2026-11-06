from __future__ import annotations

import math
from typing import Sequence

from ..contracts import EffortRef, ModelRef
from ..errors import AmbiguousMatchError, RemoteNotFound
from .policies import Efforts, Models


class SelectionResolver:
    @staticmethod
    def resolve_model(
        target: Models | ModelRef | str,
        candidates: Sequence[ModelRef],
        *,
        default_model_id: str | None = None,
    ) -> ModelRef:
        if not candidates:
            raise RemoteNotFound("Kullanılabilir model bulunamadı")

        if isinstance(target, ModelRef):
            match = next((m for m in candidates if m.id == target.id), None)
            if match is not None:
                return match
            return target

        if isinstance(target, Models) or target in ("auto", "low", "medium", "high"):
            policy = target.value if isinstance(target, Models) else target
            n = len(candidates)
            if policy == "low":
                return candidates[0]
            if policy == "high":
                return candidates[n - 1]
            if policy == "medium":
                return candidates[math.floor(n / 2)]
            if policy == "auto":
                if default_model_id is not None:
                    matched = next((m for m in candidates if m.id == default_model_id), None)
                    if matched is not None:
                        return matched
                return candidates[math.floor(n / 2)]

        exact_matches = [m for m in candidates if m.id == target or m.name == target]
        if len(exact_matches) == 1:
            return exact_matches[0]
        if len(exact_matches) > 1:
            names = [m.id for m in exact_matches]
            raise AmbiguousMatchError(f"Birden fazla model eşleşti: {', '.join(names)}")

        target_lower = str(target).lower()
        partial_matches = [
            m for m in candidates if target_lower in m.id.lower() or target_lower in m.name.lower()
        ]
        if len(partial_matches) == 1:
            return partial_matches[0]
        if len(partial_matches) > 1:
            names = [m.id for m in partial_matches]
            raise AmbiguousMatchError(
                f"Model adı belirsiz, birden çok aday bulundu: {', '.join(names)}"
            )

        raise RemoteNotFound(f"Model bulunamadı: {target}")

    @staticmethod
    def resolve_effort(
        target: Efforts | EffortRef | str | int | None,
        candidates: Sequence[EffortRef],
        *,
        default_effort_id: str | None = None,
    ) -> EffortRef | None:
        if not candidates:
            return None

        if target is None:
            if default_effort_id is not None:
                return next((e for e in candidates if e.id == default_effort_id), candidates[0])
            return None

        if isinstance(target, EffortRef):
            match = next((e for e in candidates if e.id == target.id), None)
            if match is not None:
                return match
            return target

        if isinstance(target, Efforts) or target in ("auto", "low", "medium", "high"):
            policy = target.value if isinstance(target, Efforts) else target
            n = len(candidates)
            if policy == "low":
                return candidates[0]
            if policy == "high":
                return candidates[n - 1]
            if policy == "medium":
                return candidates[math.floor(n / 2)]
            if policy == "auto":
                if default_effort_id is not None:
                    matched = next((e for e in candidates if e.id == default_effort_id), None)
                    if matched is not None:
                        return matched
                return candidates[math.floor(n / 2)]

        target_str = str(target).lower()
        exact = [
            e
            for e in candidates
            if e.id.lower() == target_str
            or e.name.lower() == target_str
            or str(e.value) == str(target)
        ]
        if len(exact) == 1:
            return exact[0]
        if len(exact) > 1:
            names = [e.id for e in exact]
            raise AmbiguousMatchError(f"Birden fazla efor eşleşti: {', '.join(names)}")

        raise RemoteNotFound(f"Efor seçimi bulunamadı: {target}")
