from __future__ import annotations

from collections.abc import Iterable

from ..contracts import EffortRef, ModelRef, ModelSelection
from ..errors import RemoteNotFound


def effort_refs(
    provider: str,
    model_id: str,
    choices: Iterable[str | tuple[str, str]],
) -> tuple[EffortRef, ...]:
    return tuple(
        EffortRef(
            provider=provider,
            model_id=model_id,
            id=choice[0] if isinstance(choice, tuple) else choice,
            name=choice[1] if isinstance(choice, tuple) else choice.replace(":", " ").title(),
            rank=rank,
        )
        for rank, choice in enumerate(choices)
    )


class StaticModelCatalog:
    provider: str = ""

    @classmethod
    def list(cls) -> list[ModelRef]:
        return [v for v in cls.__dict__.values() if isinstance(v, ModelRef)]

    @classmethod
    def get(cls, model_id: str) -> ModelRef:
        for m in cls.list():
            if m.id == model_id:
                return m
        raise RemoteNotFound(f"Model ID {model_id!r} {cls.provider} kataloğunda bulunamadı")

    @classmethod
    def find(cls, query: str) -> ModelRef:
        from .resolver import SelectionResolver

        result = SelectionResolver.resolve(query, cls.list())
        if not isinstance(result, ModelRef):
            raise TypeError("Model query did not resolve to a model")
        return result

    @classmethod
    def search(cls, query: str) -> list[ModelRef]:
        q = query.lower()
        return [m for m in cls.list() if q in m.id.lower() or q in m.name.lower()]

    @classmethod
    def contains(cls, item: str | ModelRef | ModelSelection) -> bool:
        target = (
            item.model.id
            if isinstance(item, ModelSelection)
            else item.id
            if isinstance(item, ModelRef)
            else item
        )
        return any(m.id == target for m in cls.list())

    @classmethod
    def __class_getitem__(cls, key: str) -> ModelRef:
        return cls.get(key)
