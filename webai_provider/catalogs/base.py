from __future__ import annotations

from ..contracts import EffortRef, ModelRef, ModelSelection
from ..errors import RemoteNotFound


def effort_ref(
    provider: str,
    model_id: str,
    effort_id: str,
    name: str | None = None,
) -> EffortRef:
    return EffortRef(
        provider,
        model_id,
        effort_id,
        name or effort_id.replace(":", " ").title(),
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
    def contains(cls, item: str | EffortRef | ModelRef | ModelSelection) -> bool:
        target = (
            item.model.id
            if isinstance(item, ModelSelection)
            else item.model_id
            if isinstance(item, EffortRef)
            else item.id
            if isinstance(item, ModelRef)
            else item
        )
        return any(m.id == target for m in cls.list())

    @classmethod
    def __class_getitem__(cls, key: str) -> ModelRef:
        return cls.get(key)
