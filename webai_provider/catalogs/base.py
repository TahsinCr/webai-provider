from __future__ import annotations

from typing import TYPE_CHECKING

from ..contracts import ModelRef
from ..errors import RemoteNotFound

if TYPE_CHECKING:
    pass


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

        return SelectionResolver.resolve_model(query, cls.list())

    @classmethod
    def search(cls, query: str) -> list[ModelRef]:
        q = query.lower()
        return [m for m in cls.list() if q in m.id.lower() or q in m.name.lower()]

    @classmethod
    def contains(cls, item: str | ModelRef) -> bool:
        target = item.id if isinstance(item, ModelRef) else item
        return any(m.id == target for m in cls.list())

    @classmethod
    def __class_getitem__(cls, key: str) -> ModelRef:
        return cls.get(key)
