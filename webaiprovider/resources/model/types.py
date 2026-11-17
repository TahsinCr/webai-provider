from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from ...collections import BaseCollection, validate_limit, validate_match_flags
from ...errors import ProviderError, RemoteNotFound

if TYPE_CHECKING:
    from .model import EffortRef, ModelCatalog, ModelRef, ModelSelection


class ModelCatalogState(str, Enum):
    AVAILABLE = "available"
    UNKNOWN = "unknown"
    UNAVAILABLE = "unavailable"


class Models(str, Enum):
    AUTO = "auto"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass(frozen=True)
class EffortOption:
    id: str
    title: str


def effort_ref(
    provider: str,
    model_id: str,
    effort_id: str,
    name: str | None = None,
) -> EffortRef:
    from .model import EffortRef

    return EffortRef(
        provider,
        model_id,
        effort_id,
        name or effort_id.replace(":", " ").title(),
    )


def _model_refs_from_catalog(provider: str, catalog: ModelCatalog) -> list[ModelRef]:
    from .model import EffortRef, EffortSet, ModelCatalog, ModelRef

    if not isinstance(catalog, ModelCatalog):
        raise ProviderError("Provider returned an invalid model catalog")
    default_model = catalog.default_chat_model
    result: list[ModelRef] = []
    for rank, model in enumerate(catalog.chat_models):
        efforts = tuple(
            EffortRef(
                provider=provider,
                model_id=model.id,
                id=choice.id,
                name=(
                    {"standard": "Medium", "extended": "High"}.get(choice.id.casefold())
                    if provider.casefold() == "chatgpt"
                    else None
                )
                or choice.title
                or choice.id,
                rank=index,
            )
            for index, choice in enumerate(model.efforts)
        )
        effort_ids = {effort.id for effort in efforts}
        metadata = dict(model.metadata)
        if default_model is not None and default_model.id == model.id:
            metadata["default"] = True
        result.append(
            ModelRef(
                provider=provider,
                id=model.id,
                name=model.title or model.id,
                efforts=EffortSet.from_refs(
                    efforts,
                    name=f"{model.id}_Efforts",
                    default_id=(
                        model.default_effort if model.default_effort in effort_ids else None
                    ),
                ),
                rank=rank,
                metadata=metadata,
            )
        )
    return result


class StaticModelCatalog:
    provider: str = ""

    @classmethod
    def list(cls, *, limit: int | None = None) -> list[ModelRef]:
        from .model import ModelRef

        validated = validate_limit(limit)
        items = [v for v in cls.__dict__.values() if isinstance(v, ModelRef)]
        if validated is None:
            return items
        return items[:validated]

    @classmethod
    def get(cls, model_id: str) -> ModelRef:
        for m in cls.list():
            if m.id == model_id:
                return m
        raise RemoteNotFound(f"Model ID {model_id!r} {cls.provider} was not found in the catalog")

    @classmethod
    def find(
        cls,
        query: str,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> ModelRef:
        from .model import ModelRef
        from .resolver import SelectionResolver

        validate_match_flags(exact=exact, startswith=startswith, endswith=endswith)
        if not (exact or startswith or endswith):
            result = SelectionResolver.resolve(query, cls.list())
            if not isinstance(result, ModelRef):
                raise TypeError("Model query did not resolve to a model")
            return result
        return BaseCollection._find_in(
            cls.list(), query, exact=exact, startswith=startswith, endswith=endswith
        )

    @classmethod
    def first(
        cls,
        query: str,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> ModelRef:
        return BaseCollection._first_in(
            cls.list(), query, exact=exact, startswith=startswith, endswith=endswith
        )

    @classmethod
    def last(
        cls,
        query: str,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> ModelRef:
        return BaseCollection._last_in(
            cls.list(), query, exact=exact, startswith=startswith, endswith=endswith
        )

    @classmethod
    def search(
        cls,
        query: str,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> list[ModelRef]:
        return BaseCollection._search_in(
            cls.list(), query, exact=exact, startswith=startswith, endswith=endswith
        )

    @classmethod
    def contains(cls, item: str | EffortRef | ModelRef | ModelSelection) -> bool:
        from .model import EffortRef, ModelRef, ModelSelection

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
