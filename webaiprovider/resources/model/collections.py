from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable, Sequence
from enum import Enum
from typing import TYPE_CHECKING

from ...collections import AsyncQueryableCollection, QueryableCollection, _CollectionFacade
from ...errors import AmbiguousMatchError, ProviderError, RemoteNotFound
from .model import EffortRef, EffortSet, ModelCatalog, ModelRef, ModelSelection

if TYPE_CHECKING:
    from ...provider import Provider


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


def _model_refs_from_catalog(provider: str, catalog: ModelCatalog) -> list[ModelRef]:
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
    def list(cls) -> list[ModelRef]:
        return [v for v in cls.__dict__.values() if isinstance(v, ModelRef)]

    @classmethod
    def get(cls, model_id: str) -> ModelRef:
        for m in cls.list():
            if m.id == model_id:
                return m
        raise RemoteNotFound(f"Model ID {model_id!r} {cls.provider} was not found in the catalog")

    @classmethod
    def find(cls, query: str) -> ModelRef:
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


class Models(str, Enum):
    AUTO = "auto"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


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


class AsyncModelsCollection(AsyncQueryableCollection[ModelRef]):
    def __init__(self, loader: Callable[[bool], Awaitable[list[ModelRef]]]) -> None:
        self._loader = loader
        self._items_tuple: tuple[ModelRef, ...] | None = None
        self._lock = asyncio.Lock()

    async def list(self, *, refresh: bool = False) -> list[ModelRef]:
        """Load once to prewarm the account cache; refresh forces a provider fetch."""
        async with self._lock:
            if self._items_tuple is None or refresh:
                loaded = await self._loader(refresh)
                self._items_tuple = tuple(loaded)
            return list(self._items_tuple)

    async def refresh(self) -> list[ModelRef]:
        """Refresh the account catalog and bypass provider-side cache entries."""
        return await self.list(refresh=True)

    async def get(self, model_id: str, /) -> ModelRef:
        models = await self.list()
        for model in models:
            if model.id == model_id:
                return model
        raise RemoteNotFound(f"Model ID {model_id!r} is not available for this account")

    async def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> ModelRef:
        models = await self.list()
        if not (startswith or endswith or exact):
            result = SelectionResolver.resolve(query, models)
            if not isinstance(result, ModelRef):
                raise TypeError("Model query did not resolve to a model")
            return result
        return QueryableCollection._find_in(
            models,
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    async def resolve(
        self,
        target: Models | EffortRef | ModelRef | ModelSelection | str,
    ) -> ModelSelection:
        result = SelectionResolver.resolve(target, await self.list())
        if isinstance(result, ModelRef):
            return ModelSelection(result)
        if isinstance(result, ModelSelection):
            return result
        raise TypeError("Selection did not resolve to a model")


class ModelsCollection(_CollectionFacade[ModelRef, ModelRef]):
    def __init__(self, owner: Provider) -> None:
        super().__init__(owner, owner._provider.models)

    def list(self, *, refresh: bool = False) -> list[ModelRef]:
        self._items = self._owner._call(self._collection.list(refresh=refresh))
        return list(self._items)

    def refresh(self) -> list[ModelRef]:
        self._items = self._owner._call(self._collection.refresh())
        return list(self._items)

    def resolve(
        self, target: Models | EffortRef | ModelRef | ModelSelection | str
    ) -> ModelSelection:
        return self._owner._call(self._collection.resolve(target))
