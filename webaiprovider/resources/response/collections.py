from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

from ...collections import (
    AsyncDerivedCollection,
    BaseCollection,
    _RemoteCollectionFacade,
    validate_limit,
    validate_predicate,
)
from .artifact import Artifact
from .response import Response
from .source import Source

if TYPE_CHECKING:
    from ...provider import Provider
    from ..chat.chat import AsyncChat
    from ..message.message import AsyncMessage


def _build_sources(raw_sources: Iterable[Any]) -> tuple[Source, ...]:
    sources: list[Source] = []
    seen: set[str] = set()
    for index, entry in enumerate(raw_sources):
        if isinstance(entry, Source):
            key = entry.id or entry.url
            if key not in seen:
                seen.add(key)
                sources.append(entry)
        elif isinstance(entry, dict):
            url = str(entry.get("url") or entry.get("uri") or entry.get("href") or "")
            sid = str(entry.get("id") or entry.get("source_id") or url or f"source-{index}")
            if not url and not entry.get("title"):
                continue
            key = f"{sid}:{url}"
            if key in seen:
                continue
            seen.add(key)
            sources.append(
                Source(
                    id=sid,
                    url=url,
                    title=str(entry.get("title") or entry.get("name") or url),
                    snippet=str(entry.get("snippet") or entry.get("description") or ""),
                    kind=str(entry.get("kind") or "web"),
                    metadata=dict(entry),
                )
            )
    return tuple(sources)


def _build_artifacts(item: Any) -> tuple[Artifact, ...]:
    artifacts: list[Artifact] = []
    seen: set[str] = set()
    raw_files = item.metadata.get("files") if isinstance(item.metadata, dict) else ()
    for index, entry in enumerate(raw_files or ()):
        if isinstance(entry, Artifact):
            key = entry.id or entry.download_url or entry.name
            if key not in seen:
                seen.add(key)
                artifacts.append(entry)
        elif isinstance(entry, dict):
            url = entry.get("remote_url") or entry.get("download_url") or entry.get("url")
            name = str(entry.get("name") or entry.get("filename") or "file")
            aid = str(entry.get("id") or url or f"artifact-{index}")
            key = f"{aid}:{url or name}"
            if key in seen:
                continue
            seen.add(key)
            size = entry.get("size_bytes", entry.get("size"))
            artifacts.append(
                Artifact(
                    id=aid,
                    kind=str(entry.get("kind") or "file"),
                    name=name,
                    mime_type=str(
                        entry.get("media_type")
                        or entry.get("mime_type")
                        or "application/octet-stream"
                    ),
                    size=size if isinstance(size, int) and not isinstance(size, bool) else None,
                    download_url=str(url) if url else None,
                    metadata=dict(entry),
                )
            )
    for part in getattr(item, "content", ()):
        resource = getattr(part, "resource", None)
        if resource is not None and item.role == "assistant":
            key = f"{resource.id}:{resource.remote_url or resource.name}"
            if key in seen:
                continue
            seen.add(key)
            artifacts.append(
                Artifact(
                    id=resource.id,
                    kind="resource",
                    name=resource.name,
                    mime_type=resource.media_type or "application/octet-stream",
                    download_url=resource.remote_url,
                    metadata=dict(resource.metadata),
                )
            )
    return tuple(artifacts)


class AsyncResponsesCollection(AsyncDerivedCollection[Response]):
    """Derived Response query view over chat.messages without duplicating remote history."""

    def __init__(self, chat: AsyncChat) -> None:
        self.chat = chat

    @staticmethod
    def _extract_id(item: Any) -> str:
        if isinstance(item, Response):
            return item.message.remote_id or item.message.id
        return BaseCollection._extract_id(item)

    @staticmethod
    def _extract_name(item: Any) -> str:
        if isinstance(item, Response):
            return item.text
        return BaseCollection._extract_name(item)

    @classmethod
    def _item_matches_exact_id(cls, item: Any, item_id: str) -> bool:
        if isinstance(item, Response):
            return item.message.id == item_id or item.message.remote_id == item_id
        return BaseCollection._item_matches_exact_id(item, item_id)

    def _responses_from_messages(self, messages: Iterable[AsyncMessage]) -> list[Response]:
        responses: list[Response] = []
        for message in messages:
            if message.role == "assistant":
                if message.response is None:
                    message.response = Response(
                        message,
                        metadata={"from_history": True, "message_id": message.id},
                        timing=None,
                    )
                responses.append(message.response)
        return responses

    async def list(
        self,
        predicate: Callable[[Response], bool] | None = None,
        *,
        limit: int | None = 50,
        **_options: Any,
    ) -> list[Response]:
        validate_predicate(predicate)
        validated = validate_limit(limit)
        if validated == 0:
            return []
        messages = await self.chat.messages._load_history(emit_event=False)
        responses = self._responses_from_messages(messages)
        if predicate is not None:
            responses = [item for item in responses if predicate(item)]
        if validated is None:
            return responses
        return responses[-validated:]

    async def refresh(self, *, limit: int | None = None, **_options: Any) -> list[Response]:
        validated = validate_limit(limit)
        if validated == 0:
            return []
        messages = await self.chat.messages._load_history(emit_event=True)
        responses = self._responses_from_messages(messages)
        if validated is None:
            return responses
        return responses[-validated:]


class ResponsesCollection(_RemoteCollectionFacade[Response, Response]):
    def __init__(self, owner: Provider, chat: AsyncChat) -> None:
        super().__init__(owner, chat.responses)

    def _wrap(self, item: Response) -> Response:
        from ..message.message import AsyncMessage, Message

        msg = item.message
        if isinstance(msg, AsyncMessage):
            msg = Message._from_async(self._owner, msg)
        return Response(
            msg,
            sources=item.sources,
            artifacts=item.artifacts,
            metadata=item.metadata,
            timing=item.timing,
        )
