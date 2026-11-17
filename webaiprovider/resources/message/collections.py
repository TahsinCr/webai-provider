from __future__ import annotations

from collections.abc import AsyncIterator, Iterable
from copy import copy
from typing import TYPE_CHECKING, Any

from ...collections import (
    AsyncManageableCollection,
    AsyncRemoteCollection,
    BaseCollection,
    ManageableCollection,
    _CollectionFacade,
    _RemoteCollectionFacade,
    validate_limit,
)
from ...core.hooks import OperationHooks, SyncOperationHooks
from ...errors import ProviderError, RemoteNotFound
from ..artifact import Artifact
from ..attachment import Attachment
from ..chat.chat import AsyncChat, _project_from_data
from ..chat.types import Conversation
from ..model.collections import Models
from ..model.model import (
    EffortRef,
    ModelRef,
    ModelSelection,
)
from ..source import Source
from .attachments import Attachments
from .message import AsyncMessage, Message
from .operation import AsyncMessageOperation, MessageOperation, _close_async_iterator
from .response import Response

if TYPE_CHECKING:
    from ...provider import Provider
    from .operation import StreamEvent


def _build_sources(raw_sources: Iterable[Any]) -> tuple[Source, ...]:
    sources: list[Source] = []
    for index, raw in enumerate(raw_sources):
        if isinstance(raw, Source):
            sources.append(raw)
        elif isinstance(raw, dict):
            url = str(raw.get("url") or raw.get("uri") or "")
            sid = str(raw.get("id") or raw.get("file_id") or url or f"source-{index}")
            sources.append(
                Source(
                    id=sid,
                    url=url,
                    title=str(raw.get("title") or raw.get("name") or ""),
                    snippet=str(raw.get("description") or raw.get("snippet") or ""),
                    kind=str(raw.get("kind") or ("file" if raw.get("file_id") else "web")),
                    metadata=dict(raw),
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


class AsyncMessagesCollection(AsyncManageableCollection[AsyncMessage]):
    def __init__(self, chat: AsyncChat) -> None:
        super().__init__()
        self.chat = chat
        self.hooks = OperationHooks()
        self._known_by_id: dict[str, AsyncMessage] = {}

    def _track_local_message(self, message: AsyncMessage) -> None:
        self._known_by_id[message.id] = message
        if message.remote_id:
            self._known_by_id[message.remote_id] = message
        if not any(item is message for item in self._items):
            self._items.append(message)

    def _reconcile_assistant_message(self, message: AsyncMessage) -> AsyncMessage:
        existing = None
        if message.id:
            existing = next(
                (
                    item
                    for item in self._items
                    if item.remote_id == message.id or item.id == message.id
                ),
                None,
            )
        if existing is None:
            self._items.append(message)
            self._known_by_id[message.id] = message
            if message.remote_id:
                self._known_by_id[message.remote_id] = message
            return message
        existing.content = message.content
        existing.model = message.model
        existing.remote_id = message.remote_id
        self._known_by_id[existing.id] = existing
        if existing.remote_id:
            self._known_by_id[existing.remote_id] = existing
        return existing

    async def _load_history(self, *, emit_event: bool = False) -> list[AsyncMessage]:
        if not self.chat.remote_id:
            return list(self._items)
        session = self.chat.session
        if session is None:
            raise ProviderError("Chat is not attached to an open provider")
        conversation = await session._get_chat_data(self.chat.remote_id)
        if not isinstance(conversation, Conversation):
            raise ProviderError("Provider returned an invalid conversation")
        if conversation.title and not self.chat.title:
            self.chat.title = conversation.title
        self.chat.metadata = dict(conversation.metadata)
        if conversation.project is not None:
            resolved_project = _project_from_data(session, conversation.project)
            if self.chat.project is None:
                self.chat.project = resolved_project
            elif conversation.project.name and not self.chat.project.title:
                self.chat.project.title = conversation.project.name

        existing = {
            **self._known_by_id,
            **{(item.remote_id or item.id): item for item in self._items},
        }
        messages: list[AsyncMessage] = []
        remote_ids: set[str] = set()
        for item in conversation.messages:
            if item.channel == "analysis":
                continue
            remote_ids.add(item.id)
            message = existing.get(item.id)
            content = "\n".join(
                part.text for part in item.content if part.kind == "text" and part.text
            )
            if message is None:
                message = AsyncMessage(role=item.role, id=item.id, chat=self.chat)
            message.remote_id = item.id
            message.content = content
            message.metadata = {**item.metadata, "actions": item.actions}
            if item.role == "assistant":
                raw_sources = (
                    item.metadata.get("sources") if isinstance(item.metadata, dict) else ()
                )
                sources = _build_sources(raw_sources or ())
                artifacts = _build_artifacts(item)
                message.response = Response(
                    message,
                    sources=sources,
                    artifacts=artifacts,
                    metadata={"from_history": True, "message_id": item.id},
                    timing=None,
                )
            self._known_by_id[message.id] = message
            self._known_by_id[item.id] = message
            messages.append(message)

        messages.extend(
            item
            for item in self._items
            if item.remote_id is None
            and item.id not in remote_ids
            and item.metadata.get("action") not in ("retry", "edit")
        )
        self._items = list(messages)
        if emit_event:
            runtime = getattr(session, "runtime", None)
            if runtime is not None:
                await runtime.emit(
                    "collection.refreshed",
                    resource_id=self.chat.remote_id,
                    payload={"collection": "messages", "count": len(messages)},
                )
        return list(messages)

    async def list(self, *, limit: int | None = 50) -> list[AsyncMessage]:
        validated = validate_limit(limit)
        if validated == 0:
            return []
        messages = await self._load_history(emit_event=False)
        if validated is None:
            return messages
        return messages[-validated:]

    async def refresh(self, *, limit: int | None = None) -> list[AsyncMessage]:
        validated = validate_limit(limit)
        if validated == 0:
            return []
        messages = await self._load_history(emit_event=True)
        if validated is None:
            return messages
        return messages[-validated:]

    async def get(self, item_id: str, /) -> AsyncMessage:
        return BaseCollection._get_from(await self.list(limit=None), item_id)

    async def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> AsyncMessage:
        return BaseCollection._find_in(
            await self.list(limit=None),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    async def first(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> AsyncMessage:
        return BaseCollection._first_in(
            await self.list(limit=None),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    async def last(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> AsyncMessage:
        return BaseCollection._last_in(
            await self.list(limit=None),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    async def search(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> list[AsyncMessage]:
        return BaseCollection._search_in(
            await self.list(limit=None),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    def create(
        self,
        message: AsyncMessage | Message | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Iterable[Attachment] | Attachments | None = None,
        **metadata: Any,
    ) -> AsyncMessageOperation:
        if isinstance(message, str):
            msg = AsyncMessage(
                content=message,
                model=model,
                attachments=attachments,
                metadata=metadata,
                chat=self.chat,
            )
        else:
            msg = message._resource if isinstance(message, Message) else message
            if msg.chat is not None and msg.chat is not self.chat:
                raise ValueError("Message belongs to a different chat")
            if any(item is msg for item in self._items):
                raise ValueError("Message is already in this chat")
            msg.chat = self.chat
            if model is not None:
                msg.model = model
            if attachments is not None:
                for item in attachments:
                    msg.attachments.create(item)

        self._track_local_message(msg)
        return AsyncMessageOperation(self.chat, msg, executor=self._execute_send)

    async def _execute_send(self, operation: AsyncMessageOperation) -> AsyncIterator[StreamEvent]:
        session = self.chat.session
        if session is None:
            raise ProviderError("No provider session is attached to this chat")
        stream = session._require_provider().execute_stream(operation)
        try:
            async for event in stream:
                yield event
        finally:
            await _close_async_iterator(stream)

    def _resolve_known_message(self, message: AsyncMessage | Message | str) -> AsyncMessage:
        if isinstance(message, Message):
            return message._resource
        if isinstance(message, AsyncMessage):
            return message
        found = self._known_by_id.get(message)
        if found is not None:
            return found
        return BaseCollection._get_from(self._items, message)

    def update(
        self,
        message: AsyncMessage | Message | str,
        *,
        content: str | None = None,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> AsyncMessageOperation:
        target = self._resolve_known_message(message)
        new_content = content if content is not None else target.content
        return self.edit(target.id, new_content, model=model, **options)

    def edit(
        self,
        message_id: str,
        new_content: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> AsyncMessageOperation:
        target = self._resolve_known_message(message_id)
        msg = AsyncMessage(
            content=new_content,
            model=model if model is not None else target.model,
            attachments=Attachments(copy(item) for item in target.attachments),
            metadata={
                **target.metadata,
                **options,
                "parent_message_id": target.id,
                "action": "edit",
            },
            chat=self.chat,
        )
        self._track_local_message(msg)
        return AsyncMessageOperation(self.chat, msg, executor=self._execute_send)

    def retry(
        self,
        message_id: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> AsyncMessageOperation:
        target = self._resolve_known_message(message_id)
        msg = AsyncMessage(
            content=target.content,
            model=model if model is not None else target.model,
            attachments=Attachments(copy(item) for item in target.attachments),
            metadata={
                **target.metadata,
                **options,
                "parent_message_id": target.id,
                "action": "retry",
            },
            chat=self.chat,
        )
        self._track_local_message(msg)
        return AsyncMessageOperation(self.chat, msg, executor=self._execute_send)

    async def delete(self, target: AsyncMessage | Message | str) -> None:
        if isinstance(target, Message):
            target = target._resource
        target_id = target.id if isinstance(target, AsyncMessage) else str(target)
        self._items = [m for m in self._items if m.id != target_id]
        self._known_by_id.pop(target_id, None)


class AsyncResponsesCollection(AsyncRemoteCollection[Response]):
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

    async def _fetch_page(self, cursor: str | None, page_limit: int | None, **options: Any):
        raise NotImplementedError("Responses delegate directly to shared chat.messages history")

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

    async def list(self, *, limit: int | None = 50, **_options: Any) -> list[Response]:
        validated = validate_limit(limit)
        if validated == 0:
            return []
        messages = await self.chat.messages._load_history(emit_event=False)
        responses = self._responses_from_messages(messages)
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

    async def get(self, item_id: str, /) -> Response:
        responses = await self.list(limit=None)
        for item in responses:
            if self._item_matches_exact_id(item, item_id):
                return item
        raise RemoteNotFound(f"Record was not found: {item_id}")

    async def search(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        **_options: Any,
    ) -> list[Response]:
        return self._search_in(
            await self.list(limit=None),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    async def find(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        **_options: Any,
    ) -> Response:
        return self._find_in(
            await self.list(limit=None),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    async def first(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        **_options: Any,
    ) -> Response:
        return self._first_in(
            await self.list(limit=None),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )

    async def last(
        self,
        query: str,
        /,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
        **_options: Any,
    ) -> Response:
        return self._last_in(
            await self.list(limit=None),
            query,
            exact=exact,
            startswith=startswith,
            endswith=endswith,
        )


class MessagesCollection(_CollectionFacade[AsyncMessage, Message], ManageableCollection[Message]):
    def __init__(self, owner: Provider, chat: AsyncChat) -> None:
        super().__init__(owner, chat.messages)
        self.hooks = SyncOperationHooks(chat.messages.hooks)

    def _wrap(self, item: AsyncMessage) -> Message:
        return Message._from_async(self._owner, item)

    def list(self, *, limit: int | None = 50) -> list[Message]:
        items = [self._wrap(item) for item in self._owner._call(self._collection.list(limit=limit))]
        self._items = items
        return list(items)

    def refresh(self, *, limit: int | None = None) -> list[Message]:
        items = [
            self._wrap(item) for item in self._owner._call(self._collection.refresh(limit=limit))
        ]
        self._items = items
        return list(items)

    def create(
        self,
        message: Message | AsyncMessage | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Any = None,
        **metadata: Any,
    ) -> MessageOperation:
        operation = self._collection.create(
            message, model=model, attachments=attachments, **metadata
        )
        return MessageOperation(self._owner, operation)

    def update(
        self,
        message: Message | AsyncMessage | str,
        *,
        content: str | None = None,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> MessageOperation:
        return MessageOperation(
            self._owner,
            self._collection.update(message, content=content, model=model, **options),
        )

    def edit(
        self,
        message_id: str,
        new_content: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> MessageOperation:
        return MessageOperation(
            self._owner,
            self._collection.edit(message_id, new_content, model=model, **options),
        )

    def retry(
        self,
        message_id: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> MessageOperation:
        return MessageOperation(
            self._owner,
            self._collection.retry(message_id, model=model, **options),
        )

    def delete(self, message: Message | str) -> None:
        self._owner._call(self._collection.delete(message))


class ResponsesCollection(_RemoteCollectionFacade[Response, Response]):
    def __init__(self, owner: Provider, chat: AsyncChat) -> None:
        super().__init__(owner, chat.responses)

    def _wrap(self, item: Response) -> Response:
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
