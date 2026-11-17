from __future__ import annotations

from collections.abc import AsyncIterator, Iterable
from copy import copy
from typing import TYPE_CHECKING, Any

from ...collections import (
    AsyncManageableCollection,
    BaseCollection,
    ManageableCollection,
    _CollectionFacade,
    validate_limit,
)
from ...core.hooks import OperationHooks, SyncOperationHooks
from ...errors import ProviderError, UnsupportedOperationError
from ..chat.chat import AsyncChat, _project_from_data
from ..chat.types import Conversation
from ..model.collections import Models
from ..model.model import (
    EffortRef,
    ModelRef,
    ModelSelection,
)
from ..response.collections import _build_artifacts, _build_sources
from ..response.response import Response
from .attachment import Attachment, Attachments
from .message import AsyncMessage, Message
from .operation import AsyncMessageOperation, MessageOperation, _close_async_iterator

if TYPE_CHECKING:
    from ...provider import Provider
    from .operation import StreamEvent


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
        rebuilt_known: dict[str, AsyncMessage] = {}
        for msg in messages:
            if msg.id:
                rebuilt_known[msg.id] = msg
            if msg.remote_id:
                rebuilt_known[msg.remote_id] = msg
        self._known_by_id = rebuilt_known
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
        """Delete an unsent local message; deleting persisted remote messages is unsupported."""
        if isinstance(target, Message):
            target = target._resource
        if isinstance(target, AsyncMessage):
            if target.remote_id:
                raise UnsupportedOperationError(
                    "Deleting persisted remote messages is not supported"
                )
            target_id = target.id
        else:
            target_id = str(target)
            existing = self._known_by_id.get(target_id) or next(
                (m for m in self._items if m.id == target_id or m.remote_id == target_id),
                None,
            )
            if existing is not None and existing.remote_id:
                raise UnsupportedOperationError(
                    "Deleting persisted remote messages is not supported"
                )
        self._items = [m for m in self._items if m.id != target_id]
        self._known_by_id.pop(target_id, None)


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
