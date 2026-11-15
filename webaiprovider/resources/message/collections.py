from __future__ import annotations

from collections.abc import AsyncIterator, Iterable
from copy import copy
from typing import TYPE_CHECKING, Any

from ...collections import (
    AsyncManageableCollection,
    ManageableCollection,
    QueryableCollection,
    _CollectionFacade,
)
from ...errors import ProviderError
from ..attachment import Attachment
from ..chat.chat import AsyncChat, _project_from_data
from ..chat.types import Conversation
from ..message.attachments import Attachments
from ..message.message import AsyncMessage, Message
from ..message.operation import AsyncMessageOperation, MessageOperation, _close_async_iterator
from ..model.collections import Models
from ..model.model import (
    EffortRef,
    ModelRef,
    ModelSelection,
)
from .operation import StreamEvent

if TYPE_CHECKING:
    from ...provider import Provider


class AsyncMessagesCollection(AsyncManageableCollection[AsyncMessage]):
    def __init__(self, chat: AsyncChat) -> None:
        super().__init__()
        self.chat = chat

    async def refresh(self) -> list[AsyncMessage]:
        if not self.chat.remote_id:
            return list(self._items)
        session = self.chat.session
        if session is None:
            raise ProviderError("Chat is not attached to an open provider")
        conversation = await session._require_provider().get_chat(self.chat.remote_id)
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
        messages = [
            AsyncMessage(
                content="\n".join(
                    part.text for part in item.content if part.kind == "text" and part.text
                ),
                role=item.role,
                id=item.id,
                metadata={**item.metadata, "actions": item.actions},
                chat=self.chat,
            )
            for item in conversation.messages
            if item.channel != "analysis"
        ]
        self._items = list(messages)
        return list(self._items)

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

        self._items.append(msg)
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

    def update(
        self,
        message: AsyncMessage | Message | str,
        *,
        content: str | None = None,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> AsyncMessageOperation:
        target = (
            message._resource
            if isinstance(message, Message)
            else message
            if isinstance(message, AsyncMessage)
            else QueryableCollection._get_from(self._items, message)
        )
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
        target = QueryableCollection._get_from(self._items, message_id)
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
        self._items.append(msg)
        return AsyncMessageOperation(self.chat, msg, executor=self._execute_send)

    def retry(
        self,
        message_id: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> AsyncMessageOperation:
        target = QueryableCollection._get_from(self._items, message_id)
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
        self._items.append(msg)
        return AsyncMessageOperation(self.chat, msg, executor=self._execute_send)

    async def delete(self, target: AsyncMessage | Message | str) -> None:
        if isinstance(target, Message):
            target = target._resource
        target_id = target.id if isinstance(target, AsyncMessage) else str(target)
        self._items = [m for m in self._items if m.id != target_id]


class MessagesCollection(_CollectionFacade[AsyncMessage, Message], ManageableCollection[Message]):
    def __init__(self, owner: Provider, chat: AsyncChat) -> None:
        super().__init__(owner, chat.messages)

    def _wrap(self, item: AsyncMessage) -> Message:
        return Message._from_async(self._owner, item)

    def refresh(self) -> list[Message]:
        return [self._wrap(item) for item in self._owner._call(self._collection.refresh())]

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
