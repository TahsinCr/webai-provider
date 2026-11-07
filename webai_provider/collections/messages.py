from __future__ import annotations

from typing import TYPE_CHECKING, Any, AsyncIterator

from ..catalogs.policies import Models
from ..contracts import EffortRef, ModelRef, ModelSelection, StreamEvent
from ..resources.message import Message
from ..resources.operation import MessageOperation
from .base import ManageableCollection

if TYPE_CHECKING:
    from ..resources.chat import Chat


class MessagesCollection(ManageableCollection[Message]):
    def __init__(self, chat: Chat) -> None:
        super().__init__()
        self.chat = chat

    def create(
        self,
        message: Message | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Any = None,
        **metadata: Any,
    ) -> MessageOperation:
        if isinstance(message, str):
            msg = Message(
                content=message,
                model=model,
                attachments=attachments,
                metadata=metadata,
                chat=self.chat,
            )
        else:
            msg = message
            msg.chat = self.chat
            if model is not None:
                msg.model = model
            if attachments is not None:
                msg.attachments.extend(attachments)

        self._items.append(msg)
        return MessageOperation(self.chat, msg, executor=self._execute_send)

    async def _execute_send(self, operation: MessageOperation) -> AsyncIterator[StreamEvent]:
        session = self.chat.session
        if session is not None and hasattr(session, "_bridge") and session._bridge:
            async for ev in session._bridge.execute_stream(operation):
                yield ev
        else:
            from ..contracts import OutputBegin, OutputDelta, OutputEnd

            yield OutputBegin()
            yield OutputDelta(f"Yanıt: {operation.message.content}")
            yield OutputEnd(f"Yanıt: {operation.message.content}")

    def update(
        self,
        message: Message | str,
        *,
        content: str | None = None,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> MessageOperation:
        target = message if isinstance(message, Message) else self.get(message)
        new_content = content if content is not None else target.content
        return self.edit(target.id, new_content, model=model, **options)

    def edit(
        self,
        message_id: str,
        new_content: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> MessageOperation:
        target = self.get(message_id)
        msg = Message(
            content=new_content,
            model=model if model is not None else target.model,
            attachments=target.attachments,
            metadata={
                **target.metadata,
                **options,
                "parent_message_id": target.id,
                "action": "edit",
            },
            chat=self.chat,
        )
        self._items.append(msg)
        return MessageOperation(self.chat, msg, executor=self._execute_send)

    def retry(
        self,
        message_id: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> MessageOperation:
        target = self.get(message_id)
        msg = Message(
            content=target.content,
            model=model if model is not None else target.model,
            attachments=target.attachments,
            metadata={
                **target.metadata,
                **options,
                "parent_message_id": target.id,
                "action": "retry",
            },
            chat=self.chat,
        )
        self._items.append(msg)
        return MessageOperation(self.chat, msg, executor=self._execute_send)

    async def delete(self, target: Message | str) -> None:
        target_id = target.id if isinstance(target, Message) else str(target)
        self._items = [m for m in self._items if m.id != target_id]

    async def branch(self, message_id: str, *, title: str | None = None) -> Chat:
        return await self.chat.branch(message_id, title=title)
