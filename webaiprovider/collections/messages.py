from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, AsyncIterator

from ..catalogs import Models
from ..contracts import EffortRef, ModelRef, ModelSelection, StreamEvent
from ..errors import ProviderError
from ..models import Conversation
from ..resources.message import Message
from ..resources.operation import MessageOperation
from .attachments import Attachment, Attachments
from .base import ManageableCollection

if TYPE_CHECKING:
    from ..resources.chat import Chat


class MessagesCollection(ManageableCollection[Message]):
    def __init__(self, chat: Chat) -> None:
        super().__init__()
        self.chat = chat

    async def refresh(self) -> list[Message]:
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
        messages = [
            Message(
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
        message: Message | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Iterable[Attachment] | Attachments | None = None,
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
                for item in attachments:
                    msg.attachments.create(item)

        self._items.append(msg)
        return MessageOperation(self.chat, msg, executor=self._execute_send)

    async def _execute_send(self, operation: MessageOperation) -> AsyncIterator[StreamEvent]:
        session = self.chat.session
        if session is None:
            raise ProviderError("No provider session is attached to this chat")
        async for event in session._require_provider().execute_stream(operation):
            yield event

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
