from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Mapping

from ..catalogs.policies import Models
from ..contracts import EffortRef, ModelRef, ModelSelection
from ..errors import UnsupportedOperationError
from .attachments import Attachment, Attachments

if TYPE_CHECKING:
    from .chat import Chat
    from .operation import MessageOperation


class Message:
    def __init__(
        self,
        content: str = "",
        *,
        role: str = "user",
        id: str | None = None,
        remote_id: str | None = None,
        attachments: Attachments | list[Attachment] | None = None,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        metadata: Mapping[str, Any] | None = None,
        chat: Chat | None = None,
    ) -> None:
        self.id = id or str(uuid.uuid4())
        self.role = role
        self.content = content
        self.remote_id = remote_id
        self.model = model
        self.metadata: dict[str, Any] = dict(metadata) if metadata is not None else {}
        self.chat = chat

        if isinstance(attachments, Attachments):
            self.attachments = attachments
        elif attachments is not None:
            self.attachments = Attachments(attachments)
        else:
            self.attachments = Attachments()

    @property
    def text(self) -> str:
        return self.content

    def edit(
        self,
        new_content: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
    ) -> MessageOperation:
        if self.chat is None:
            raise UnsupportedOperationError(
                "Cannot edit a message that does not belong to a conversation"
            )
        if self not in self.chat.messages._items:
            self.chat.messages._items.append(self)
        return self.chat.messages.edit(
            self.id,
            new_content,
            model=model if model is not None else self.model,
        )

    def retry(
        self,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
    ) -> MessageOperation:
        if self.chat is None:
            raise UnsupportedOperationError(
                "Cannot retry a message that does not belong to a conversation"
            )
        if self not in self.chat.messages._items:
            self.chat.messages._items.append(self)
        return self.chat.messages.retry(
            self.id,
            model=model if model is not None else self.model,
        )

    async def branch(self, title: str | None = None) -> Chat:
        if self.chat is None:
            raise UnsupportedOperationError(
                "Cannot branch a message that does not belong to a conversation"
            )
        return await self.chat.branch(self.id, title=title)

    def __repr__(self) -> str:
        preview = self.content[:40] + ("..." if len(self.content) > 40 else "")
        return (
            f"<Message role={self.role!r} content={preview!r} attachments={len(self.attachments)}>"
        )
