from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from ...errors import UnsupportedOperationError
from ..chat.chat import AsyncChat, Chat
from ..model.collections import Models
from ..model.model import (
    EffortRef,
    ModelRef,
    ModelSelection,
)
from ..response.artifact import ArtifactsCollection
from ..response.source import SourcesCollection
from .attachment import Attachment, Attachments
from .response import Response

if TYPE_CHECKING:
    from ...provider import Provider
    from .operation import AsyncMessageOperation, MessageOperation


class AsyncMessage:
    def __init__(
        self,
        content: str = "",
        *,
        role: str = "user",
        id: str | None = None,
        remote_id: str | None = None,
        attachments: Attachments | Iterable[Attachment] | None = None,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        metadata: Mapping[str, Any] | None = None,
        chat: AsyncChat | None = None,
        response: Response | None = None,
    ) -> None:
        self.id = id or str(uuid.uuid4())
        self.role = role
        self.content = content
        self.remote_id = remote_id
        self.model = model
        self.metadata: dict[str, Any] = dict(metadata) if metadata is not None else {}
        self.chat = chat
        self._response: Response | None = response

        if isinstance(attachments, Attachments):
            self.attachments = attachments
        elif attachments is not None:
            self.attachments = Attachments(attachments)
        else:
            self.attachments = Attachments()

    @property
    def text(self) -> str:
        return self.content

    @property
    def response(self) -> Response | None:
        return self._response

    @response.setter
    def response(self, value: Response | None) -> None:
        self._response = value

    @property
    def sources(self) -> SourcesCollection:
        if self._response is not None:
            return self._response.sources
        return SourcesCollection()

    @property
    def artifacts(self) -> ArtifactsCollection:
        if self._response is not None:
            return self._response.artifacts
        return ArtifactsCollection()

    def edit(
        self,
        new_content: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
    ) -> AsyncMessageOperation:
        if self.chat is None:
            raise UnsupportedOperationError(
                "Cannot edit a message that does not belong to a conversation"
            )
        self.chat.messages._track_local_message(self)
        return self.chat.messages.edit(
            self.id,
            new_content,
            model=model if model is not None else self.model,
        )

    def retry(
        self,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
    ) -> AsyncMessageOperation:
        if self.chat is None:
            raise UnsupportedOperationError(
                "Cannot retry a message that does not belong to a conversation"
            )
        self.chat.messages._track_local_message(self)
        return self.chat.messages.retry(
            self.id,
            model=model if model is not None else self.model,
        )

    def __repr__(self) -> str:
        preview = self.content[:40] + ("..." if len(self.content) > 40 else "")
        return (
            f"<AsyncMessage role={self.role!r} content={preview!r} "
            f"attachments={len(self.attachments)}>"
        )


class Message:
    """Synchronous view and input value for a message."""

    def __init__(
        self,
        content: str = "",
        *,
        role: str = "user",
        id: str | None = None,
        remote_id: str | None = None,
        attachments: Attachments | Iterable[Attachment] | None = None,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        metadata: Mapping[str, Any] | None = None,
        chat: Chat | AsyncChat | None = None,
    ) -> None:
        self._owner: Provider | None = None
        self._chat: Chat | AsyncChat | None = chat
        async_chat = chat._resource if isinstance(chat, Chat) else chat
        self._resource = AsyncMessage(
            content,
            role=role,
            id=id,
            remote_id=remote_id,
            attachments=attachments,
            model=model,
            metadata=metadata,
            chat=async_chat,
        )

    @classmethod
    def _from_async(cls, owner: Provider, resource: AsyncMessage) -> Message:
        message = cls.__new__(cls)
        message._owner = owner
        message._chat = None
        message._resource = resource
        return message

    @property
    def id(self) -> str:
        return self._resource.id

    @id.setter
    def id(self, value: str) -> None:
        self._resource.id = value

    @property
    def role(self) -> str:
        return self._resource.role

    @role.setter
    def role(self, value: str) -> None:
        self._resource.role = value

    @property
    def content(self) -> str:
        return self._resource.content

    @content.setter
    def content(self, value: str) -> None:
        self._resource.content = value

    @property
    def text(self) -> str:
        return self._resource.text

    @property
    def remote_id(self) -> str | None:
        return self._resource.remote_id

    @remote_id.setter
    def remote_id(self, value: str | None) -> None:
        self._resource.remote_id = value

    @property
    def model(self) -> Models | EffortRef | ModelRef | ModelSelection | str | None:
        return self._resource.model

    @model.setter
    def model(self, value: Models | EffortRef | ModelRef | ModelSelection | str | None) -> None:
        self._resource.model = value

    @property
    def metadata(self) -> dict[str, Any]:
        return self._resource.metadata

    @metadata.setter
    def metadata(self, value: Mapping[str, Any]) -> None:
        self._resource.metadata = dict(value)

    @property
    def attachments(self) -> Attachments:
        return self._resource.attachments

    @property
    def response(self) -> Response | None:
        raw = self._resource.response
        if raw is None:
            return None
        msg = raw.message
        if self._owner is not None and isinstance(msg, AsyncMessage):
            msg = Message._from_async(self._owner, msg)
        return Response(
            msg,
            sources=raw.sources,
            artifacts=raw.artifacts,
            metadata=raw.metadata,
            timing=raw.timing,
        )

    @property
    def sources(self) -> SourcesCollection:
        return self._resource.sources

    @property
    def artifacts(self) -> ArtifactsCollection:
        return self._resource.artifacts

    @property
    def chat(self) -> Chat | AsyncChat | None:
        if self._chat is None and self._owner is not None and self._resource.chat is not None:
            self._chat = self._owner._wrap_chat(self._resource.chat)
        return self._chat or self._resource.chat

    @chat.setter
    def chat(self, value: Chat | AsyncChat | None) -> None:
        self._chat = value
        self._resource.chat = value._resource if isinstance(value, Chat) else value

    def edit(
        self,
        new_content: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
    ) -> MessageOperation | AsyncMessageOperation:
        chat = self.chat
        if chat is None:
            raise UnsupportedOperationError(
                "Cannot edit a message that does not belong to a conversation"
            )
        return chat.messages.edit(
            self.id,
            new_content,
            model=model if model is not None else self.model,
        )

    def retry(
        self,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
    ) -> MessageOperation | AsyncMessageOperation:
        chat = self.chat
        if chat is None:
            raise UnsupportedOperationError(
                "Cannot retry a message that does not belong to a conversation"
            )
        return chat.messages.retry(
            self.id,
            model=model if model is not None else self.model,
        )

    def __repr__(self) -> str:
        preview = self.content[:40] + ("..." if len(self.content) > 40 else "")
        return (
            f"<Message role={self.role!r} content={preview!r} attachments={len(self.attachments)}>"
        )
