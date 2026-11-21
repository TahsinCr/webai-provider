from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from ...errors import UnsupportedOperationError
from ..model.model import (
    EffortRef,
    ModelRef,
    ModelSelection,
)
from ..model.types import Models
from ..response.artifact import ArtifactsCollection
from ..response.response import Response
from ..response.source import SourcesCollection
from .attachment import Attachment, Attachments

if TYPE_CHECKING:
    from ..chat.chat import AsyncChat, Chat
    from ..response.operation import AsyncMessageOperation, MessageOperation


class _BaseMessage:
    """Shared message state for both synchronous and asynchronous chat turns."""

    _attachment_type = Attachment

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
        chat: Any = None,
        response: Response | None = None,
    ) -> None:
        self.id = id or str(uuid.uuid4())
        self.role = role
        self.content = content
        self.remote_id = remote_id
        self.model = model
        self.metadata: dict[str, Any] = dict(metadata) if metadata is not None else {}
        self.chat: Any = chat
        self._response: Response | None = response

        if isinstance(attachments, Attachments):
            self.attachments = attachments
        else:
            self.attachments = Attachments(attachments, attachment_type=self._attachment_type)

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

    def __repr__(self) -> str:
        preview = self.content[:40] + ("..." if len(self.content) > 40 else "")
        return (
            f"<{type(self).__name__} role={self.role!r} content={preview!r} "
            f"attachments={len(self.attachments)}>"
        )


class AsyncMessage(_BaseMessage):
    chat: AsyncChat | None

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
        self.chat.responses._track_local_message(self)
        return self.chat.responses.edit(
            self,
            new_content,
            model=model if model is not None else self.model,
        )


class Message(_BaseMessage):
    chat: Chat | None

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
        self.chat.responses._track_local_message(self)
        return self.chat.responses.edit(
            self,
            new_content,
            model=model if model is not None else self.model,
        )
