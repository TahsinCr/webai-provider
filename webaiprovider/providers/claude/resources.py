from __future__ import annotations

from ...resources.chat.chat import (
    AsyncMovableChat,
    Chat,
    MovableChat,
)
from ...resources.message.attachment import Attachment
from ...resources.message.message import AsyncMessage, Message
from ...resources.project.project import AsyncProject, Project
from ...resources.response.artifact import Artifact
from ...resources.response.response import Response
from ...resources.response.source import Source


class AsyncClaudeChat(AsyncMovableChat):
    pass


class ClaudeChat(MovableChat, Chat):
    __slots__ = ()


class AsyncClaudeProject(AsyncProject):
    __slots__ = ()


class ClaudeProject(Project):
    __slots__ = ()


class AsyncClaudeMessage(AsyncMessage):
    __slots__ = ()


class ClaudeMessage(Message):
    __slots__ = ()
    _resource_type = AsyncClaudeMessage


class ClaudeResponse(Response):
    __slots__ = ()


class ClaudeAttachment(Attachment):
    __slots__ = ()


class ClaudeArtifact(Artifact):
    __slots__ = ()


class ClaudeSource(Source):
    __slots__ = ()
