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


class AsyncGrokChat(AsyncMovableChat):
    pass


class GrokChat(MovableChat, Chat):
    __slots__ = ()


class AsyncGrokProject(AsyncProject):
    __slots__ = ()


class GrokProject(Project):
    __slots__ = ()


class AsyncGrokMessage(AsyncMessage):
    __slots__ = ()


class GrokMessage(Message):
    __slots__ = ()
    _resource_type = AsyncGrokMessage


class GrokResponse(Response):
    __slots__ = ()


class GrokAttachment(Attachment):
    __slots__ = ()


class GrokArtifact(Artifact):
    __slots__ = ()


class GrokSource(Source):
    __slots__ = ()
