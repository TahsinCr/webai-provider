from __future__ import annotations

from ...resources.chat.chat import (
    AsyncBranchableChat,
    AsyncMovableChat,
    BranchableChat,
    Chat,
    MovableChat,
)
from ...resources.message.attachment import Attachment
from ...resources.message.message import AsyncMessage, Message
from ...resources.project.project import AsyncProject, Project
from ...resources.response.artifact import Artifact
from ...resources.response.response import Response
from ...resources.response.source import Source


class AsyncChatGPTChat(AsyncBranchableChat, AsyncMovableChat):
    pass


class ChatGPTChat(BranchableChat, MovableChat, Chat):
    __slots__ = ()


class AsyncChatGPTProject(AsyncProject):
    __slots__ = ()


class ChatGPTProject(Project):
    __slots__ = ()


class AsyncChatGPTMessage(AsyncMessage):
    __slots__ = ()


class ChatGPTMessage(Message):
    __slots__ = ()
    _resource_type = AsyncChatGPTMessage


class ChatGPTResponse(Response):
    __slots__ = ()


class ChatGPTAttachment(Attachment):
    __slots__ = ()


class ChatGPTArtifact(Artifact):
    __slots__ = ()


class ChatGPTSource(Source):
    __slots__ = ()
