from __future__ import annotations

from ...resources.chat.chat import (
    AsyncBranchableChat,
    BranchableChat,
)
from ...resources.message.attachment import Attachment
from ...resources.message.message import AsyncMessage, Message
from ...resources.project.project import AsyncProject, Project
from ...resources.response.artifact import Artifact
from ...resources.response.response import Response
from ...resources.response.source import Source


class AsyncGeminiChat(AsyncBranchableChat):
    pass


class GeminiChat(BranchableChat):
    __slots__ = ()


class AsyncGeminiProject(AsyncProject):
    __slots__ = ()


class GeminiProject(Project):
    __slots__ = ()


class GeminiAttachment(Attachment):
    __slots__ = ()


class AsyncGeminiMessage(AsyncMessage):
    __slots__ = ()
    _attachment_type = GeminiAttachment


class GeminiMessage(Message):
    __slots__ = ()
    _attachment_type = GeminiAttachment


class GeminiResponse(Response):
    __slots__ = ()


class GeminiArtifact(Artifact):
    __slots__ = ()


class GeminiSource(Source):
    __slots__ = ()
