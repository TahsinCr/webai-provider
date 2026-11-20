from __future__ import annotations

from typing import cast

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
    async def move_to(self, project: AsyncProject | str | None) -> AsyncChatGPTChat:
        """Assign, transfer, or detach this conversation."""
        return await self._move_to(project, allow_reassignment=True)


class ChatGPTChat(BranchableChat, MovableChat, Chat):
    __slots__ = ()

    def move_to(self, project: Project | AsyncProject | str | None) -> ChatGPTChat:
        """Assign, transfer, or detach this conversation."""
        target = project._resource if isinstance(project, Project) else project
        self._owner._call(cast(AsyncChatGPTChat, self._resource).move_to(target))
        return self


class AsyncChatGPTProject(AsyncProject):
    __slots__ = ()


class ChatGPTProject(Project):
    __slots__ = ()


class ChatGPTAttachment(Attachment):
    __slots__ = ()


class AsyncChatGPTMessage(AsyncMessage):
    __slots__ = ()
    _attachment_type = ChatGPTAttachment


class ChatGPTMessage(Message):
    __slots__ = ()
    _resource_type = AsyncChatGPTMessage


class ChatGPTResponse(Response):
    __slots__ = ()


class ChatGPTArtifact(Artifact):
    __slots__ = ()


class ChatGPTSource(Source):
    __slots__ = ()
