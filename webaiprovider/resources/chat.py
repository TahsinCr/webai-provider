from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from ..contracts import Artifact
from ..errors import ProviderError, UnsupportedOperationError
from ..models import Conversation
from ..utils.files import download_source

if TYPE_CHECKING:
    from ..async_provider import AsyncProvider
    from ..provider import Provider, SyncChat, SyncProject
    from ..resources.message import Message
    from .project import Project


class Chat:
    _messages_collection_type = None

    def __init__(
        self,
        id: str | None = None,
        title: str = "",
        *,
        session: AsyncProvider | None = None,
        remote_id: str | None = None,
        project_id: str | None = None,
    ) -> None:
        self.id = id or str(uuid.uuid4())
        self.title = title
        self.session = session
        self.remote_id = remote_id
        self.project_id = project_id

        from ..collections.messages import MessagesCollection

        self.messages = (self._messages_collection_type or MessagesCollection)(self)

    def get(self, message_id: str) -> Message:
        return self.messages.get(message_id)

    async def download(self, target: Artifact | str) -> bytes:
        if self.remote_id:
            if self.session is None:
                raise ProviderError("Chat is not attached to an open provider")
            url = target.download_url if isinstance(target, Artifact) else target
            if not url:
                raise ValueError("Artifact has no download URL")
            source = await self.session._require_adapter().resolve_download(
                url,
                conversation_id=self.remote_id,
            )
            return await download_source(source)
        return b""

    async def rename(self, title: str) -> Chat:
        if self.session is not None:
            return await self.session.chats.update(self, title=title)
        self.title = title
        return self

    async def delete(self) -> None:
        if self.remote_id:
            if self.session is None:
                raise ProviderError("Chat is not attached to an open provider")
            await self.session._require_adapter().delete_chat(self.remote_id)

    def __repr__(self) -> str:
        return f"<Chat id={self.id!r} title={self.title!r} remote_id={self.remote_id!r}>"


class AsyncBranchableChat(Chat):
    async def branch(self: Chat, message_id: str, *, title: str | None = None) -> Chat:
        if not self.remote_id:
            raise UnsupportedOperationError(
                "Cannot branch a conversation that has not been persisted remotely"
            )
        if self.session is None:
            raise ProviderError("Chat is not attached to an open provider")
        branched = await self.session._require_adapter().branch_chat(
            self.remote_id, message_id
        )
        if not isinstance(branched, Conversation):
            raise ProviderError("Provider returned an invalid branched conversation")
        return type(self)(
            title=title or branched.title,
            remote_id=branched.origin.remote_id,
            project_id=branched.project_id,
            session=self.session,
        )


class AsyncMovableChat(Chat):
    async def move_to(self: Chat, project: Project | str | None) -> Chat:
        if self.session is None:
            raise ProviderError("Chat is not attached to a provider session")
        if project is not None and not isinstance(project, str):
            if project.session is not self.session:
                raise ValueError("Project and chat must belong to the same provider session")
            if self.remote_id and not project.remote_id:
                raise UnsupportedOperationError(
                    "A remote chat cannot be moved into a project that is not persisted remotely"
                )
            project_id = project.remote_id or project.id
        else:
            project_id = str(project) if project is not None else None
        if self.remote_id:
            await self.session._require_adapter().move_chat(self.remote_id, project_id)
        self.project_id = project_id
        return self


class _SyncChatCapability:
    _owner: Provider
    _resource: Any


class BranchableChat(_SyncChatCapability):
    def branch(self, message_id: str, *, title: str | None = None) -> SyncChat:
        chat = self._owner._call(self._resource.branch(message_id, title=title))
        return self._owner._wrap_chat(chat)


class MovableChat(_SyncChatCapability):
    def move_to(self, project: SyncProject | Project | str | None) -> SyncChat:
        target = getattr(project, "_resource", project)
        chat = self._owner._call(self._resource.move_to(target))
        return self._owner._wrap_chat(chat)
