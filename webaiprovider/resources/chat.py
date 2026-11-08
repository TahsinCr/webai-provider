from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from ..errors import ProviderError, UnsupportedOperationError

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

    async def download(self, target: Any) -> bytes:
        if self.remote_id:
            if self.session is None:
                raise ProviderError("Chat is not attached to an open provider")
            return await self.session._require_bridge().download(
                target, conversation_id=self.remote_id
            )
        return b""

    async def rename(self, title: str) -> Chat:
        if self.session is not None and hasattr(self.session, "chats"):
            return await self.session.chats.update(self, title=title)
        self.title = title
        if self.session is not None and hasattr(self.session, "_bridge") and self.session._bridge:
            await self.session._bridge.rename_chat(self, title)
        return self

    async def delete(self) -> None:
        if self.remote_id:
            if self.session is None:
                raise ProviderError("Chat is not attached to an open provider")
            await self.session._require_bridge().delete_chat(self)

    def __repr__(self) -> str:
        return f"<Chat id={self.id!r} title={self.title!r} remote_id={self.remote_id!r}>"


class AsyncBranchableChat(Chat):
    async def branch(self: Chat, message_id: str, *, title: str | None = None) -> Chat:
        if self.remote_id:
            if self.session is None:
                raise ProviderError("Chat is not attached to an open provider")
            return await self.session._require_bridge().branch_chat(self, message_id, title=title)
        return type(self)(
            title=title or f"{self.title} (Branch)",
            session=self.session,
            project_id=self.project_id,
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
            await self.session._require_bridge().move_chat(self, project_id)
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
