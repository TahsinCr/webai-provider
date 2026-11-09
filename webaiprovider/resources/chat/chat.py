from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from ...contracts import Artifact
from ...errors import ProviderError, UnsupportedOperationError
from ...models import Conversation
from ...utils.files import DownloadSource, download_source

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider
    from ..message.message import AsyncMessage, Message
    from ..project.project import AsyncProject, Project


class AsyncChat:
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

        from ..message.collections import AsyncMessagesCollection

        self.messages = (self._messages_collection_type or AsyncMessagesCollection)(self)

    async def get(self, message_id: str) -> AsyncMessage:
        return await self.messages.get(message_id)

    async def download(self, target: Artifact | str) -> bytes:
        if self.remote_id:
            if self.session is None:
                raise ProviderError("Chat is not attached to an open provider")
            url = target.download_url if isinstance(target, Artifact) else target
            if not url:
                raise ValueError("Artifact has no download URL")
            source: DownloadSource | None = await self.session._require_provider().resolve_download(
                url,
                conversation_id=self.remote_id,
            )
            return await download_source(source)
        return b""

    async def rename(self, title: str) -> AsyncChat:
        if self.session is not None:
            return await self.session.chats.update(self, title=title)
        self.title = title
        return self

    async def delete(self) -> None:
        if self.remote_id:
            if self.session is None:
                raise ProviderError("Chat is not attached to an open provider")
            await self.session._require_provider().delete_chat(self.remote_id)

    def __repr__(self) -> str:
        return f"<AsyncChat id={self.id!r} title={self.title!r} remote_id={self.remote_id!r}>"


class AsyncBranchableChat(AsyncChat):
    async def branch(self: AsyncChat, message_id: str, *, title: str | None = None) -> AsyncChat:
        if not self.remote_id:
            raise UnsupportedOperationError(
                "Cannot branch a conversation that has not been persisted remotely"
            )
        if self.session is None:
            raise ProviderError("Chat is not attached to an open provider")
        branched = await self.session._require_provider().branch_chat(self.remote_id, message_id)
        if not isinstance(branched, Conversation):
            raise ProviderError("Provider returned an invalid branched conversation")
        return type(self)(
            title=title or branched.title,
            remote_id=branched.origin.remote_id,
            project_id=branched.project_id,
            session=self.session,
        )


class AsyncMovableChat(AsyncChat):
    async def move_to(self: AsyncChat, project: AsyncProject | str | None) -> AsyncChat:
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
            await self.session._require_provider().move_chat(self.remote_id, project_id)
        self.project_id = project_id
        return self


class _ChatCapability:
    _owner: Provider


class BranchableChat(_ChatCapability):
    _resource: AsyncBranchableChat

    def branch(self, message_id: str, *, title: str | None = None) -> Chat:
        chat = self._owner._call(self._resource.branch(message_id, title=title))
        return self._owner._wrap_chat(chat)


class MovableChat(_ChatCapability):
    _resource: AsyncMovableChat

    def move_to(self, project: Project | AsyncProject | str | None) -> Chat:
        if project is None or isinstance(project, str):
            target = project
        else:
            from ..project.project import Project

            target = project._resource if isinstance(project, Project) else project
        chat = self._owner._call(self._resource.move_to(target))
        return self._owner._wrap_chat(chat)


class Chat:
    messages_collection_type = None

    def __init__(self, owner: Provider, resource: AsyncChat) -> None:
        self._owner = owner
        self._resource = resource
        from ..message.collections import MessagesCollection

        self.messages = (self.messages_collection_type or MessagesCollection)(owner, resource)

    @property
    def id(self) -> str:
        return self._resource.id

    @property
    def remote_id(self) -> str | None:
        return self._resource.remote_id

    @property
    def title(self) -> str:
        return self._resource.title

    @property
    def project_id(self) -> str | None:
        return self._resource.project_id

    def get(self, message_id: str) -> Message:
        return self.messages.get(message_id)

    def download(self, target: Any) -> bytes:
        return self._owner._call(self._resource.download(target))

    def rename(self, title: str) -> Chat:
        return self._owner._wrap_chat(self._owner._call(self._resource.rename(title)))

    def delete(self) -> None:
        self._owner._call(self._resource.delete())

    def __repr__(self) -> str:
        return f"<Chat id={self.id!r} title={self.title!r} remote_id={self.remote_id!r}>"
