from __future__ import annotations

import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from ...core.operations import execute_with_hooks
from ...errors import ProviderError, UnsupportedOperationError
from ...utils.files import DownloadSource, download_source
from ..artifact import Artifact
from ..project.types import ProjectData
from .types import Conversation

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider
    from ..message.message import AsyncMessage, Message
    from ..project.project import AsyncProject, Project


def _resolve_project_arg(
    session: AsyncProvider,
    project: AsyncProject | str | None,
) -> AsyncProject | None:
    if project is None:
        return None
    from ..project.project import AsyncProject

    if isinstance(project, AsyncProject):
        if project.session is not session:
            raise ValueError("Project and chat must belong to the same provider session")
        return project
    return session._project_type(remote_id=str(project), session=session)


def _project_from_data(
    session: AsyncProvider,
    data: ProjectData | None,
    projects: Mapping[str, AsyncProject] | None = None,
) -> AsyncProject | None:
    if data is None:
        return None
    remote_id = data.origin.remote_id
    existing = projects.get(remote_id) if projects is not None else None
    if existing is not None:
        if data.name and not existing.title:
            existing.title = data.name
        return existing
    created = session._project_type(
        title=data.name,
        remote_id=remote_id,
        session=session,
    )
    if data.instructions is not None or data.resources:
        created._apply_provider_project(data)
    return created


class AsyncChat:
    _messages_collection_type = None
    _responses_collection_type = None

    def __init__(
        self,
        id: str | None = None,
        title: str = "",
        *,
        session: AsyncProvider | None = None,
        remote_id: str | None = None,
        project: AsyncProject | None = None,
        temporary: bool = False,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self.id = id or str(uuid.uuid4())
        self.title = title
        self.session = session
        self.remote_id = remote_id
        self.project: AsyncProject | None = project
        self.temporary: bool = bool(temporary)
        self.metadata: dict[str, Any] = dict(metadata) if metadata is not None else {}

        from ..message.collections import AsyncMessagesCollection, AsyncResponsesCollection

        self.messages = (self._messages_collection_type or AsyncMessagesCollection)(self)
        self.responses = (self._responses_collection_type or AsyncResponsesCollection)(self)

    async def get(self, message_id: str) -> AsyncMessage:
        return await self.messages.get(message_id)

    async def download(self, target: Artifact | str) -> bytes:
        if self.remote_id:
            if self.session is None:
                raise ProviderError("Chat is not attached to an open provider")
            url = target.download_url if isinstance(target, Artifact) else target
            if not url:
                raise ValueError("Artifact has no download URL")
            source: DownloadSource | None = await self.session._resolve_download_data(
                url,
                conversation_id=self.remote_id,
            )
            return await download_source(source)
        raise UnsupportedOperationError("Cannot download from an unpersisted chat")

    async def rename(self, title: str) -> AsyncChat:
        if self.session is not None:
            return await self.session.chats.update(self, title=title)
        self.title = title
        return self

    async def delete(self) -> None:
        if self.session is not None:
            await self.session.chats.delete(self)
        elif self.remote_id:
            raise ProviderError("Chat is not attached to an open provider")

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
        session = self.session
        runtime = session.runtime
        context = runtime.new_context(
            "chat.branch",
            resource_id=self.remote_id,
            parameters={"message_id": message_id, "title": title},
        )

        async def work(ctx) -> AsyncChat:
            branched = await session._branch_chat_data(self.remote_id, ctx.parameters["message_id"])
            if not isinstance(branched, Conversation):
                raise ProviderError("Provider returned an invalid branched conversation")
            project = (
                _project_from_data(session, branched.project)
                if branched.project is not None
                else self.project
            )
            return type(self)(
                title=ctx.parameters["title"] or branched.title,
                remote_id=branched.origin.remote_id,
                project=project,
                metadata=branched.metadata,
                session=session,
            )

        async def on_success(ctx) -> None:
            created_chat: AsyncChat = ctx.result
            await runtime.emit(
                "chat.branched",
                operation_id=ctx.operation_id,
                resource_id=created_chat.remote_id,
                payload={"source_chat_id": self.remote_id, "title": created_chat.title},
            )

        return await execute_with_hooks(
            operation_name="chat.branch",
            context=context,
            hooks_chain=(runtime.hooks, session.chats.hooks),
            work=work,
            on_success_event=on_success,
        )


class AsyncMovableChat(AsyncChat):
    async def move_to(self: AsyncChat, project: AsyncProject | str) -> AsyncChat:
        if self.session is None:
            raise ProviderError("Chat is not attached to a provider session")
        if self.project is not None:
            raise UnsupportedOperationError(
                "Moving a chat that already belongs to a project is not supported by the common chat contract"
            )
        target = _resolve_project_arg(self.session, project)
        if target is None:
            raise UnsupportedOperationError("Moving a chat requires a target project")
        if self.remote_id and not target.remote_id:
            raise UnsupportedOperationError(
                "A remote chat cannot be moved into a project that is not persisted remotely"
            )
        session = self.session
        runtime = session.runtime
        target_id = target.remote_id or target.id
        context = runtime.new_context(
            "chat.move",
            resource_id=self.remote_id or self.id,
            parameters={"project_id": target_id},
        )

        async def work(ctx) -> AsyncChat:
            if self.remote_id:
                await session._move_chat_data(self.remote_id, ctx.parameters["project_id"])
            self.project = target
            return self

        async def on_success(ctx) -> None:
            await runtime.emit(
                "chat.moved",
                operation_id=ctx.operation_id,
                resource_id=self.remote_id or self.id,
                payload={"project_id": target_id},
            )

        return await execute_with_hooks(
            operation_name="chat.move",
            context=context,
            hooks_chain=(runtime.hooks, session.chats.hooks),
            work=work,
            on_success_event=on_success,
        )


class _ChatCapability:
    _owner: Provider


class BranchableChat(_ChatCapability):
    _resource: AsyncBranchableChat

    def branch(self, message_id: str, *, title: str | None = None) -> Chat:
        chat = self._owner._call(self._resource.branch(message_id, title=title))
        return self._owner._wrap_chat(chat)


class MovableChat(_ChatCapability):
    _resource: AsyncMovableChat

    def move_to(self, project: Project | AsyncProject | str) -> Chat:
        from ..project.project import Project

        target = project._resource if isinstance(project, Project) else project
        chat = self._owner._call(self._resource.move_to(target))
        return self._owner._wrap_chat(chat)


class Chat:
    messages_collection_type = None
    responses_collection_type = None

    def __init__(self, owner: Provider, resource: AsyncChat) -> None:
        self._owner = owner
        self._resource = resource
        from ..message.collections import MessagesCollection, ResponsesCollection

        self.messages = (self.messages_collection_type or MessagesCollection)(owner, resource)
        self.responses = (self.responses_collection_type or ResponsesCollection)(owner, resource)

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
    def project(self) -> Project | None:
        if self._resource.project is None:
            return None
        return self._owner._wrap_project(self._resource.project)

    @property
    def temporary(self) -> bool:
        return self._resource.temporary

    @property
    def metadata(self) -> dict[str, Any]:
        return self._resource.metadata

    @metadata.setter
    def metadata(self, value: Mapping[str, Any]) -> None:
        self._resource.metadata = dict(value)

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
