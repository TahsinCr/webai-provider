from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Iterable, Iterator, Mapping
from typing import TYPE_CHECKING, Any

from ...collections import _publish_collection_change
from ...core.files import DownloadSource, download_source
from ...core.operations import execute_with_hooks
from ...errors import ProviderError, UnsupportedOperationError
from ..project.types import ProjectData
from ..response.artifact import Artifact
from .types import Conversation

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider
    from ..message.attachment import Attachment, Attachments
    from ..message.message import AsyncMessage, Message
    from ..message.operation import AsyncMessageOperation, StreamEvent
    from ..model.model import EffortRef, ModelRef, ModelSelection
    from ..model.types import Models
    from ..project.project import AsyncProject, Project
    from ..response.response import Response


def _resolve_project_arg(
    provider: AsyncProvider,
    project: AsyncProject | str | None,
) -> AsyncProject | None:
    if project is None:
        return None
    from ..project.project import AsyncProject

    if isinstance(project, AsyncProject):
        if project.provider is not provider:
            raise ValueError("Project and chat must belong to the same provider session")
        return project
    return provider._project_type(remote_id=str(project), provider=provider)


def _project_from_data(
    provider: AsyncProvider,
    data: ProjectData | None,
    projects: Mapping[str, AsyncProject] | None = None,
) -> AsyncProject | None:
    if data is None:
        return None
    remote_id = data.id
    existing = projects.get(remote_id) if projects is not None else None
    if existing is not None:
        if data.name and not existing.title:
            existing.title = data.name
        return existing
    created = provider._project_type(
        title=data.name,
        remote_id=remote_id,
        provider=provider,
    )
    if data.instructions is not None or data.resources:
        created._apply_provider_project(data)
    return created


class AsyncChat:
    _responses_collection_type = None

    def __init__(
        self,
        id: str | None = None,
        title: str = "",
        *,
        provider: AsyncProvider | None = None,
        remote_id: str | None = None,
        project: AsyncProject | None = None,
        temporary: bool = False,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self.id = id or str(uuid.uuid4())
        self.title = title
        self.provider = provider
        self.remote_id = remote_id
        self.project: AsyncProject | None = project
        self.temporary: bool = bool(temporary)
        self.metadata: dict[str, Any] = dict(metadata) if metadata is not None else {}

        from ..response.collections import AsyncResponsesCollection

        self.responses = (self._responses_collection_type or AsyncResponsesCollection)(self)

    async def get(self, item_id: str) -> Response:
        return await self.responses.get(item_id)

    def ask(
        self,
        prompt: AsyncMessage | Message | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Iterable[Attachment] | Attachments | None = None,
        **metadata: Any,
    ) -> AsyncMessageOperation:
        return self.responses.create(
            prompt,
            model=model,
            attachments=attachments,
            **metadata,
        )

    async def stream(
        self,
        prompt: AsyncMessage | Message | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Iterable[Attachment] | Attachments | None = None,
        **metadata: Any,
    ) -> AsyncIterator[StreamEvent]:
        operation = self.responses.create(
            prompt,
            model=model,
            attachments=attachments,
            **metadata,
        )
        async for event in operation.stream():
            yield event

    async def download(self, target: Artifact | str) -> bytes:
        if self.remote_id:
            if self.provider is None:
                raise ProviderError("Chat is not attached to an open provider")
            url = target.download_url if isinstance(target, Artifact) else target
            if not url:
                raise ValueError("Artifact has no download URL")
            source: DownloadSource | None = await self.provider._resolve_download_data(
                url,
                conversation_id=self.remote_id,
            )
            return await download_source(source)
        raise UnsupportedOperationError("Cannot download from an unpersisted chat")

    async def rename(self, title: str) -> AsyncChat:
        if self.provider is not None:
            return await self.provider.chats.update(self, title=title)
        self.title = title
        return self

    async def delete(self) -> None:
        if self.provider is not None:
            await self.provider.chats.delete(self)
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
        if self.provider is None:
            raise ProviderError("Chat is not attached to an open provider")
        provider = self.provider
        runtime = provider.runtime
        context = runtime.new_context(
            "chat.branch",
            resource_id=self.remote_id,
            parameters={"message_id": message_id, "title": title},
        )

        async def work(ctx) -> AsyncChat:
            lock = runtime.send_lock(self.remote_id)
            async with runtime.activity(send_lock_id=id(lock)):
                async with lock:
                    provider._require_provider()
                    branched = await provider._branch_chat_data(
                        self.remote_id, ctx.parameters["message_id"]
                    )
            if not isinstance(branched, Conversation):
                raise ProviderError("Provider returned an invalid branched conversation")
            project = (
                _project_from_data(provider, branched.project)
                if branched.project is not None
                else self.project
            )
            return type(self)(
                title=ctx.parameters["title"] or branched.title,
                remote_id=branched.id,
                project=project,
                metadata=branched.metadata,
                provider=provider,
            )

        async def on_success(ctx) -> None:
            created_chat: AsyncChat = ctx.result
            if created_chat.remote_id and not created_chat.metadata.get("pending_branch"):
                await _publish_collection_change(
                    runtime, "chats", "create", created_chat, operation_id=ctx.operation_id
                )
            await runtime.emit(
                "chat.branched",
                operation_id=ctx.operation_id,
                resource_id=created_chat.remote_id,
                payload={"source_chat_id": self.remote_id, "title": created_chat.title},
            )

        return await execute_with_hooks(
            operation_name="chat.branch",
            context=context,
            hooks_chain=(runtime.hooks, provider.chats.hooks),
            work=work,
            on_success_event=on_success,
        )


class AsyncMovableChat(AsyncChat):
    async def move_to(self: AsyncChat, project: AsyncProject | str) -> AsyncChat:
        if self.provider is None:
            raise ProviderError("Chat is not attached to a provider session")
        if self.project is not None:
            raise UnsupportedOperationError(
                "Moving a chat that already belongs to a project is not supported by the common chat contract"
            )
        target = _resolve_project_arg(self.provider, project)
        if target is None:
            raise UnsupportedOperationError("Moving a chat requires a target project")
        if self.remote_id and not target.remote_id:
            raise UnsupportedOperationError(
                "A remote chat cannot be moved into a project that is not persisted remotely"
            )
        provider = self.provider
        runtime = provider.runtime
        target_id = target.remote_id or target.id
        context = runtime.new_context(
            "chat.move",
            resource_id=self.remote_id or self.id,
            parameters={"project_id": target_id},
        )

        async def work(ctx) -> AsyncChat:
            if self.remote_id:
                lock = runtime.send_lock(self.remote_id)
                async with runtime.activity(send_lock_id=id(lock)):
                    async with lock:
                        provider._require_provider()
                        await provider._move_chat_data(self.remote_id, ctx.parameters["project_id"])
            self.project = target
            return self

        async def on_success(ctx) -> None:
            if self.remote_id and not self.temporary:
                await _publish_collection_change(
                    runtime, "chats", "update", self, operation_id=ctx.operation_id
                )
            await runtime.emit(
                "chat.moved",
                operation_id=ctx.operation_id,
                resource_id=self.remote_id or self.id,
                payload={"project_id": target_id},
            )

        return await execute_with_hooks(
            operation_name="chat.move",
            context=context,
            hooks_chain=(runtime.hooks, provider.chats.hooks),
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
    responses_collection_type = None

    def __init__(self, owner: Provider, resource: AsyncChat) -> None:
        self._owner = owner
        self._resource = resource
        from ..response.collections import ResponsesCollection

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

    def get(self, item_id: str) -> Response:
        return self.responses.get(item_id)

    def ask(
        self,
        prompt: Message | AsyncMessage | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Iterable[Attachment] | Attachments | None = None,
        **metadata: Any,
    ) -> Response:
        return self.responses.create(
            prompt,
            model=model,
            attachments=attachments,
            **metadata,
        ).wait()

    def stream(
        self,
        prompt: Message | AsyncMessage | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Iterable[Attachment] | Attachments | None = None,
        **metadata: Any,
    ) -> Iterator[StreamEvent]:
        operation = self.responses.create(
            prompt,
            model=model,
            attachments=attachments,
            **metadata,
        )
        yield from operation.stream()

    def download(self, target: Any) -> bytes:
        return self._owner._call(self._resource.download(target))

    def rename(self, title: str) -> Chat:
        return self._owner._wrap_chat(self._owner._call(self._resource.rename(title)))

    def delete(self) -> None:
        self._owner._call(self._resource.delete())
