from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Iterable, Iterator, Mapping
from contextlib import AsyncExitStack, ExitStack
from typing import TYPE_CHECKING, Any, TypeVar

from ...collections import _async_publish_collection_change, _publish_collection_change
from ...errors import ProviderError, UnsupportedOperationError
from ...transport.downloader import DownloadSource, async_download_source, download_source
from ..operations import async_execute_with_hooks, execute_with_hooks
from ..project.types import ProjectData
from ..response.artifact import Artifact
from .types import Conversation

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider
    from ..message.attachment import Attachment, Attachments
    from ..message.message import AsyncMessage, Message
    from ..model.model import EffortRef, ModelRef, ModelSelection
    from ..model.types import Models
    from ..project.project import AsyncProject, Project
    from ..response.operation import AsyncMessageOperation, StreamEvent
    from ..response.response import Response


def _resolve_project_arg(provider: Any, project: Any) -> Any:
    if project is None:
        return None
    from ..project.project import AsyncProject, Project

    if isinstance(project, (AsyncProject, Project)):
        if project.provider is not provider:
            raise ValueError("Project and chat must belong to the same provider session")
        return project
    return provider.projects._new(remote_id=str(project))


def _project_from_data(
    provider: Any,
    data: ProjectData | None,
    projects: Mapping[str, Any] | None = None,
) -> Any:
    if data is None:
        return None
    remote_id = data.id
    existing = projects.get(remote_id) if projects is not None else None
    if existing is not None:
        if data.name and not existing.title:
            existing.title = data.name
        return existing
    created = provider.projects._new(title=data.name, remote_id=remote_id)
    if data.instructions is not None or data.resources:
        created._apply_provider_project(data)
    return created


class _BaseChat:
    """Shared chat state, validation, and transition helpers for sync and async chats."""

    id: str
    title: str
    provider: Any
    remote_id: str | None
    project: Any
    temporary: bool
    metadata: dict[str, Any]
    responses: Any

    def _init_chat_state(
        self,
        *,
        id: str | None = None,
        title: str = "",
        provider: Any = None,
        remote_id: str | None = None,
        project: Any = None,
        temporary: bool = False,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self.id = id or str(uuid.uuid4())
        self.title = title
        self.provider = provider
        self.remote_id = remote_id
        self.project = project
        self.temporary = bool(temporary)
        self.metadata = dict(metadata) if metadata is not None else {}

    def _prepare_download_url(self, target: Artifact | str) -> str:
        if not self.remote_id:
            raise UnsupportedOperationError("Cannot download from an unpersisted chat")
        if self.provider is None:
            raise ProviderError("Chat is not attached to an open provider")
        url = target.download_url if isinstance(target, Artifact) else target
        if not url:
            raise ValueError("Artifact has no download URL")
        return url

    def _prepare_branch(self, message_id: str, title: str | None) -> tuple[Any, Any]:
        if not self.remote_id:
            raise UnsupportedOperationError(
                "Cannot branch a conversation that has not been persisted remotely"
            )
        if self.provider is None:
            raise ProviderError("Chat is not attached to an open provider")
        provider = self.provider
        context = provider._new_context(
            "chat.branch",
            resource_id=self.remote_id,
            parameters={"message_id": message_id, "title": title},
        )
        return provider, context

    def _build_branched_chat(self, provider: Any, branched: Any, title: str | None) -> Any:
        if not isinstance(branched, Conversation):
            raise ProviderError("Provider returned an invalid branched conversation")
        project = (
            _project_from_data(provider, branched.project)
            if branched.project is not None
            else self.project
        )
        return type(self)(
            title=title or branched.title,
            remote_id=branched.id,
            project=project,
            metadata=branched.metadata,
            provider=provider,
        )

    def _prepare_move(self, project: Any) -> tuple[Any, Any, str | None, Any]:
        provider = self.provider
        if provider is None:
            raise ProviderError("Chat is not attached to a provider session")
        target = _resolve_project_arg(provider, project)
        target_id = (target.remote_id or target.id) if target is not None else None
        context = provider._new_context(
            "chat.move",
            resource_id=self.remote_id or self.id,
            parameters={"project_id": target_id},
        )
        return provider, target, target_id, context

    def _resolve_move_destination(
        self, provider: Any, target: Any, target_id: str | None, effective_id: Any
    ) -> Any:
        if effective_id is not None and (
            not isinstance(effective_id, str) or not effective_id.strip()
        ):
            raise ValueError("Project ID must be a non-empty string")
        return target if effective_id == target_id else _resolve_project_arg(provider, effective_id)

    def _validate_move_rules(
        self,
        destination: Any,
        source_id: str | None,
        *,
        allow_reassignment: bool,
    ) -> tuple[str | None, list[str]]:
        if not allow_reassignment:
            if source_id is not None or (not self.remote_id and self.project is not None):
                raise UnsupportedOperationError(
                    "Moving a chat that already belongs to a project is not supported by the common chat contract"
                )
            if destination is None:
                raise UnsupportedOperationError("Moving a chat requires a target project")
        if not self.remote_id:
            return None, []
        if destination is not None and not destination.remote_id:
            raise UnsupportedOperationError(
                "A remote chat cannot be moved into a project that is not persisted remotely"
            )
        destination_id = destination.remote_id if destination is not None else None
        lock_ids = sorted({pid for pid in (source_id, destination_id) if pid})
        return destination_id, lock_ids

    def _finalize_move_state(self, ctx: Any, destination: Any) -> None:
        self.project = destination
        ctx.parameters["project_id"] = (
            destination.remote_id or destination.id if destination is not None else None
        )

    def __repr__(self) -> str:
        return (
            f"<{type(self).__name__} id={self.id!r} title={self.title!r} "
            f"remote_id={self.remote_id!r}>"
        )


class AsyncChat(_BaseChat):
    def __init__(
        self,
        id: str | None = None,
        title: str = "",
        *,
        provider: AsyncProvider[Any, Any] | None = None,
        remote_id: str | None = None,
        project: AsyncProject | None = None,
        temporary: bool = False,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self._init_chat_state(
            id=id,
            title=title,
            provider=provider,
            remote_id=remote_id,
            project=project,
            temporary=temporary,
            metadata=metadata,
        )
        from ..response.collections import AsyncResponsesCollection

        self.responses = AsyncResponsesCollection(self)

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
        url = self._prepare_download_url(target)
        source: DownloadSource | None = await self.provider._resolve_download_data(
            url,
            conversation_id=self.remote_id,
        )
        return await async_download_source(source)

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


class AsyncBranchableChat(AsyncChat):
    async def branch(self: AsyncChat, message_id: str, *, title: str | None = None) -> AsyncChat:
        provider, context = self._prepare_branch(message_id, title)

        async def work(ctx) -> AsyncChat:
            lock = provider._send_lock(self.remote_id)
            async with provider._activity(send_lock_id=id(lock)), lock:
                provider._require_provider()
                branched = await provider._branch_chat_data(
                    self.remote_id, ctx.parameters["message_id"]
                )
            return self._build_branched_chat(provider, branched, ctx.parameters["title"])

        async def on_success(ctx) -> None:
            created_chat: AsyncChat = ctx.result
            if created_chat.remote_id and not created_chat.metadata.get("pending_branch"):
                await _async_publish_collection_change(
                    provider, "chats", "create", created_chat, operation_id=ctx.operation_id
                )
            await provider._emit(
                "chat.branched",
                operation_id=ctx.operation_id,
                resource_id=created_chat.remote_id,
                payload={"source_chat_id": self.remote_id, "title": created_chat.title},
            )

        return await async_execute_with_hooks(
            operation_name="chat.branch",
            context=context,
            hooks_chain=(provider.hooks, provider.chats.hooks),
            work=work,
            on_success_event=on_success,
        )


_MovableChatT = TypeVar("_MovableChatT", bound="AsyncMovableChat")


class AsyncMovableChat(AsyncChat):
    async def move_to(self: _MovableChatT, project: AsyncProject | str) -> _MovableChatT:
        return await self._move_to(project, allow_reassignment=False)

    async def _move_to(
        self: _MovableChatT,
        project: AsyncProject | str | None,
        *,
        allow_reassignment: bool,
    ) -> _MovableChatT:
        provider, target, target_id, context = self._prepare_move(project)

        async def work(ctx) -> _MovableChatT:
            destination = self._resolve_move_destination(
                provider, target, target_id, ctx.parameters["project_id"]
            )
            lock = provider._send_lock(self.remote_id or self.id)
            async with provider._activity(send_lock_id=id(lock)), lock:
                source_id = self.project.remote_id if self.project is not None else None
                if self.remote_id:
                    provider._require_provider()
                    current = await provider._get_chat_metadata(self.remote_id)
                    source_id = current.project.id if current.project is not None else None
                destination_id, lock_ids = self._validate_move_rules(
                    destination, source_id, allow_reassignment=allow_reassignment
                )
                if self.remote_id:
                    async with AsyncExitStack() as locks:
                        for project_id in lock_ids:
                            await locks.enter_async_context(provider._project_lock(project_id))
                        await provider._move_chat_data(
                            self.remote_id, destination_id, source_project_id=source_id
                        )
                self._finalize_move_state(ctx, destination)
            return self

        async def on_success(ctx) -> None:
            if self.remote_id and not self.temporary:
                await _async_publish_collection_change(
                    provider, "chats", "update", self, operation_id=ctx.operation_id
                )
            await provider._emit(
                "chat.moved",
                operation_id=ctx.operation_id,
                resource_id=self.remote_id or self.id,
                payload={"project_id": ctx.parameters["project_id"]},
            )

        return await async_execute_with_hooks(
            operation_name="chat.move",
            context=context,
            hooks_chain=(provider.hooks, provider.chats.hooks),
            work=work,
            on_success_event=on_success,
        )


class Chat(_BaseChat):
    def __init__(
        self,
        id: str | None = None,
        title: str = "",
        *,
        provider: Provider[Any, Any] | None = None,
        remote_id: str | None = None,
        project: Project | None = None,
        temporary: bool = False,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self._init_chat_state(
            id=id,
            title=title,
            provider=provider,
            remote_id=remote_id,
            project=project,
            temporary=temporary,
            metadata=metadata,
        )
        from ..response.collections import ResponsesCollection

        self.responses = ResponsesCollection(self)

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

    def download(self, target: Artifact | str) -> bytes:
        url = self._prepare_download_url(target)
        source: DownloadSource | None = self.provider._resolve_download_data(
            url,
            conversation_id=self.remote_id,
        )
        return download_source(source)

    def rename(self, title: str) -> Chat:
        if self.provider is not None:
            return self.provider.chats.update(self, title=title)
        self.title = title
        return self

    def delete(self) -> None:
        if self.provider is not None:
            self.provider.chats.delete(self)
        elif self.remote_id:
            raise ProviderError("Chat is not attached to an open provider")


class BranchableChat(Chat):
    def branch(self: Chat, message_id: str, *, title: str | None = None) -> Chat:
        provider, context = self._prepare_branch(message_id, title)

        def work(ctx) -> Chat:
            lock = provider._send_lock(self.remote_id)
            with provider._activity(send_lock_id=id(lock)), lock:
                provider._require_provider()
                branched = provider._branch_chat_data(self.remote_id, ctx.parameters["message_id"])
            return self._build_branched_chat(provider, branched, ctx.parameters["title"])

        def on_success(ctx) -> None:
            created_chat: Chat = ctx.result
            if created_chat.remote_id and not created_chat.metadata.get("pending_branch"):
                _publish_collection_change(
                    provider, "chats", "create", created_chat, operation_id=ctx.operation_id
                )
            provider._emit(
                "chat.branched",
                operation_id=ctx.operation_id,
                resource_id=created_chat.remote_id,
                payload={"source_chat_id": self.remote_id, "title": created_chat.title},
            )

        return execute_with_hooks(
            operation_name="chat.branch",
            context=context,
            hooks_chain=(provider.hooks, provider.chats.hooks),
            work=work,
            on_success_event=on_success,
        )


_SyncMovableChatT = TypeVar("_SyncMovableChatT", bound="MovableChat")


class MovableChat(Chat):
    def move_to(self: _SyncMovableChatT, project: Project | str) -> _SyncMovableChatT:
        return self._move_to(project, allow_reassignment=False)

    def _move_to(
        self: _SyncMovableChatT,
        project: Project | str | None,
        *,
        allow_reassignment: bool,
    ) -> _SyncMovableChatT:
        provider, target, target_id, context = self._prepare_move(project)

        def work(ctx) -> _SyncMovableChatT:
            destination = self._resolve_move_destination(
                provider, target, target_id, ctx.parameters["project_id"]
            )
            lock = provider._send_lock(self.remote_id or self.id)
            with provider._activity(send_lock_id=id(lock)), lock:
                source_id = self.project.remote_id if self.project is not None else None
                if self.remote_id:
                    provider._require_provider()
                    current = provider._get_chat_metadata(self.remote_id)
                    source_id = current.project.id if current.project is not None else None
                destination_id, lock_ids = self._validate_move_rules(
                    destination, source_id, allow_reassignment=allow_reassignment
                )
                if self.remote_id:
                    with ExitStack() as locks:
                        for project_id in lock_ids:
                            locks.enter_context(provider._project_lock(project_id))
                        provider._move_chat_data(
                            self.remote_id, destination_id, source_project_id=source_id
                        )
                self._finalize_move_state(ctx, destination)
            return self

        def on_success(ctx) -> None:
            if self.remote_id and not self.temporary:
                _publish_collection_change(
                    provider, "chats", "update", self, operation_id=ctx.operation_id
                )
            provider._emit(
                "chat.moved",
                operation_id=ctx.operation_id,
                resource_id=self.remote_id or self.id,
                payload={"project_id": ctx.parameters["project_id"]},
            )

        return execute_with_hooks(
            operation_name="chat.move",
            context=context,
            hooks_chain=(provider.hooks, provider.chats.hooks),
            work=work,
            on_success_event=on_success,
        )
