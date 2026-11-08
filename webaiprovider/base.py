from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Protocol, cast

from .accounts.status import SessionState, SessionStatus
from .contracts import (
    Artifact,
    ArtifactDiscovered,
    OutputBegin,
    OutputDelta,
    OutputEnd,
    OutputReplace,
    Source,
    SourceDiscovered,
    StreamEvent,
    ThinkingBegin,
    ThinkingDelta,
    ThinkingEnd,
)
from .errors import FeatureUnavailable, ProviderError, RemoteNotFound, UnsupportedOperationError
from .models import (
    Capability,
    Conversation,
    Feature,
    FeatureState,
    MessageAction,
    ModelCatalog,
    Origin,
    Page,
    Project,
    ProjectOptions,
    ProviderDescription,
)


@dataclass(frozen=True)
class _ProviderRequest:
    prompt: str
    conversation_id: str | None = None
    model: str | None = None
    effort: str | None = None
    project_id: str | None = None
    temporary: bool = False
    attachments: tuple[dict, ...] = ()
    action: MessageAction | None = None
    message_id: str | None = None


async def _prepare_operation(operation: Any, provider: Any) -> _ProviderRequest:
    message = operation.message
    chat = operation.chat
    raw_action = message.metadata.get("action")
    try:
        action = MessageAction(raw_action) if raw_action is not None else None
    except ValueError:
        raise UnsupportedOperationError("Unsupported message action") from None

    message_id = message.metadata.get("parent_message_id")
    if action is not None:
        if action not in (MessageAction.EDIT, MessageAction.RETRY):
            raise UnsupportedOperationError("Unsupported message action")
        if not chat.remote_id or not message_id:
            raise UnsupportedOperationError("Message actions require remote message identities")

    session_account = getattr(getattr(chat, "session", None), "account", None)
    if session_account is not None and (
        session_account.provider != provider.id or session_account.id != provider.account_id
    ):
        raise ValueError("message operation belongs to a different provider account")

    model: str | None = None
    effort: str | None = None
    if message.model is not None:
        if chat.session is None:
            raise UnsupportedOperationError("Model selection requires an active provider session")
        selection = await chat.session.models.resolve(message.model)
        model = selection.model.id
        effort = selection.effort.id if selection.effort is not None else None

    attachments = tuple(
        {
            key: value
            for key, value in {
                "id": item.id,
                "path": str(item.path) if item.path is not None else None,
                "bytes": item._content,
                "name": item.name,
                "mime": item.mime_type,
                "size_bytes": getattr(item, "size", None),
                "grok_file": getattr(item, "grok_file", None),
            }.items()
            if value is not None
        }
        for item in message.attachments
    )
    return _ProviderRequest(
        prompt=message.content,
        model=model,
        effort=effort,
        attachments=attachments,
        action=action,
        message_id=message_id,
        temporary=bool(message.metadata.get("temporary", False)),
        conversation_id=chat.remote_id,
        project_id=chat.project_id,
    )


@dataclass(frozen=True)
class _TextDelta:
    text: str
    channel: str | None = None


@dataclass(frozen=True)
class _ThoughtDelta:
    text: str


@dataclass(frozen=True)
class _ReplaceText:
    text: str
    channel: str | None = None


@dataclass(frozen=True)
class _ResponseComplete:
    pass


@dataclass(frozen=True)
class _ConversationBound:
    conversation_id: str


@dataclass(frozen=True)
class _MessageMetadata:
    message_id: str | None = None
    user_message_id: str | None = None
    model: str | None = None
    effort: str | None = None
    sources: tuple[dict, ...] = ()
    files: tuple[dict, ...] = ()


_ProviderEvent = (
    _TextDelta
    | _ThoughtDelta
    | _ReplaceText
    | _ResponseComplete
    | _ConversationBound
    | _MessageMetadata
)


class AdapterProtocol(Protocol):
    id: str
    account_id: str
    capabilities: frozenset[Capability]

    async def prepare(self) -> None: ...

    def execute_stream(self, operation: Any) -> AsyncGenerator[StreamEvent, None]: ...


class _ProjectEditingHost(Protocol):
    id: str
    account_id: str
    _gate: asyncio.Lock
    editor: Any

    async def get_project(self, project_id: str) -> Project: ...


class ProjectEditing:
    def _host(self) -> _ProjectEditingHost:
        return cast(_ProjectEditingHost, self)

    async def project_options(self, project_id: str | None = None) -> ProjectOptions:
        host = self._host()
        async with host._gate:
            return await host.editor.options(project_id)

    async def _project_after_write(self, project_id: str, confirmed: Any) -> Project:
        host = self._host()
        for _ in range(12):
            project = await host.get_project(project_id)
            if confirmed(project):
                return project
            await asyncio.sleep(0.4)
        raise ProviderError(
            "Project change could not be confirmed by the server; sync before retrying"
        )

    async def update_project(self, project: Project) -> Project:
        host = self._host()
        if project.origin.provider_id != host.id or project.origin.account_id != host.account_id:
            raise ValueError("Project belongs to a different provider account")
        remote = project.origin.remote_id
        if not project.name.strip():
            raise ValueError("Project name is required")
        current = await host.get_project(remote)
        changes = {}
        if current.name != project.name:
            changes["name"] = project.name
        if (current.instructions or "") != (project.instructions or ""):
            changes["instructions"] = project.instructions or ""
        if not changes:
            return current
        async with host._gate:
            await host.editor.write("update", remote, **changes)
        return await self._project_after_write(
            remote,
            lambda current: (
                current.name == project.name
                and (current.instructions or "") == (project.instructions or "")
            ),
        )

    async def add_project_resource(self, project_id: str, kind: str, **values: Any) -> Project:
        host = self._host()
        before = await host.get_project(project_id)
        ids = {resource.id for resource in before.resources}
        async with host._gate:
            await host.editor.write(
                "add_source",
                project_id,
                kind=kind,
                existing_resources=before.resources,
                **values,
            )
        return await self._project_after_write(
            project_id,
            lambda project: any(resource.id not in ids for resource in project.resources),
        )

    async def remove_project_resource(self, project_id: str, resource_id: str) -> Project:
        host = self._host()
        before = await host.get_project(project_id)
        source = next(
            (resource for resource in before.resources if resource.id == resource_id), None
        )
        if source is None:
            raise ValueError("Project resource was not found")
        async with host._gate:
            await host.editor.write(
                "remove_source",
                project_id,
                resource_id=resource_id,
                resource_name=source.name,
                resource=source,
            )
        return await self._project_after_write(
            project_id,
            lambda project: all(resource.id != resource_id for resource in project.resources),
        )


class ProviderAdapter(AdapterProtocol):
    account_id: str

    async def close(self) -> None:
        pass

    def _stream_request(self, request: _ProviderRequest) -> AsyncGenerator[_ProviderEvent, None]:
        raise NotImplementedError

    async def execute_stream(self, operation: Any) -> AsyncGenerator[StreamEvent, None]:
        request = await _prepare_operation(operation, self)
        output_started = thinking_started = False
        thinking_text = ""
        try:
            async for event in self._stream_request(request):
                if isinstance(event, _ConversationBound):
                    operation.chat.remote_id = event.conversation_id
                elif isinstance(event, _ThoughtDelta) or (
                    isinstance(event, (_TextDelta, _ReplaceText)) and event.channel == "analysis"
                ):
                    delta = event.text
                    if isinstance(event, _ReplaceText):
                        delta = (
                            event.text[len(thinking_text) :]
                            if event.text.startswith(thinking_text)
                            else event.text
                        )
                        thinking_text = event.text
                    else:
                        thinking_text += delta
                    if not thinking_started:
                        thinking_started = True
                        yield ThinkingBegin()
                    if delta:
                        yield ThinkingDelta(delta=delta)
                elif isinstance(event, _ReplaceText):
                    if not event.text and not output_started:
                        continue
                    if thinking_started:
                        thinking_started = False
                        yield ThinkingEnd(text=thinking_text)
                    if not output_started:
                        output_started = True
                        yield OutputBegin()
                    yield OutputReplace(text=event.text)
                elif isinstance(event, _TextDelta) and event.text:
                    if thinking_started:
                        thinking_started = False
                        yield ThinkingEnd(text=thinking_text)
                    if not output_started:
                        output_started = True
                        yield OutputBegin()
                    yield OutputDelta(delta=event.text)
                elif isinstance(event, _MessageMetadata):
                    if event.user_message_id:
                        operation.message.id = event.user_message_id
                    if event.message_id:
                        operation._assistant_message_id = event.message_id
                    for record in event.sources:
                        yield SourceDiscovered(
                            Source(
                                id=str(record.get("id") or record.get("source_id") or ""),
                                url=str(record.get("url") or ""),
                                title=str(record.get("title") or ""),
                                snippet=str(record.get("snippet") or ""),
                                kind=str(record.get("kind") or "web"),
                                metadata=record,
                            )
                        )
                    for record in event.files:
                        size = record.get("size")
                        yield ArtifactDiscovered(
                            Artifact(
                                id=str(record.get("id") or record.get("file_id") or ""),
                                kind=str(record.get("kind") or "file"),
                                name=str(
                                    record.get("name")
                                    or record.get("filename")
                                    or record.get("file_name")
                                    or ""
                                ),
                                mime_type=str(
                                    record.get("mime_type") or "application/octet-stream"
                                ),
                                size=size if isinstance(size, int) else None,
                                download_url=record.get("download_url") or record.get("url"),
                                content=record.get("content"),
                                metadata=record,
                            )
                        )

            if thinking_started:
                yield ThinkingEnd(text=thinking_text)
            if output_started:
                yield OutputEnd()
        except ProviderError:
            raise
        except Exception as exc:
            raise ProviderError("Provider request failed") from exc

    def attachment_policy(self) -> dict | None:
        from .utils.attachments import policy_for

        policy = policy_for(getattr(self, "id", ""))
        return policy.as_dict() if policy else None

    def _origin(self, remote: str) -> Origin:
        return Origin(getattr(self, "id", ""), getattr(self, "account_id", "default"), remote)

    async def describe(self) -> ProviderDescription:
        caps = getattr(self, "capabilities", frozenset())
        return ProviderDescription(
            getattr(self, "id", ""),
            tuple(
                Feature(
                    c,
                    FeatureState.AVAILABLE if c in caps else FeatureState.UNSUPPORTED,
                )
                for c in Capability
            ),
        )

    async def authenticate(self) -> SessionStatus:
        raise FeatureUnavailable(Capability.AUTH, "not_implemented")

    async def session_status(self) -> SessionStatus:
        return SessionStatus(SessionState.UNKNOWN, reason="not_implemented")

    async def forget_session(self) -> SessionStatus:
        raise FeatureUnavailable(Capability.AUTH, "forget_session_not_implemented")

    async def models(self, *, force: bool = False) -> ModelCatalog:
        raise FeatureUnavailable(Capability.MODELS, "not_implemented")

    async def list_chats(self, cursor: str | None = None, limit: int = 28) -> Page[Conversation]:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    async def get_chat(self, conversation_id: str) -> Conversation:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    async def branch_chat(self, conversation_id: str, message_id: str) -> Conversation:
        raise FeatureUnavailable(Capability.CHAT_BRANCH, "not_implemented")

    async def conversation_attachment_count(self, conversation_id: str) -> int | None:
        raise FeatureUnavailable(Capability.ATTACHMENTS, "not_implemented")

    async def rename_chat(self, conversation_id: str, title: str) -> None:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    async def delete_chat(self, conversation_id: str) -> None:
        raise FeatureUnavailable(Capability.DELETE, "not_implemented")

    async def upload_file(self, path: Any) -> dict:
        raise FeatureUnavailable(Capability.ATTACHMENTS, "not_implemented")

    async def list_projects(self, cursor: str | None = None) -> Page[Project]:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    async def get_project(self, project_id: str) -> Project:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    async def list_project_chats(
        self, project_id: str, cursor: str | None = None
    ) -> Page[Conversation]:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    async def create_project(
        self,
        name: str,
        instructions: str = "",
    ) -> Project:
        raise FeatureUnavailable(Capability.PROJECT_CREATE, "not_implemented")

    async def update_project(self, project: Project) -> Project:
        raise FeatureUnavailable(Capability.PROJECT_UPDATE, "not_implemented")

    async def project_options(self, project_id: str | None = None) -> ProjectOptions:
        return ProjectOptions()

    async def add_project_resource(self, project_id: str, kind: str, **values: Any) -> Project:
        raise FeatureUnavailable(Capability.RESOURCES, "not_implemented")

    async def remove_project_resource(self, project_id: str, resource_id: str) -> Project:
        raise FeatureUnavailable(Capability.RESOURCES, "not_implemented")

    async def delete_project(self, project_id: str) -> None:
        raise FeatureUnavailable(Capability.PROJECT_DELETE, "not_implemented")

    async def move_chat(self, conversation_id: str, project_id: str | None) -> None:
        raise FeatureUnavailable(Capability.PROJECT_MEMBERSHIP, "not_implemented")

    async def resolve_download(
        self, url: str, conversation_id: str | None = None, message_id: str | None = None
    ) -> Any | None:
        return None


async def confirm_deleted(read: Any, missing: Any = lambda value: False) -> None:
    for delay in (0, 0.2, 0.5, 1):
        if delay:
            await asyncio.sleep(delay)
        try:
            value = await read()
        except RemoteNotFound:
            return
        if missing(value):
            return
    raise ProviderError(
        "Deletion could not be confirmed by the server; the local record was preserved. Sync before retrying."
    )
