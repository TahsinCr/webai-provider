from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Any, AsyncGenerator, Protocol

from .errors import FeatureUnavailable, ProviderError, RemoteNotFound
from .models import (
    Capability,
    Conversation,
    Feature,
    FeatureState,
    ModelCatalog,
    Origin,
    Page,
    Project,
    ProjectOptions,
    ProviderDescription,
    SessionStatus,
)


class MessageAction(str, Enum):
    EDIT = "edit"
    RETRY = "retry"
    BRANCH = "branch"

    @property
    def capability(self):
        return {
            self.EDIT: Capability.MESSAGE_EDIT,
            self.RETRY: Capability.MESSAGE_RETRY,
            self.BRANCH: Capability.CHAT_BRANCH,
        }[self]


@dataclass(frozen=True)
class BranchContext:
    conversation_id: str
    message_id: str


@dataclass(frozen=True)
class SendRequest:
    provider_id: str
    account_id: str
    chat_id: str
    prompt: str
    conversation_id: str | None = None
    model: str | None = None
    effort: str | None = None
    project_id: str | None = None
    temporary: bool = False
    attachments: tuple[dict, ...] = ()
    action: MessageAction | None = None
    message_id: str | None = None
    branch: BranchContext | None = None


@dataclass(frozen=True)
class TextDelta:
    text: str
    channel: str | None = None


@dataclass(frozen=True)
class ThoughtDelta:
    text: str


@dataclass(frozen=True)
class ActionBegin:
    action_id: str
    kind: str
    title: str = ""


@dataclass(frozen=True)
class ActionDelta:
    action_id: str
    text: str


@dataclass(frozen=True)
class ActionEnd:
    action_id: str


@dataclass(frozen=True)
class ReplaceText:
    text: str
    channel: str | None = None


@dataclass(frozen=True)
class ResponseComplete:
    pass


@dataclass(frozen=True)
class ConversationBound:
    conversation_id: str


@dataclass(frozen=True)
class MessageMetadata:
    message_id: str | None = None
    user_message_id: str | None = None
    model: str | None = None
    effort: str | None = None
    sources: tuple[dict, ...] = ()
    actions: tuple[str, ...] = ()
    files: tuple[dict, ...] = ()


ProviderEvent = (
    TextDelta
    | ThoughtDelta
    | ActionBegin
    | ActionDelta
    | ActionEnd
    | ReplaceText
    | ResponseComplete
    | ConversationBound
    | MessageMetadata
)


class Provider(Protocol):
    id: str
    account_id: str
    capabilities: frozenset[Capability]

    async def prepare(self) -> None: ...

    def stream(self, request: SendRequest) -> AsyncGenerator[ProviderEvent, None]: ...


def validate_request(provider: Provider, request: SendRequest) -> None:
    if request.provider_id != provider.id or request.account_id != provider.account_id:
        raise ValueError("send request belongs to a different provider/account")
    if request.action is not None:
        if request.action not in (MessageAction.EDIT, MessageAction.RETRY):
            raise ValueError("unsupported generation action")
        if not request.conversation_id or not request.message_id:
            raise ValueError("message actions require a conversation and message identity")
    if request.branch is not None:
        if request.action is not None or request.temporary:
            raise ValueError("a branch continuation requires a new persistent turn")
        if not request.branch.conversation_id or not request.branch.message_id:
            raise ValueError("a branch continuation requires its source identities")
        if provider.id != "chatgpt":
            raise ValueError(
                "this provider creates branches immediately; no continuation context is needed"
            )


class ProjectEditing:
    async def project_options(self, project_id: str | None = None) -> ProjectOptions:
        async with self._gate:
            return await self.editor.options(project_id)

    async def _project_after_write(self, project_id: str, confirmed: Any) -> Project:
        for _ in range(12):
            project = await self.get_project(project_id)
            if confirmed(project):
                return project
            await asyncio.sleep(0.4)
        raise ProviderError(
            "Proje değişikliği sunucuda doğrulanamadı; tekrarlamadan önce eşitleyin"
        )

    async def update_project(self, project: Project) -> Project:
        if project.origin.provider_id != self.id or project.origin.account_id != self.account_id:
            raise ValueError("Proje başka bir sağlayıcı hesabına ait")
        remote = project.origin.remote_id
        if not project.name.strip():
            raise ValueError("Proje adı gerekir")
        current = await self.get_project(remote)
        changes = {}
        if current.name != project.name:
            changes["name"] = project.name
        if (current.instructions or "") != (project.instructions or ""):
            changes["instructions"] = project.instructions or ""
        if not changes:
            return current
        async with self._gate:
            await self.editor.write("update", remote, **changes)
        return await self._project_after_write(
            remote,
            lambda p: (
                p.name == project.name and (p.instructions or "") == (project.instructions or "")
            ),
        )

    async def add_project_resource(self, project_id: str, kind: str, **values: Any) -> Project:
        before = await self.get_project(project_id)
        ids = {r.id for r in before.resources}
        async with self._gate:
            await self.editor.write(
                "add_source", project_id, kind=kind, existing_resources=before.resources, **values
            )
        return await self._project_after_write(
            project_id, lambda p: any(r.id not in ids for r in p.resources)
        )

    async def remove_project_resource(self, project_id: str, resource_id: str) -> Project:
        before = await self.get_project(project_id)
        source = next((r for r in before.resources if r.id == resource_id), None)
        if source is None:
            raise ValueError("Proje kaynağı bulunamadı")
        async with self._gate:
            await self.editor.write(
                "remove_source",
                project_id,
                resource_id=resource_id,
                resource_name=source.name,
                resource=source,
            )
        return await self._project_after_write(
            project_id, lambda p: all(r.id != resource_id for r in p.resources)
        )


class ProviderAdapter(Provider):
    account_id: str

    async def close(self) -> None:
        pass

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

    async def login(self) -> SessionStatus:
        return await self.authenticate()

    async def authenticate(self) -> SessionStatus:
        return SessionStatus(
            getattr(self, "id", ""),
            getattr(self, "account_id", "default"),
            authenticated=True,
        )

    async def session_status(self) -> SessionStatus:
        return await self.authenticate()

    async def forget_session(self) -> SessionStatus:
        return SessionStatus(
            getattr(self, "id", ""),
            getattr(self, "account_id", "default"),
            authenticated=False,
        )

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
        *,
        visibility: str = "private",
        memory: str = "default",
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
    raise ProviderError("Silme sunucuda doğrulanamadı; yerel kayıt korundu. Eşitleyin.")
