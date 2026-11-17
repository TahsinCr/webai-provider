from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, runtime_checkable

from .errors import FeatureUnavailable, ProviderError, RemoteNotFound

if TYPE_CHECKING:
    from .accounts.account import Account
    from .accounts.status import SessionStatus
    from .resources.chat.types import Conversation
    from .resources.message.protocol import ProviderRequest, ProviderStreamEvent
    from .resources.model.model import ModelCatalog
    from .resources.project.types import ProjectData, ProjectOptions
    from .transport import AsyncTransport


class ProviderType(str, Enum):
    CHATGPT = "chatgpt"
    CLAUDE = "claude"
    GEMINI = "gemini"
    GROK = "grok"

    @classmethod
    def parse(cls, value: ProviderType | str) -> ProviderType:
        try:
            return value if isinstance(value, cls) else cls(value.casefold())
        except (AttributeError, ValueError):
            supported = ", ".join(item.value for item in cls)
            raise ValueError(f"Unsupported provider type; expected one of: {supported}") from None


class Capability(str, Enum):
    CHAT = "chat"
    TEMPORARY = "temporary"
    HISTORY = "history"
    RENAME = "rename"
    DELETE = "delete"
    AUTH = "auth"
    ATTACHMENTS = "attachments"
    RESOURCES = "resources"
    PROJECTS = "projects"
    PROJECT_CREATE = "project_create"
    PROJECT_UPDATE = "project_update"
    PROJECT_DELETE = "project_delete"
    PROJECT_MEMBERSHIP = "project_membership"
    MODELS = "models"
    EFFORT = "effort"
    MESSAGE_EDIT = "message_edit"
    MESSAGE_RETRY = "message_retry"
    CHAT_BRANCH = "chat_branch"


class BaseProvider:
    """Shared account and provider identity for sync and async clients."""

    provider_type: ClassVar[ProviderType | None] = None

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
    ) -> None:
        from .provider import ProviderType

        account_type = ProviderType.parse(account.provider)
        expected_type = self.provider_type
        requested_type = ProviderType.parse(type) if type is not None else expected_type
        selected_type = requested_type or account_type
        if expected_type is not None and selected_type is not expected_type:
            raise ValueError(
                f"{self.__class__.__name__} requires provider type {expected_type.value!r}"
            )
        if selected_type is not account_type:
            raise ValueError("Provider type must match the account provider")
        self.account = account
        self.type = selected_type

    @property
    def provider(self) -> str:
        return self.type.value

    def __repr__(self) -> str:
        return f"<{type(self).__name__} type={self.type.value!r} account={self.account.id!r}>"


@runtime_checkable
class BranchableChatAdapter(Protocol):
    async def branch_chat(self, conversation_id: str, message_id: str) -> Conversation: ...


@runtime_checkable
class MovableChatAdapter(Protocol):
    async def move_chat(self, conversation_id: str, project_id: str | None) -> None: ...


class BaseAttachmentAdapter:
    """Protocol-level attachment upload and download resolution contract."""

    async def upload_file(self, path: Any) -> dict[str, Any]:
        raise FeatureUnavailable(Capability.ATTACHMENTS, "not_implemented")

    async def resolve_download(
        self,
        url: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
    ) -> Any | None:
        return None


class BaseChatAdapter:
    """Protocol-level conversation listing, inspection, and mutation contract."""

    async def list_chats(self, *, cursor: str | None = None, limit: int = 50) -> Any:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    async def get_chat(self, conversation_id: str) -> Conversation:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    async def branch_chat(self, conversation_id: str, message_id: str) -> Conversation:
        raise FeatureUnavailable(Capability.CHAT_BRANCH, "not_implemented")

    async def rename_chat(self, conversation_id: str, title: str) -> None:
        raise FeatureUnavailable(Capability.RENAME, "not_implemented")

    async def delete_chat(self, conversation_id: str) -> None:
        raise FeatureUnavailable(Capability.DELETE, "not_implemented")

    async def move_chat(self, conversation_id: str, project_id: str | None) -> None:
        raise FeatureUnavailable(Capability.PROJECT_MEMBERSHIP, "not_implemented")

    async def conversation_attachment_count(self, conversation_id: str) -> int | None:
        raise FeatureUnavailable(Capability.ATTACHMENTS, "not_implemented")


class BaseMessageAdapter:
    """Protocol-level message completion streaming contract."""

    def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderStreamEvent]:
        raise NotImplementedError


class BaseModelAdapter:
    """Protocol-level model catalog loading contract."""

    async def load_catalog(self, *, force: bool = False) -> ModelCatalog:
        raise FeatureUnavailable(Capability.MODELS, "not_implemented")


class BaseProjectAdapter:
    """Protocol-level project listing, CRUD, resource, and chat listing contract."""

    async def list_projects(self, *, cursor: str | None = None) -> Any:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    async def get_project(self, project_id: str) -> ProjectData:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    async def list_project_chats(self, project_id: str, *, cursor: str | None = None) -> Any:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    async def create_project(
        self,
        name: str,
        instructions: str = "",
        **options: Any,
    ) -> ProjectData:
        raise FeatureUnavailable(Capability.PROJECT_CREATE, "not_implemented")

    async def patch_project(
        self,
        project_id: str,
        *,
        name: str | None = None,
        instructions: str | None = None,
    ) -> ProjectData:
        raise FeatureUnavailable(Capability.PROJECT_UPDATE, "not_implemented")

    async def delete_project(self, project_id: str) -> None:
        raise FeatureUnavailable(Capability.PROJECT_DELETE, "not_implemented")

    async def project_options(self, project_id: str | None = None) -> ProjectOptions:
        from .resources.project.types import ProjectOptions

        return ProjectOptions()

    async def add_project_resource(
        self,
        project_id: str,
        kind: str,
        **values: Any,
    ) -> ProjectData:
        raise FeatureUnavailable(Capability.RESOURCES, "not_implemented")

    async def remove_project_resource(
        self,
        project_id: str,
        resource_id: str,
    ) -> ProjectData:
        raise FeatureUnavailable(Capability.RESOURCES, "not_implemented")

    def invalidate_cache(self, project_id: str) -> None:
        del project_id


class EditorProjectAdapter(BaseProjectAdapter):
    """Shared project mutations backed by a provider editor and concurrency gate."""

    def _get_editor(self) -> Any:
        raise NotImplementedError

    def _get_gate(self) -> asyncio.Lock:
        raise NotImplementedError

    async def _project_after_write(self, project_id: str, confirmed: Any) -> ProjectData:
        self.invalidate_cache(project_id)
        for _ in range(12):
            project = await self.get_project(project_id)
            if confirmed(project):
                return project
            await asyncio.sleep(0.4)
        raise ProviderError(
            "Project change could not be confirmed by the server; sync before retrying"
        )

    async def project_options(self, project_id: str | None = None) -> ProjectOptions:
        from .resources.project.types import ProjectOptions

        editor = self._get_editor()
        if editor is None:
            return ProjectOptions()
        async with self._get_gate():
            return await editor.options(project_id)

    async def patch_project(
        self,
        project_id: str,
        *,
        name: str | None = None,
        instructions: str | None = None,
    ) -> ProjectData:
        from .resources.project.types import ProjectData

        if name is not None and not name.strip():
            raise ValueError("Project name is required")
        current = await self.get_project(project_id)
        changes: dict[str, Any] = {}
        if name is not None and current.name != name:
            changes["name"] = name
        if instructions is not None and (current.instructions or "") != instructions:
            changes["instructions"] = instructions
        if not changes:
            return current
        editor = self._get_editor()
        async with self._get_gate():
            result = await editor.write("update", project_id, current=current, **changes)
        if isinstance(result, ProjectData):
            self.invalidate_cache(project_id)
            return result
        return await self._project_after_write(
            project_id,
            lambda saved: all(
                (getattr(saved, field) or "") == value for field, value in changes.items()
            ),
        )

    async def add_project_resource(
        self,
        project_id: str,
        kind: str,
        **values: Any,
    ) -> ProjectData:
        from .resources.common import ResourceData
        from .resources.project.types import ProjectData

        before = await self.get_project(project_id)
        ids = {resource.id for resource in before.resources}
        editor = self._get_editor()
        async with self._get_gate():
            result = await editor.write(
                "add_source",
                project_id,
                kind=kind,
                existing_resources=before.resources,
                **values,
            )
        self.invalidate_cache(project_id)
        if isinstance(result, ResourceData):
            return replace(before, resources=(*before.resources, result))
        if isinstance(result, ProjectData):
            return result
        return await self._project_after_write(
            project_id,
            lambda project: any(resource.id not in ids for resource in project.resources),
        )

    async def remove_project_resource(
        self,
        project_id: str,
        resource_id: str,
    ) -> ProjectData:
        from .resources.project.types import ProjectData

        before = await self.get_project(project_id)
        source = next(
            (resource for resource in before.resources if resource.id == resource_id),
            None,
        )
        if source is None:
            raise ValueError("Project resource was not found")
        editor = self._get_editor()
        async with self._get_gate():
            result = await editor.write(
                "remove_source",
                project_id,
                resource_id=resource_id,
                resource_name=source.name,
                resource=source,
            )
        if isinstance(result, ProjectData):
            self.invalidate_cache(project_id)
            return result
        return await self._project_after_write(
            project_id,
            lambda project: all(resource.id != resource_id for resource in project.resources),
        )


class BaseSessionAdapter:
    """Protocol-level session setup, authentication, and teardown contract."""

    async def prepare(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def authenticate(self) -> SessionStatus:
        raise FeatureUnavailable(Capability.AUTH, "not_implemented")

    async def session_status(self) -> SessionStatus:
        from .accounts.status import SessionState, SessionStatus

        return SessionStatus(SessionState.UNKNOWN, reason="not_implemented")

    async def forget_session(self) -> SessionStatus:
        raise FeatureUnavailable(Capability.AUTH, "forget_session_not_implemented")


async def confirm_deleted(read: Any, missing: Any = lambda value: False) -> None:
    """Poll a remote resource until removal is confirmed or raise ProviderError."""
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


class BaseProviderAdapter:
    """Composite provider protocol adapter coordinating domain sub-adapters."""

    provider_id: str = ""
    capabilities: frozenset[Capability] = frozenset()

    def __init__(
        self,
        *,
        provider_id: str,
        account_id: str,
        capabilities: frozenset[Capability] | None = None,
        session: BaseSessionAdapter | None = None,
        models: BaseModelAdapter | None = None,
        chats: BaseChatAdapter | None = None,
        projects: BaseProjectAdapter | None = None,
        messages: BaseMessageAdapter | None = None,
        attachments: BaseAttachmentAdapter | None = None,
        transport: AsyncTransport | None = None,
    ) -> None:
        self.provider_id = provider_id
        self.account_id = account_id
        if capabilities is not None:
            self.capabilities = capabilities
        self.session = session or BaseSessionAdapter()
        self.models = models or BaseModelAdapter()
        self.chats = chats or BaseChatAdapter()
        self.projects = projects or BaseProjectAdapter()
        self.messages = messages or BaseMessageAdapter()
        self.attachments = attachments or BaseAttachmentAdapter()
        self._transport = transport

    @property
    def transport(self) -> AsyncTransport | None:
        return self._transport
