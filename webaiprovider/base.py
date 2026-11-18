from __future__ import annotations

import asyncio
from contextvars import ContextVar
from dataclasses import replace
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar

from .errors import ProviderError, RemoteNotFound

if TYPE_CHECKING:
    from .accounts.account import Account
    from .resources.project.types import ProjectData, ProjectOptions

_STAGED_PROJECT: ContextVar[ProjectData | None] = ContextVar("_staged_project", default=None)


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


class ProjectMutationsMixin:
    """Shared project mutations backed by a provider editor and concurrency gate."""

    if TYPE_CHECKING:

        async def _get_project_data(self, project_id: str) -> ProjectData: ...

        def _invalidate_project_cache(self, project_id: str) -> None: ...

    def _get_project_editor(self) -> Any:
        raise NotImplementedError

    def _get_project_gate(self) -> asyncio.Lock:
        raise NotImplementedError

    async def _project_after_write(self, project_id: str, confirmed: Any) -> ProjectData:
        self._invalidate_project_cache(project_id)
        for _ in range(12):
            project = await self._get_project_data(project_id)
            if confirmed(project):
                return project
            await asyncio.sleep(0.4)
        raise ProviderError(
            "Project change could not be confirmed by the server; sync before retrying"
        )

    async def _project_options(self, project_id: str | None = None) -> ProjectOptions:
        from .resources.project.types import ProjectOptions

        editor = self._get_project_editor()
        if editor is None:
            return ProjectOptions()
        async with self._get_project_gate():
            return await editor.options(project_id)

    async def _patch_project(
        self,
        project_id: str,
        *,
        name: str | None = None,
        instructions: str | None = None,
        current: ProjectData | None = None,
    ) -> ProjectData:
        from .resources.project.types import ProjectData

        if name is not None and not name.strip():
            raise ValueError("Project name is required")
        staged = current if current is not None else _STAGED_PROJECT.get()
        if staged is None or staged.id != project_id:
            staged = await self._get_project_data(project_id)
        changes: dict[str, Any] = {}
        if name is not None and staged.name != name:
            changes["name"] = name
        if instructions is not None and (staged.instructions or "") != instructions:
            changes["instructions"] = instructions
        if not changes:
            return staged
        editor = self._get_project_editor()
        async with self._get_project_gate():
            result = await editor.write("update", project_id, current=staged, **changes)
        if isinstance(result, ProjectData):
            self._invalidate_project_cache(project_id)
            return result
        return await self._project_after_write(
            project_id,
            lambda saved: all(
                (getattr(saved, field) or "") == value for field, value in changes.items()
            ),
        )

    async def _add_project_resource(
        self,
        project_id: str,
        kind: str,
        *,
        current: ProjectData | None = None,
        **values: Any,
    ) -> ProjectData:
        from .resources.message.attachment import Attachment
        from .resources.project.types import ProjectData

        staged = current if current is not None else _STAGED_PROJECT.get()
        before = (
            staged
            if staged is not None and staged.id == project_id
            else await self._get_project_data(project_id)
        )
        ids = {resource.id for resource in before.resources}
        editor = self._get_project_editor()
        async with self._get_project_gate():
            result = await editor.write(
                "add_source",
                project_id,
                kind=kind,
                existing_resources=before.resources,
                **values,
            )
        self._invalidate_project_cache(project_id)
        if isinstance(result, Attachment):
            return replace(before, resources=(*before.resources, result))
        if isinstance(result, ProjectData):
            return result
        return await self._project_after_write(
            project_id,
            lambda project: any(resource.id not in ids for resource in project.resources),
        )

    async def _remove_project_resource(
        self,
        project_id: str,
        resource_id: str,
        *,
        current: ProjectData | None = None,
    ) -> ProjectData:
        from .resources.project.types import ProjectData

        staged = current if current is not None else _STAGED_PROJECT.get()
        before = (
            staged
            if staged is not None and staged.id == project_id
            else await self._get_project_data(project_id)
        )
        source = next(
            (resource for resource in before.resources if resource.id == resource_id),
            None,
        )
        if source is None:
            raise ValueError("Project resource was not found")
        editor = self._get_project_editor()
        async with self._get_project_gate():
            result = await editor.write(
                "remove_source",
                project_id,
                current=before,
                resource_id=resource_id,
                resource_name=source.name,
                resource=source,
            )
        if isinstance(result, ProjectData):
            self._invalidate_project_cache(project_id)
            return result
        return await self._project_after_write(
            project_id,
            lambda project: all(resource.id != resource_id for resource in project.resources),
        )


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
