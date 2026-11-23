from __future__ import annotations

import uuid
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from ...errors import WebAIProviderError
from .types import ProjectData as ProviderProject

if TYPE_CHECKING:
    from ...async_provider import AsyncWebAIProvider
    from ...provider import WebAIProvider
    from ..message.attachment import Attachment


def _is_project_attachment(resource: Attachment) -> bool:
    return str(resource.metadata.get("kind", "file")) in ("file", "text")


class _BaseProject:
    id: str
    title: str
    provider: Any
    remote_id: str | None
    _provider_project: ProviderProject | None
    instruction: Any
    attachments: Any
    chats: Any

    def _apply_provider_project(
        self,
        project: ProviderProject,
        *,
        replace_resources: bool = True,
    ) -> None:
        if not isinstance(project, ProviderProject):
            raise WebAIProviderError("Provider returned an invalid project")
        if replace_resources or self._provider_project is None:
            self._provider_project = project
        else:
            self._provider_project = replace(project, resources=self._provider_project.resources)
        self.remote_id = project.id
        self.title = project.name
        self.instruction._text = project.instructions or ""
        if replace_resources:
            self.attachments._sync_from_resources(
                [res for res in project.resources if _is_project_attachment(res)]
            )

    def __repr__(self) -> str:
        return f"<Project id={self.id!r} title={self.title!r} remote_id={self.remote_id!r}>"


class AsyncProjectInstruction:
    def __init__(self, project: AsyncProject) -> None:
        self.project = project
        self._text: str = ""

    async def get(self) -> str:
        provider = self.project.provider
        if self.project.remote_id and provider is not None and provider.opened:
            remote = await provider._get_project_data(self.project.remote_id)
            self.project._apply_provider_project(remote, replace_resources=False)
        return self._text

    async def edit(self, text: str) -> str:
        provider = self.project.provider
        if self.project.remote_id:
            if provider is None:
                raise WebAIProviderError("Project is not attached to an open provider")
            await provider.projects.update(self.project, instruction=text)
        else:
            self._text = text
        return self._text

    async def clear(self) -> None:
        await self.edit("")


class AsyncProject(_BaseProject):
    def __init__(
        self,
        id: str | None = None,
        title: str = "",
        *,
        provider: AsyncWebAIProvider[Any, Any] | None = None,
        remote_id: str | None = None,
    ) -> None:
        self.id = id or str(uuid.uuid4())
        self.title = title
        self.provider = provider
        self.remote_id = remote_id
        self._provider_project: ProviderProject | None = None
        self.instruction = AsyncProjectInstruction(self)
        from .chats import AsyncProjectChatsCollection
        from .collections import AsyncProjectAttachments

        self.attachments = AsyncProjectAttachments(self)
        self.chats = AsyncProjectChatsCollection(self)

    async def rename(self, title: str) -> None:
        if self.remote_id:
            if self.provider is None:
                raise WebAIProviderError("Project is not attached to an open provider")
            await self.provider.projects.update(self, title=title)
        else:
            self.title = title

    async def delete(self) -> None:
        provider = self.provider
        if provider is not None:
            await provider.projects.delete(self)
        elif self.remote_id:
            raise WebAIProviderError("Project is not attached to an open provider")


class ProjectInstruction:
    def __init__(self, project: Project) -> None:
        self.project = project
        self._text: str = ""

    def get(self) -> str:
        provider = self.project.provider
        if self.project.remote_id and provider is not None and provider.opened:
            remote = provider._get_project_data(self.project.remote_id)
            self.project._apply_provider_project(remote, replace_resources=False)
        return self._text

    def edit(self, text: str) -> str:
        provider = self.project.provider
        if self.project.remote_id:
            if provider is None:
                raise WebAIProviderError("Project is not attached to an open provider")
            provider.projects.update(self.project, instruction=text)
        else:
            self._text = text
        return self._text

    def clear(self) -> None:
        self.edit("")


class Project(_BaseProject):
    def __init__(
        self,
        id: str | None = None,
        title: str = "",
        *,
        provider: WebAIProvider[Any, Any] | None = None,
        remote_id: str | None = None,
    ) -> None:
        self.id = id or str(uuid.uuid4())
        self.title = title
        self.provider = provider
        self.remote_id = remote_id
        self._provider_project: ProviderProject | None = None
        self.instruction = ProjectInstruction(self)
        from .chats import ProjectChatsCollection
        from .collections import ProjectAttachments

        self.attachments = ProjectAttachments(self)
        self.chats = ProjectChatsCollection(self)

    def rename(self, title: str) -> None:
        if self.remote_id:
            if self.provider is None:
                raise WebAIProviderError("Project is not attached to an open provider")
            self.provider.projects.update(self, title=title)
        else:
            self.title = title

    def delete(self) -> None:
        provider = self.provider
        if provider is not None:
            provider.projects.delete(self)
        elif self.remote_id:
            raise WebAIProviderError("Project is not attached to an open provider")
