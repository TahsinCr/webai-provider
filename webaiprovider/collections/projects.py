from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from ..errors import ProviderError
from ..models import Page
from ..models import Project as ProviderProject
from ..resources.project import Project
from .base import ManageableCollection

if TYPE_CHECKING:
    from ..async_provider import AsyncProvider


class ProjectsCollection(ManageableCollection[Project]):
    def __init__(self, session: AsyncProvider) -> None:
        super().__init__()
        self.session = session

    async def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
    ) -> Project:
        project = self.session._project_type(title=title, session=self.session)
        if instruction is not None:
            project.instruction._text = instruction
        created = await self.session._require_provider().create_project(
            project.title,
            instructions=instruction or "",
        )
        project._apply_provider_project(created)
        self._items.insert(0, project)
        return project

    async def refresh(self) -> list[Project]:
        page = await self.session._require_provider().list_projects()
        if not isinstance(page, Page) or any(
            not isinstance(item, ProviderProject) for item in page.items
        ):
            raise ProviderError("Provider returned an invalid project page")
        projects = []
        for item in page.items:
            project = self.session._project_type(
                title=item.name,
                remote_id=item.origin.remote_id,
                session=self.session,
            )
            project._apply_provider_project(item)
            projects.append(project)
        self._items = projects
        return list(self._items)

    async def update(
        self,
        project: Project | str,
        *,
        title: str | None = None,
        instruction: str | None = None,
    ) -> Project:
        target = project if isinstance(project, Project) else self.get(project)
        if target.remote_id:
            provider = self.session._require_provider()
            current = await provider.get_project(target.remote_id)
            if not isinstance(current, ProviderProject):
                raise ProviderError("Provider returned an invalid project")
            updated = await provider.update_project(
                replace(
                    current,
                    name=title if title is not None else current.name,
                    instructions=(instruction if instruction is not None else current.instructions),
                )
            )
            target._apply_provider_project(updated, replace_resources=False)
        else:
            if title is not None:
                target.title = title
            if instruction is not None:
                target.instruction._text = instruction
        return target

    async def delete(self, project: Project | str) -> None:
        target = project if isinstance(project, Project) else self.get(project)
        if target.remote_id:
            await self.session._require_provider().delete_project(target.remote_id)
        self._items = [item for item in self._items if item is not target]
