from __future__ import annotations

from typing import TYPE_CHECKING

from ..resources.project import Project
from .base import ManageableCollection

if TYPE_CHECKING:
    from ..session import Provider


class ProjectsCollection(ManageableCollection[Project]):
    def __init__(self, session: Provider) -> None:
        super().__init__()
        self.session = session

    async def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
    ) -> Project:
        project = Project(title=title, session=self.session)
        if instruction is not None:
            project.instruction._text = instruction
        await self.session._require_bridge().create_project(project, instruction=instruction)
        self._items.insert(0, project)
        return project

    async def refresh(self) -> list[Project]:
        projects = await self.session._require_bridge().list_projects(self.session)
        self._items = list(projects)
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
            await self.session._require_bridge().update_project(
                target, title=title, instructions=instruction
            )
        else:
            if title is not None:
                target.title = title
            if instruction is not None:
                target.instruction._text = instruction
        return target

    async def delete(self, project: Project | str) -> None:
        target = project if isinstance(project, Project) else self.get(project)
        if target.remote_id:
            await self.session._require_bridge().delete_project(target)
        self._items = [item for item in self._items if item is not target]
