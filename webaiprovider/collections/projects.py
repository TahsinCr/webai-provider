from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..errors import UnsupportedOperationError
from ..resources.project import Project
from .base import ManageableCollection

if TYPE_CHECKING:
    from ..session import ProviderSession


class ProjectsCollection(ManageableCollection[Project]):
    def __init__(self, session: ProviderSession) -> None:
        super().__init__()
        self.session = session

    async def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
        **options: Any,
    ) -> Project:
        project = Project(title=title, session=self.session)
        if instruction is not None:
            project.instruction._text = instruction
        if self.session._bridge is not None:
            await self.session._bridge.create_project(project, instruction=instruction, **options)
        self._items.insert(0, project)
        return project

    async def refresh(self) -> list[Project]:
        if hasattr(self.session, "_bridge") and self.session._bridge:
            projects = await self.session._bridge.list_projects(self.session)
            self._items = list(projects)
        return list(self._items)

    async def update(
        self,
        project: Project | str,
        *,
        title: str | None = None,
        instruction: str | None = None,
        **options: Any,
    ) -> Project:
        if options:
            raise UnsupportedOperationError(
                "Provider-specific project options cannot be updated through this method"
            )
        target = project if isinstance(project, Project) else self.get(project)
        if target.remote_id and self.session._bridge is not None:
            await self.session._bridge.update_project(target, title=title, instructions=instruction)
        else:
            if title is not None:
                target.title = title
            if instruction is not None:
                target.instruction._text = instruction
        return target

    async def delete(self, project: Project | str) -> None:
        target = project if isinstance(project, Project) else self.get(project)
        if target.remote_id and self.session._bridge is not None:
            await self.session._bridge.delete_project(target)
        self._items = [item for item in self._items if item is not target]
