from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..resources.project import Project
from .base import ResourceCollection

if TYPE_CHECKING:
    from ..resources.session import ProviderSession


class ProjectsCollection(ResourceCollection[Project]):
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
        project = Project(
            title=title,
            session=self.session,
        )
        if instruction:
            project.instructions._text = instruction
        self._items.insert(0, project)
        if hasattr(self.session, "_bridge") and self.session._bridge:
            await self.session._bridge.create_project(project, instruction=instruction, **options)
        return project

    async def refresh(self) -> list[Project]:
        if hasattr(self.session, "_bridge") and self.session._bridge:
            projects = await self.session._bridge.list_projects(self.session)
            self._items = list(projects)
        return list(self._items)

    async def delete(self, project_or_id: Project | str) -> None:
        project_id = project_or_id.id if isinstance(project_or_id, Project) else project_or_id
        target = self.get(project_id)
        await target.delete()
        self._items = [p for p in self._items if p.id != project_id]
