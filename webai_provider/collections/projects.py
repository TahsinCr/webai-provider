from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..resources.project import Project
from .base import ManageableCollection

if TYPE_CHECKING:
    from ..resources.session import ProviderSession


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
        project = Project(
            title=title,
            session=self.session,
        )
        if instruction:
            project.instruction._text = instruction
        self._items.insert(0, project)
        if hasattr(self.session, "_bridge") and self.session._bridge:
            await self.session._bridge.create_project(project, instruction=instruction, **options)
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
        target = project if isinstance(project, Project) else self.get(project)
        if title is not None and title != target.title:
            target.title = title
            if (
                self.session is not None
                and hasattr(self.session, "_bridge")
                and self.session._bridge
            ):
                await self.session._bridge.rename_project(target, title)
        if instruction is not None:
            await target.instruction.edit(instruction)
        return target

    async def delete(self, project: Project | str) -> None:
        target = project if isinstance(project, Project) else self.get(project)
        await target.delete()
        self._items = [p for p in self._items if p.id != target.id]
