from __future__ import annotations

from ...collections.projects import ProjectsCollection
from ...resources.project import Project


class AsyncChatGPTProjectsCollection(ProjectsCollection):
    async def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
        memory_share: bool = True,
    ) -> Project:
        """Create a project with shared default memory or project-only memory."""
        project = self.session._project_type(title=title, session=self.session)
        if instruction is not None:
            project.instruction._text = instruction
        created = await self.session._require_adapter().create_project(
            title,
            instructions=instruction or "",
            memory_share=memory_share,
        )
        project._apply_provider_project(created)
        self._items.insert(0, project)
        return project
