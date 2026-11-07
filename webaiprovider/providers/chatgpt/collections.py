from __future__ import annotations

from ...collections.projects import ProjectsCollection
from ...resources.project import Project


class ChatGPTProjectsCollection(ProjectsCollection):
    async def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
        memory_share: bool = True,
    ) -> Project:
        """Create a project with shared default memory or project-only memory."""
        project = Project(title=title, session=self.session)
        if instruction is not None:
            project.instruction._text = instruction
        await self.session._require_bridge().create_chatgpt_project(
            project, instruction=instruction, memory_share=memory_share
        )
        self._items.insert(0, project)
        return project
