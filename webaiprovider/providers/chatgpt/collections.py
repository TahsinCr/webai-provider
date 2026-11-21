from __future__ import annotations

from ...resources.project.collections import AsyncProjectsCollection, ProjectsCollection
from .resources import AsyncChatGPTProject, ChatGPTProject


class AsyncChatGPTProjectsCollection(AsyncProjectsCollection[AsyncChatGPTProject]):
    async def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
        memory_share: bool = True,
    ) -> AsyncChatGPTProject:
        """Create a project with shared default memory or project-only memory."""
        return await self._create_project(title, instruction, memory_share=memory_share)


class ChatGPTProjectsCollection(ProjectsCollection[ChatGPTProject]):
    def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
        memory_share: bool = True,
    ) -> ChatGPTProject:
        """Create a project with shared default memory or project-only memory."""
        return self._create_project(title, instruction, memory_share=memory_share)
