from __future__ import annotations

from ...resources.project.collections import AsyncProjectsCollection
from ...resources.project.project import AsyncProject


class AsyncChatGPTProjectsCollection(AsyncProjectsCollection):
    async def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
        memory_share: bool = True,
    ) -> AsyncProject:
        """Create a project with shared default memory or project-only memory."""
        return await self._create_project(title, instruction, memory_share=memory_share)
