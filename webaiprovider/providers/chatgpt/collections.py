from __future__ import annotations

from ...resources.project.collections import AsyncProjectsCollection
from .resources import AsyncChatGPTProject


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
