from __future__ import annotations

from typing import TYPE_CHECKING, cast

from ...resources.project.collections import AsyncProjectsCollection
from ...resources.project.project import AsyncProject

if TYPE_CHECKING:
    from .provider import AsyncChatGPTProvider


class AsyncChatGPTProjectsCollection(AsyncProjectsCollection):
    async def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
        memory_share: bool = True,
    ) -> AsyncProject:
        """Create a project with shared default memory or project-only memory."""
        project = self.session._project_type(title=title, session=self.session)
        if instruction is not None:
            project.instruction._text = instruction
        provider = cast("AsyncChatGPTProvider", self.session._require_provider())
        created = await provider.create_project(
            title,
            instructions=instruction or "",
            memory_share=memory_share,
        )
        project._apply_provider_project(created)
        self._items.insert(0, project)
        return project
