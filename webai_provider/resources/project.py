from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from .attachments import Attachments

if TYPE_CHECKING:
    from .session import ProviderSession


class ProjectInstructions:
    def __init__(self, project: Project) -> None:
        self.project = project
        self._text: str = ""

    async def get(self) -> str:
        return self._text

    async def set(self, text: str) -> None:
        self._text = text
        if (
            self.project.session is not None
            and hasattr(self.project.session, "_bridge")
            and self.project.session._bridge
        ):
            await self.project.session._bridge.update_project_instruction(self.project, text)


class ProjectResources:
    def __init__(self, project: Project) -> None:
        self.project = project
        self._items: list[dict[str, Any]] = []

    async def list(self) -> list[dict[str, Any]]:
        return list(self._items)

    async def add(self, resource_type: str, data: Any, **options: Any) -> dict[str, Any]:
        item = {"id": str(uuid.uuid4()), "type": resource_type, "data": data, **options}
        self._items.append(item)
        if (
            self.project.session is not None
            and hasattr(self.project.session, "_bridge")
            and self.project.session._bridge
        ):
            await self.project.session._bridge.add_project_resource(self.project, item)
        return item

    async def remove(self, resource_id: str) -> bool:
        for i, it in enumerate(self._items):
            if it.get("id") == resource_id:
                del self._items[i]
                if (
                    self.project.session is not None
                    and hasattr(self.project.session, "_bridge")
                    and self.project.session._bridge
                ):
                    await self.project.session._bridge.remove_project_resource(
                        self.project, resource_id
                    )
                return True
        return False


class Project:
    def __init__(
        self,
        id: str | None = None,
        title: str = "",
        *,
        session: ProviderSession | None = None,
        remote_id: str | None = None,
    ) -> None:
        self.id = id or str(uuid.uuid4())
        self.title = title
        self.session = session
        self.remote_id = remote_id
        self.instructions = ProjectInstructions(self)
        self.resources = ProjectResources(self)
        self.attachments = Attachments()

    async def rename(self, title: str) -> Project:
        self.title = title
        if self.session is not None and hasattr(self.session, "_bridge") and self.session._bridge:
            await self.session._bridge.rename_project(self, title)
        return self

    async def delete(self) -> None:
        if self.session is not None and hasattr(self.session, "_bridge") and self.session._bridge:
            await self.session._bridge.delete_project(self)

    async def set_instruction(self, text: str) -> None:
        await self.instructions.set(text)

    async def add_resource(
        self, data: Any, resource_type: str = "custom", **options: Any
    ) -> dict[str, Any]:
        return await self.resources.add(resource_type, data, **options)

    async def remove_resource(self, resource_id: str) -> bool:
        return await self.resources.remove(resource_id)

    def __repr__(self) -> str:
        return f"<Project id={self.id!r} title={self.title!r} remote_id={self.remote_id!r}>"
