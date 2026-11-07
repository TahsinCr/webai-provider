from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from ..collections.base import ManageableCollection

if TYPE_CHECKING:
    from .session import ProviderSession


class ProjectInstruction:
    def __init__(self, project: Project) -> None:
        self.project = project
        self._text: str = ""

    async def get(self) -> str:
        return self._text

    async def edit(self, text: str) -> str:
        self._text = text
        if (
            self.project.session is not None
            and hasattr(self.project.session, "_bridge")
            and self.project.session._bridge
        ):
            await self.project.session._bridge.update_project_instruction(self.project, text)
        return self._text

    async def clear(self) -> None:
        await self.edit("")


ProjectInstructions = ProjectInstruction


class ProjectResources(ManageableCollection[dict[str, Any]]):
    def __init__(self, project: Project) -> None:
        super().__init__()
        self.project = project

    async def add(self, data: Any, resource_type: str = "custom", **options: Any) -> dict[str, Any]:
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

    async def update(
        self,
        resource: dict[str, Any] | str,
        *,
        data: Any = None,
        **options: Any,
    ) -> dict[str, Any]:
        target = resource if isinstance(resource, dict) else self.get(resource)
        if data is not None:
            target["data"] = data
        target.update(options)
        if (
            self.project.session is not None
            and hasattr(self.project.session, "_bridge")
            and self.project.session._bridge
        ):
            await self.project.session._bridge.add_project_resource(self.project, target)
        return target

    async def download(self, resource_id: str) -> Any:
        item = self.get(resource_id)
        if "bytes" in item:
            return item["bytes"]
        if "content" in item:
            return item["content"]
        if "data" in item:
            return item["data"]
        url = item.get("url") or item.get("download_url")
        if (
            url
            and self.project.session is not None
            and hasattr(self.project.session, "_bridge")
            and self.project.session._bridge
        ):
            return await self.project.session._bridge.download(url)
        return item


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
        self.instruction = ProjectInstruction(self)
        self.instructions = self.instruction
        self.resources = ProjectResources(self)

    async def rename(self, title: str) -> Project:
        if self.session is not None and hasattr(self.session, "projects"):
            return await self.session.projects.update(self, title=title)
        self.title = title
        if self.session is not None and hasattr(self.session, "_bridge") and self.session._bridge:
            await self.session._bridge.rename_project(self, title)
        return self

    async def delete(self) -> None:
        if self.session is not None and hasattr(self.session, "_bridge") and self.session._bridge:
            await self.session._bridge.delete_project(self)

    def __repr__(self) -> str:
        return f"<Project id={self.id!r} title={self.title!r} remote_id={self.remote_id!r}>"
