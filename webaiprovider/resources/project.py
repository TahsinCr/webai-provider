from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from ..collections.base import ManageableCollection
from ..errors import ProviderError, UnsupportedOperationError

if TYPE_CHECKING:
    from ..session import Provider


class ProjectInstruction:
    def __init__(self, project: Project) -> None:
        self.project = project
        self._text: str = ""

    async def get(self) -> str:
        return self._text

    async def edit(self, text: str) -> str:
        session = self.project.session
        if self.project.remote_id:
            if session is None:
                raise ProviderError("Project is not attached to an open provider")
            await session._require_bridge().update_project(self.project, instructions=text)
        else:
            self._text = text
        return self._text

    async def clear(self) -> None:
        await self.edit("")


class ProjectResources(ManageableCollection[dict[str, Any]]):
    def __init__(self, project: Project) -> None:
        super().__init__()
        self.project = project

    async def create(
        self,
        data: Any,
        resource_type: str = "custom",
        **options: Any,
    ) -> dict[str, Any]:
        return await self.add(data, resource_type=resource_type, **options)

    async def add(self, data: Any, resource_type: str = "custom", **options: Any) -> dict[str, Any]:
        item = {
            **options,
            "id": str(uuid.uuid4()),
            "type": resource_type,
            "data": data,
        }
        session = self.project.session
        if self.project.remote_id:
            if session is None:
                raise ProviderError("Project is not attached to an open provider")
            item = await session._require_bridge().add_project_resource(self.project, item)
        self._items.append(item)
        return item

    async def remove(self, resource_id: str) -> bool:
        for index, item in enumerate(self._items):
            if item.get("id") != resource_id:
                continue
            session = self.project.session
            if self.project.remote_id:
                if session is None:
                    raise ProviderError("Project is not attached to an open provider")
                await session._require_bridge().remove_project_resource(self.project, resource_id)
            del self._items[index]
            return True
        return False

    async def delete(self, target: dict[str, Any] | str) -> None:
        resource_id = self._extract_id(target) if not isinstance(target, str) else target
        await self.remove(resource_id)

    async def update(
        self,
        resource: dict[str, Any] | str,
        *,
        data: Any = None,
        **options: Any,
    ) -> dict[str, Any]:
        resource_id = resource if isinstance(resource, str) else self._extract_id(resource)
        target = self.get(resource_id)
        if self.project.session is not None and self.project.remote_id:
            raise UnsupportedOperationError("Updating remote project resources is not supported")
        updated = dict(target)
        if data is not None:
            updated["data"] = data
        updated.update(options)
        target.clear()
        target.update(updated)
        return target

    async def download(self, resource_id: str) -> Any:
        item = self.get(resource_id)
        url = item.get("url") or item.get("download_url")
        session = self.project.session
        if url and self.project.remote_id:
            if session is None:
                raise ProviderError("Project is not attached to an open provider")
            return await session._require_bridge().download(url)
        for key in ("bytes", "content", "data"):
            if key in item:
                return item[key]
        return item


class Project:
    def __init__(
        self,
        id: str | None = None,
        title: str = "",
        *,
        session: Provider | None = None,
        remote_id: str | None = None,
    ) -> None:
        self.id = id or str(uuid.uuid4())
        self.title = title
        self.session = session
        self.remote_id = remote_id
        self.instruction = ProjectInstruction(self)
        self.resources = ProjectResources(self)
        from ..collections.project_chats import ProjectChatsCollection

        self.chats = ProjectChatsCollection(self)

    async def rename(self, title: str) -> None:
        if self.remote_id:
            if self.session is None:
                raise ProviderError("Project is not attached to an open provider")
            await self.session._require_bridge().update_project(self, title=title)
        else:
            self.title = title

    async def delete(self) -> None:
        session = self.session
        if self.remote_id:
            if session is None:
                raise ProviderError("Project is not attached to an open provider")
            await session._require_bridge().delete_project(self)
        if session is not None:
            session.projects._items = [item for item in session.projects._items if item is not self]

    def __repr__(self) -> str:
        return f"<Project id={self.id!r} title={self.title!r} remote_id={self.remote_id!r}>"
