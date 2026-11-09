from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING, Any

from ...collections import AsyncManageableCollection, QueryableCollection, _CollectionFacade
from ...errors import ProviderError, UnsupportedOperationError
from ...models import ProjectData as ProviderProject
from ...models import ResourceData as ProviderResource
from ...utils.files import DownloadSource, download_source

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider


class AsyncProjectInstruction:
    def __init__(self, project: AsyncProject) -> None:
        self.project = project
        self._text: str = ""

    async def get(self) -> str:
        return self._text

    async def edit(self, text: str) -> str:
        session = self.project.session
        if self.project.remote_id:
            if session is None:
                raise ProviderError("Project is not attached to an open provider")
            await session.projects.update(self.project, instruction=text)
        else:
            self._text = text
        return self._text

    async def clear(self) -> None:
        await self.edit("")


class AsyncProjectResources(AsyncManageableCollection[dict[str, Any]]):
    def __init__(self, project: AsyncProject) -> None:
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
            kind = item["type"]
            data = item.get("data")
            if kind == "text" and isinstance(data, str):
                options = {
                    "name": item.get("name") or item.get("title") or "Text",
                    "text": data,
                }
            elif kind == "file":
                path = item.get("path") or data
                if not isinstance(path, (str, os.PathLike)):
                    raise ValueError("File resources require a local path")
                options = {"path": os.fspath(path)}
            else:
                raise UnsupportedOperationError(f"Project resource type is unsupported: {kind}")

            known_ids = {record.get("id") for record in self._items}
            updated = await session._require_provider().add_project_resource(
                self.project.remote_id, kind, **options
            )
            if not isinstance(updated, ProviderProject):
                raise ProviderError("Provider returned an invalid project")
            added = [resource for resource in updated.resources if resource.id not in known_ids]
            expected_name = options.get("name") or os.path.basename(options.get("path", ""))
            matching = [resource for resource in added if resource.name == expected_name]
            resource = matching[-1] if matching else added[0] if len(added) == 1 else None
            if resource is None:
                raise ProviderError("Provider did not confirm the created project resource")
            existing = {record.get("id"): record for record in self._items}
            self._items = [
                {**existing.get(remote.id, {}), **_resource_record(remote)}
                for remote in updated.resources
                if remote.id != resource.id
            ]
            item = {
                **item,
                "id": resource.id,
                "name": resource.name,
                "media_type": resource.media_type,
                "url": resource.remote_url,
                "metadata": dict(resource.metadata),
            }
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
                await session._require_provider().remove_project_resource(
                    self.project.remote_id, resource_id
                )
            del self._items[index]
            return True
        return False

    async def delete(self, target: dict[str, Any] | str) -> None:
        resource_id = (
            QueryableCollection._extract_id(target) if not isinstance(target, str) else target
        )
        await self.remove(resource_id)

    async def update(
        self,
        resource: dict[str, Any] | str,
        *,
        data: Any = None,
        **options: Any,
    ) -> dict[str, Any]:
        resource_id = (
            resource
            if isinstance(resource, str)
            else QueryableCollection._extract_id(resource)
        )
        target = await self.get(resource_id)
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
        item = await self.get(resource_id)
        url = item.get("url") or item.get("download_url")
        session = self.project.session
        if url and self.project.remote_id:
            if session is None:
                raise ProviderError("Project is not attached to an open provider")
            source: DownloadSource | None = await session._require_provider().resolve_download(url)
            return await download_source(source)
        for key in ("bytes", "content", "data"):
            if key in item:
                return item[key]
        return item


class AsyncProject:
    def __init__(
        self,
        id: str | None = None,
        title: str = "",
        *,
        session: AsyncProvider | None = None,
        remote_id: str | None = None,
    ) -> None:
        self.id = id or str(uuid.uuid4())
        self.title = title
        self.session = session
        self.remote_id = remote_id
        self.instruction = AsyncProjectInstruction(self)
        self.resources = AsyncProjectResources(self)
        from .chats import AsyncProjectChatsCollection

        self.chats = AsyncProjectChatsCollection(self)

    def _apply_provider_project(
        self,
        project: ProviderProject,
        *,
        replace_resources: bool = True,
    ) -> None:
        if not isinstance(project, ProviderProject):
            raise ProviderError("Provider returned an invalid project")
        self.remote_id = project.origin.remote_id
        self.title = project.name
        self.instruction._text = project.instructions or ""
        if replace_resources:
            self.resources._items = [_resource_record(resource) for resource in project.resources]

    async def rename(self, title: str) -> None:
        if self.remote_id:
            if self.session is None:
                raise ProviderError("Project is not attached to an open provider")
            await self.session.projects.update(self, title=title)
        else:
            self.title = title

    async def delete(self) -> None:
        session = self.session
        if self.remote_id:
            if session is None:
                raise ProviderError("Project is not attached to an open provider")
            await session._require_provider().delete_project(self.remote_id)
        if session is not None:
            session.projects._items = [item for item in session.projects._items if item is not self]

    def __repr__(self) -> str:
        return f"<Project id={self.id!r} title={self.title!r} remote_id={self.remote_id!r}>"


def _resource_record(resource: ProviderResource) -> dict[str, Any]:
    return {
        "id": resource.id,
        "type": resource.metadata.get("kind", "file"),
        "name": resource.name,
        "media_type": resource.media_type,
        "url": resource.remote_url,
        "metadata": dict(resource.metadata),
    }


class Project:
    def __init__(self, owner: Provider, resource: AsyncProject) -> None:
        self._owner = owner
        self._resource = resource
        self.instruction = ProjectInstruction(owner, resource)
        self.resources = ProjectResources(owner, resource)
        from .chats import ProjectChatsCollection

        self.chats = ProjectChatsCollection(owner, resource)

    @property
    def id(self) -> str:
        return self._resource.id

    @property
    def remote_id(self) -> str | None:
        return self._resource.remote_id

    @property
    def title(self) -> str:
        return self._resource.title

    def rename(self, title: str) -> None:
        self._owner._call(self._resource.rename(title))

    def delete(self) -> None:
        self._owner._call(self._resource.delete())

    def __repr__(self) -> str:
        return f"<Project id={self.id!r} title={self.title!r} remote_id={self.remote_id!r}>"


class ProjectInstruction:
    def __init__(self, owner: Provider, project: AsyncProject) -> None:
        self._owner = owner
        self._instruction = project.instruction

    def get(self) -> str:
        return self._owner._call(self._instruction.get())

    def edit(self, text: str) -> str:
        return self._owner._call(self._instruction.edit(text))

    def clear(self) -> None:
        self._owner._call(self._instruction.clear())


class ProjectResources(_CollectionFacade[dict[str, Any], dict[str, Any]]):
    def __init__(self, owner: Provider, project: AsyncProject) -> None:
        super().__init__(owner, project.resources)

    def create(self, data: Any, resource_type: str = "custom", **options: Any) -> dict[str, Any]:
        return self._owner._call(
            self._collection.create(data, resource_type=resource_type, **options)
        )

    def add(self, data: Any, resource_type: str = "custom", **options: Any) -> dict[str, Any]:
        return self._owner._call(self._collection.add(data, resource_type=resource_type, **options))

    def remove(self, resource_id: str) -> bool:
        return self._owner._call(self._collection.remove(resource_id))

    def delete(self, target: dict[str, Any] | str) -> None:
        self._owner._call(self._collection.delete(target))

    def update(
        self, resource: dict[str, Any] | str, *, data: Any = None, **options: Any
    ) -> dict[str, Any]:
        return self._owner._call(self._collection.update(resource, data=data, **options))

    def download(self, resource_id: str) -> Any:
        return self._owner._call(self._collection.download(resource_id))
