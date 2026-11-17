from __future__ import annotations

import os
import uuid
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from ...collections import (
    AsyncDerivedCollection,
    AsyncManageableCollection,
    BaseCollection,
    _CollectionFacade,
    _RemoteCollectionFacade,
    validate_limit,
)
from ...core.files import DownloadSource, download_source
from ...core.hooks import OperationHooks, SyncOperationHooks
from ...core.operations import execute_with_hooks
from ...errors import ProviderError, RemoteNotFound, UnsupportedOperationError
from ..message.attachment import Attachment
from .types import ProjectData as ProviderProject

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider


def _resource_record(resource: Attachment) -> dict[str, Any]:
    return {
        "id": resource.id,
        "type": resource.metadata.get("kind", "file"),
        "name": resource.name,
        "media_type": resource.media_type,
        "url": resource.remote_url,
        "metadata": dict(resource.metadata),
    }


def _attachment_from_resource(record: dict[str, Any]) -> Attachment:
    meta = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    raw_size = meta.get("size_bytes", meta.get("size"))
    size = raw_size if isinstance(raw_size, int) and not isinstance(raw_size, bool) else None
    return Attachment(
        id=str(record.get("id") or ""),
        name=str(record.get("name") or "file"),
        mime_type=record.get("media_type"),
        size=size,
    )


class AsyncProjectInstruction:
    def __init__(self, project: AsyncProject) -> None:
        self.project = project
        self._text: str = ""

    async def get(self) -> str:
        session = self.project.session
        if self.project.remote_id and session is not None and session.runtime.opened:
            remote = await session._get_project_data(self.project.remote_id)
            self.project._apply_provider_project(remote, replace_resources=False)
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
        self.hooks = OperationHooks()

    async def _load_resources(self, *, emit_event: bool = False) -> list[dict[str, Any]]:
        session = self.project.session
        if self.project.remote_id and session is not None and session.runtime.opened:
            async with session.runtime.activity():
                session._require_provider()
                remote = await session._get_project_data(self.project.remote_id)
            if not isinstance(remote, ProviderProject):
                raise ProviderError("Provider returned an invalid project")
            existing = {record.get("id"): record for record in self._items}
            self.project._provider_project = remote
            self.project.title = remote.name
            self.project.instruction._text = remote.instructions or ""
            self._items = [
                {**existing.get(res.id, {}), **_resource_record(res)} for res in remote.resources
            ]
        if emit_event and session is not None:
            await session.runtime.emit(
                "collection.refreshed",
                resource_id=self.project.remote_id or self.project.id,
                payload={"collection": "project.resources", "count": len(self._items)},
            )
        return list(self._items)

    async def list(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        validated = validate_limit(limit)
        if validated == 0:
            return []
        items = await self._load_resources(emit_event=False)
        return items if validated is None else items[:validated]

    async def refresh(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        validated = validate_limit(limit)
        if validated == 0:
            return []
        items = await self._load_resources(emit_event=True)
        return items if validated is None else items[:validated]

    async def create(
        self,
        data: Any,
        resource_type: str = "custom",
        **options: Any,
    ) -> dict[str, Any]:
        return await self.add(data, resource_type=resource_type, **options)

    async def add(
        self,
        data: Any,
        resource_type: str = "custom",
        *,
        _extra_hooks: tuple[OperationHooks, ...] = (),
        **options: Any,
    ) -> dict[str, Any]:
        session = self.project.session
        runtime = session.runtime if session is not None else None
        raw_options = dict(options)

        async def perform_add() -> dict[str, Any]:
            item = {
                **raw_options,
                "id": str(uuid.uuid4()),
                "type": resource_type,
                "data": data,
            }
            if self.project.remote_id:
                if session is None:
                    raise ProviderError("Project is not attached to an open provider")
                kind = item["type"]
                payload_data = item.get("data")
                if kind == "text" and isinstance(payload_data, str):
                    send_options = {
                        "name": item.get("name") or item.get("title") or "Text",
                        "text": payload_data,
                    }
                elif kind == "file":
                    path = item.get("path") or payload_data
                    if not isinstance(path, (str, os.PathLike)):
                        raise ValueError("File resources require a local path")
                    send_options = {"path": os.fspath(path)}
                else:
                    raise UnsupportedOperationError(f"Project resource type is unsupported: {kind}")

                known_ids = {record.get("id") for record in self._items}
                async with session.runtime.activity():
                    session._require_provider()
                    updated = await session._add_project_resource_data(
                        self.project.remote_id, kind, **send_options
                    )
                if not isinstance(updated, ProviderProject):
                    raise ProviderError("Provider returned an invalid project")
                added = [resource for resource in updated.resources if resource.id not in known_ids]
                expected_name = send_options.get("name") or os.path.basename(
                    send_options.get("path", "")
                )
                matching = [resource for resource in added if resource.name == expected_name]
                resource = matching[-1] if matching else added[0] if len(added) == 1 else None
                if resource is None:
                    raise ProviderError("Provider did not confirm the created project resource")
                self.project._provider_project = updated
                existing = {record.get("id"): record for record in self._items}
                self._items = [
                    {**existing.get(remote.id, {}), **_resource_record(remote)}
                    for remote in updated.resources
                    if remote.id != resource.id
                ]
                item = {
                    **item,
                    "id": resource.id,
                    "type": resource.metadata.get("kind", kind),
                    "name": resource.name,
                    "media_type": resource.media_type,
                    "url": resource.remote_url,
                    "metadata": dict(resource.metadata),
                }
            self._items.append(item)
            return item

        if runtime is None:
            return await perform_add()

        context = runtime.new_context(
            "project.resource.add",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"resource_type": resource_type, "options": raw_options},
        )

        async def on_success(ctx) -> None:
            added: dict[str, Any] = ctx.result
            await runtime.emit(
                "project.resource.added",
                operation_id=ctx.operation_id,
                resource_id=self.project.remote_id or self.project.id,
                payload={"resource_id": added.get("id"), "kind": added.get("type")},
            )

        return await execute_with_hooks(
            operation_name="project.resource.add",
            context=context,
            hooks_chain=(runtime.hooks, self.hooks, *_extra_hooks),
            work=lambda _ctx: perform_add(),
            on_success_event=on_success,
        )

    async def remove(
        self, resource_id: str, *, _extra_hooks: tuple[OperationHooks, ...] = ()
    ) -> bool:
        await self._load_resources(emit_event=False)
        session = self.project.session
        runtime = session.runtime if session is not None else None

        async def perform_remove() -> bool:
            for index, item in enumerate(self._items):
                if item.get("id") != resource_id:
                    continue
                if self.project.remote_id:
                    if session is None:
                        raise ProviderError("Project is not attached to an open provider")
                    async with session.runtime.activity():
                        session._require_provider()
                        updated = await session._remove_project_resource_data(
                            self.project.remote_id, resource_id
                        )
                    if isinstance(updated, ProviderProject):
                        self.project._provider_project = updated
                    elif isinstance(self.project._provider_project, ProviderProject):
                        self.project._provider_project = replace(
                            self.project._provider_project,
                            resources=tuple(
                                r
                                for r in self.project._provider_project.resources
                                if r.id != resource_id
                            ),
                        )
                del self._items[index]
                return True
            return False

        if runtime is None:
            return await perform_remove()

        context = runtime.new_context(
            "project.resource.remove",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"resource_id": resource_id},
        )

        async def on_success(ctx) -> None:
            if ctx.result:
                await runtime.emit(
                    "project.resource.removed",
                    operation_id=ctx.operation_id,
                    resource_id=self.project.remote_id or self.project.id,
                    payload={"resource_id": resource_id},
                )

        return await execute_with_hooks(
            operation_name="project.resource.remove",
            context=context,
            hooks_chain=(runtime.hooks, self.hooks, *_extra_hooks),
            work=lambda _ctx: perform_remove(),
            on_success_event=on_success,
        )

    async def delete(self, target: dict[str, Any] | str) -> None:
        resource_id = BaseCollection._extract_id(target) if not isinstance(target, str) else target
        await self.remove(resource_id)

    async def update(
        self,
        resource: dict[str, Any] | str,
        *,
        data: Any = None,
        **options: Any,
    ) -> dict[str, Any]:
        resource_id = (
            resource if isinstance(resource, str) else BaseCollection._extract_id(resource)
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
            async with session.runtime.activity():
                session._require_provider()
                source: DownloadSource | None = await session._resolve_download_data(url)
                return await download_source(source)
        for key in ("bytes", "content", "data"):
            if key in item:
                return item[key]
        return item


class AsyncProjectAttachments(AsyncDerivedCollection[Attachment]):
    """File-focused projection over AsyncProjectResources using shared resource storage."""

    def __init__(self, project: AsyncProject) -> None:
        self.project = project
        self.hooks = OperationHooks()

    def _file_attachments(self, records: list[dict[str, Any]]) -> list[Attachment]:
        return [
            _attachment_from_resource(record)
            for record in records
            if record.get("type", "file") == "file"
        ]

    async def list(self, *, limit: int | None = 50, **_options: Any) -> list[Attachment]:
        validated = validate_limit(limit)
        if validated == 0:
            return []
        records = await self.project.resources.list(limit=None)
        attachments = self._file_attachments(records)
        return attachments if validated is None else attachments[:validated]

    async def refresh(self, *, limit: int | None = None, **_options: Any) -> list[Attachment]:
        validated = validate_limit(limit)
        if validated == 0:
            return []
        records = await self.project.resources.refresh(limit=None)
        attachments = self._file_attachments(records)
        return attachments if validated is None else attachments[:validated]

    async def add(
        self, file_or_path: Attachment | str | os.PathLike[str], **options: Any
    ) -> Attachment:
        path = file_or_path.path if isinstance(file_or_path, Attachment) else file_or_path
        if path is None:
            raise ValueError("Project attachment uploads require a local file path")
        record = await self.project.resources.add(
            path, resource_type="file", _extra_hooks=(self.hooks,), **options
        )
        return _attachment_from_resource(record)

    async def remove(self, attachment: Attachment | str) -> bool:
        file_id = attachment.id if isinstance(attachment, Attachment) else str(attachment)
        records = await self.project.resources.list(limit=None)
        if not any(r.get("id") == file_id and r.get("type", "file") == "file" for r in records):
            raise RemoteNotFound(f"Project attachment was not found: {file_id}")
        return await self.project.resources.remove(file_id, _extra_hooks=(self.hooks,))

    async def delete(self, attachment: Attachment | str) -> None:
        await self.remove(attachment)

    async def download(self, attachment: Attachment | str) -> Any:
        file_id = attachment.id if isinstance(attachment, Attachment) else str(attachment)
        return await self.project.resources.download(file_id)


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
        self._provider_project: ProviderProject | None = None
        self.instruction = AsyncProjectInstruction(self)
        self.resources = AsyncProjectResources(self)
        self.attachments = AsyncProjectAttachments(self)
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
        if replace_resources or self._provider_project is None:
            self._provider_project = project
        else:
            self._provider_project = replace(project, resources=self._provider_project.resources)
        self.remote_id = project.id
        self.title = project.name
        self.instruction._text = project.instructions or ""
        if replace_resources:
            existing = {record.get("id"): record for record in self.resources._items}
            self.resources._items = [
                {**existing.get(resource.id, {}), **_resource_record(resource)}
                for resource in project.resources
            ]

    async def rename(self, title: str) -> None:
        if self.remote_id:
            if self.session is None:
                raise ProviderError("Project is not attached to an open provider")
            await self.session.projects.update(self, title=title)
        else:
            self.title = title

    async def delete(self) -> None:
        session = self.session
        if session is not None:
            await session.projects.delete(self)
        elif self.remote_id:
            raise ProviderError("Project is not attached to an open provider")

    def __repr__(self) -> str:
        return f"<Project id={self.id!r} title={self.title!r} remote_id={self.remote_id!r}>"


class Project:
    def __init__(self, owner: Provider, resource: AsyncProject) -> None:
        self._owner = owner
        self._resource = resource
        self.instruction = ProjectInstruction(owner, resource)
        self.resources = ProjectResources(owner, resource)
        self.attachments = ProjectAttachments(owner, resource)
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
        self.hooks = SyncOperationHooks(project.resources.hooks)

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


class ProjectAttachments(_RemoteCollectionFacade[Attachment, Attachment]):
    def __init__(self, owner: Provider, project: AsyncProject) -> None:
        super().__init__(owner, project.attachments)
        self.hooks = SyncOperationHooks(project.attachments.hooks)

    def add(self, file_or_path: Attachment | str | os.PathLike[str], **options: Any) -> Attachment:
        return self._owner._call(self._collection.add(file_or_path, **options))

    def remove(self, attachment: Attachment | str) -> bool:
        return self._owner._call(self._collection.remove(attachment))

    def delete(self, attachment: Attachment | str) -> None:
        self._owner._call(self._collection.delete(attachment))

    def download(self, attachment: Attachment | str) -> Any:
        return self._owner._call(self._collection.download(attachment))
