from __future__ import annotations

import os
import uuid
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ...collections import (
    AsyncDerivedCollection,
    AsyncMutableRemoteCollection,
    Page,
    _MutableRemoteCollectionFacade,
    _RemoteCollectionFacade,
    validate_limit,
)
from ...core.files import DownloadSource, download_source
from ...core.hooks import OperationHooks, SyncOperationHooks
from ...core.operations import execute_with_hooks
from ...errors import (
    FeatureUnavailable,
    ProviderError,
    RemoteNotFound,
    UnsupportedOperationError,
)
from ..message.attachment import Attachment
from .project import AsyncProject, Project, _is_project_attachment
from .types import ProjectData as ProviderProject

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider


def _project_from_data(session: AsyncProvider, item: ProviderProject) -> AsyncProject:
    project = session._project_type(
        title=item.name,
        remote_id=item.id,
        session=session,
    )
    project._apply_provider_project(item)
    return project


class AsyncProjectsCollection(AsyncMutableRemoteCollection[AsyncProject]):
    def __init__(self, session: AsyncProvider) -> None:
        super().__init__()
        self.session = session

    async def _emit_refreshed(self, count: int) -> None:
        runtime = getattr(self.session, "runtime", None)
        if runtime is not None:
            await runtime.emit(
                "collection.refreshed",
                payload={"collection": "projects", "count": count},
            )

    async def _fetch_page(
        self,
        cursor: str | None,
        page_limit: int | None,
        **options: Any,
    ) -> Page[AsyncProject]:
        del page_limit, options
        async with self.session.runtime.activity():
            self.session._require_provider()
            page = await self.session._list_projects_data(cursor=cursor)
        if not isinstance(page, Page) or any(
            not isinstance(item, ProviderProject) for item in page.items
        ):
            raise ProviderError("Provider returned an invalid project page")
        return Page(
            tuple(_project_from_data(self.session, item) for item in page.items),
            page.next_cursor,
        )

    async def _fetch_by_id(self, item_id: str) -> AsyncProject | None:
        async with self.session.runtime.activity():
            self.session._require_provider()
            try:
                data = await self.session._get_project_data(item_id)
            except FeatureUnavailable:
                return None
            if not isinstance(data, ProviderProject):
                return None
            return _project_from_data(self.session, data)

    async def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
    ) -> AsyncProject:
        runtime = self.session.runtime
        context = runtime.new_context(
            "project.create",
            parameters={"title": title, "instruction": instruction},
        )

        async def work(ctx) -> AsyncProject:
            project = self.session._project_type(
                title=ctx.parameters["title"], session=self.session
            )
            if ctx.parameters["instruction"] is not None:
                project.instruction._text = ctx.parameters["instruction"]
            async with runtime.activity():
                self.session._require_provider()
                created = await self.session._create_project_data(
                    project.title,
                    instructions=ctx.parameters["instruction"] or "",
                )
            project._apply_provider_project(created)
            return project

        async def on_success(ctx) -> None:
            project: AsyncProject = ctx.result
            await runtime.emit(
                "project.created",
                operation_id=ctx.operation_id,
                resource_id=project.remote_id or project.id,
                payload={"title": project.title},
            )

        return await execute_with_hooks(
            operation_name="project.create",
            context=context,
            hooks_chain=(runtime.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )

    async def update(
        self,
        project: AsyncProject | str,
        *,
        title: str | None = None,
        instruction: str | None = None,
    ) -> AsyncProject:
        if isinstance(project, AsyncProject):
            if project.session is not self.session:
                raise ValueError("Project belongs to a different provider session")
            target = project
        else:
            target = await self.get(project)

        runtime = self.session.runtime
        rid = target.remote_id or target.id
        context = runtime.new_context(
            "project.update",
            resource_id=rid,
            parameters={"target": target, "title": title, "instruction": instruction},
        )

        async def work(ctx) -> AsyncProject:
            new_title = ctx.parameters["title"]
            new_instruction = ctx.parameters["instruction"]
            if target.remote_id:
                async with runtime.activity():
                    self.session._require_provider()
                    updated = await self.session._patch_project_data(
                        target.remote_id, name=new_title, instructions=new_instruction
                    )
                    target._apply_provider_project(updated, replace_resources=False)
            else:
                if new_title is not None:
                    target.title = new_title
                if new_instruction is not None:
                    target.instruction._text = new_instruction
            return target

        async def on_success(ctx) -> None:
            await runtime.emit(
                "project.updated",
                operation_id=ctx.operation_id,
                resource_id=target.remote_id or target.id,
                payload={"title": target.title},
            )

        return await execute_with_hooks(
            operation_name="project.update",
            context=context,
            hooks_chain=(runtime.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )

    async def delete(self, project: AsyncProject | str) -> None:
        if isinstance(project, AsyncProject):
            if project.session is not self.session:
                raise ValueError("Project belongs to a different provider session")
            target = project
        else:
            target = await self.get(project)

        runtime = self.session.runtime
        rid = target.remote_id or target.id
        context = runtime.new_context(
            "project.delete",
            resource_id=rid,
            parameters={"target": target},
        )

        async def work(_ctx) -> None:
            if target.remote_id:
                async with runtime.activity():
                    self.session._require_provider()
                    await self.session._delete_project_data(target.remote_id)

        async def on_success(ctx) -> None:
            await runtime.emit(
                "project.deleted",
                operation_id=ctx.operation_id,
                resource_id=rid,
            )

        await execute_with_hooks(
            operation_name="project.delete",
            context=context,
            hooks_chain=(runtime.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )


class ProjectsCollection(_MutableRemoteCollectionFacade[AsyncProject, Project]):
    def __init__(self, owner: Provider) -> None:
        super().__init__(owner, owner._provider.projects)

    def _wrap(self, item: AsyncProject) -> Project:
        return self._owner._wrap_project(item)

    def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
    ) -> Project:
        project = self._owner._call(self._collection.create(title, instruction=instruction))
        return self._wrap(project)

    def update(
        self,
        project: Project | AsyncProject | str,
        *,
        title: str | None = None,
        instruction: str | None = None,
        **fields: Any,
    ) -> Project:
        if fields:
            raise TypeError(f"Unsupported project fields: {', '.join(fields)}")
        target = project._resource if isinstance(project, Project) else project
        return self._wrap(
            self._owner._call(self._collection.update(target, title=title, instruction=instruction))
        )

    def delete(self, project: Project | AsyncProject | str) -> None:
        target = project._resource if isinstance(project, Project) else project
        self._owner._call(self._collection.delete(target))


def _merge_attachment(prev: Attachment | None, remote: Attachment) -> Attachment:
    raw_size = remote.metadata.get("size_bytes", remote.metadata.get("size"))
    meta_size = raw_size if isinstance(raw_size, int) and not isinstance(raw_size, bool) else None
    resolved_size = remote.size or meta_size or (prev.size if prev is not None else None) or None
    if prev is None:
        return Attachment(
            id=remote.id,
            name=remote.name,
            media_type=remote.media_type,
            remote_url=remote.remote_url,
            path=remote.path,
            content=remote.content,
            size=resolved_size,
            metadata=dict(remote.metadata),
        )
    return Attachment(
        id=remote.id,
        name=remote.name or prev.name,
        media_type=remote.media_type or prev.media_type,
        remote_url=remote.remote_url or prev.remote_url,
        path=remote.path or prev.path,
        content=remote.content if remote.content is not None else prev.content,
        size=resolved_size,
        metadata={**prev.metadata, **remote.metadata},
    )


class AsyncProjectAttachments(AsyncDerivedCollection[Attachment]):
    """Unified file and text attachment collection for a project."""

    def __init__(self, project: AsyncProject) -> None:
        super().__init__()
        self.project = project
        self._items: list[Attachment] = []
        self.hooks = OperationHooks()

    def _sync_from_resources(self, resources: list[Attachment]) -> None:
        existing = {att.id: att for att in self._items}
        self._items = [_merge_attachment(existing.get(att.id), att) for att in resources]

    async def _load_attachments(self, *, emit_event: bool = False) -> list[Attachment]:
        session = self.project.session
        if self.project.remote_id and session is not None and session.runtime.opened:
            async with session.runtime.activity():
                session._require_provider()
                remote = await session._get_project_data(self.project.remote_id)
            if not isinstance(remote, ProviderProject):
                raise ProviderError("Provider returned an invalid project")
            self.project._apply_provider_project(remote, replace_resources=True)
        if emit_event and session is not None:
            await session.runtime.emit(
                "collection.refreshed",
                resource_id=self.project.remote_id or self.project.id,
                payload={"collection": "project.attachments", "count": len(self._items)},
            )
        return list(self._items)

    async def list(self, *, limit: int | None = 50, **_options: Any) -> list[Attachment]:
        validated = validate_limit(limit)
        if validated == 0:
            return []
        attachments = await self._load_attachments(emit_event=False)
        return attachments if validated is None else attachments[:validated]

    async def refresh(self, *, limit: int | None = None, **_options: Any) -> list[Attachment]:
        validated = validate_limit(limit)
        if validated == 0:
            return []
        attachments = await self._load_attachments(emit_event=True)
        return attachments if validated is None else attachments[:validated]

    def _coerce_add_input(
        self,
        item: Attachment | str | bytes | os.PathLike[str],
        resource_type: str | None,
        name: str | None,
        mime_type: str | None,
        options: dict[str, Any],
    ) -> tuple[str, Attachment, dict[str, Any]]:
        resolved_name = name or options.get("title")
        if isinstance(item, Attachment):
            kind = resource_type or str(
                item.metadata.get("kind")
                or ("text" if item.path is None and item.content is not None else "file")
            )
            if kind == "file":
                if item.path is None:
                    raise ValueError("Project attachment uploads require a local file path")
                send_options: dict[str, Any] = {"path": os.fspath(item.path)}
            elif kind == "text":
                if not isinstance(item.content, str):
                    raise ValueError("Text project attachments require string content")
                send_options = {"name": resolved_name or item.name or "Text", "text": item.content}
            else:
                raise UnsupportedOperationError(f"Project attachment type is unsupported: {kind}")
            local = Attachment(
                id=item.id or str(uuid.uuid4()),
                name=resolved_name or item.name,
                media_type=mime_type or item.media_type,
                remote_url=item.remote_url,
                path=item.path,
                content=item.content,
                size=item.size or None,
                metadata={**item.metadata, **options, "kind": kind},
            )
            return kind, local, send_options

        if isinstance(item, bytes):
            raise UnsupportedOperationError("Project attachment type is unsupported: bytes")

        kind = resource_type or "file"
        if kind == "text":
            if not isinstance(item, str):
                raise ValueError("Text project attachments require string content")
            att_name = str(resolved_name or "Text")
            local = Attachment.from_text(
                item,
                att_name,
                mime_type=mime_type or "text/plain",
                metadata={**options, "kind": "text"},
            )
            return kind, local, {"name": att_name, "text": item}
        if kind == "file":
            if not isinstance(item, (str, os.PathLike)):
                raise ValueError("File attachments require a local path")
            raw_path = os.fspath(item)
            local = Attachment(
                id=str(uuid.uuid4()),
                name=str(resolved_name or Path(raw_path).name),
                mime_type=mime_type,
                path=raw_path,
                metadata={**options, "kind": "file"},
            )
            return kind, local, {"path": raw_path}
        raise UnsupportedOperationError(f"Project attachment type is unsupported: {kind}")

    async def create(
        self,
        file_or_data: Attachment | str | bytes | os.PathLike[str],
        resource_type: str | None = None,
        *,
        name: str | None = None,
        mime_type: str | None = None,
        **options: Any,
    ) -> Attachment:
        return await self.add(
            file_or_data,
            resource_type,
            name=name,
            mime_type=mime_type,
            **options,
        )

    async def add(
        self,
        file_or_data: Attachment | str | bytes | os.PathLike[str],
        resource_type: str | None = None,
        *,
        name: str | None = None,
        mime_type: str | None = None,
        **options: Any,
    ) -> Attachment:
        session = self.project.session
        runtime = session.runtime if session is not None else None
        raw_options = dict(options)
        kind, local, send_options = self._coerce_add_input(
            file_or_data, resource_type, name, mime_type, raw_options
        )

        async def perform_add() -> Attachment:
            result = local
            if self.project.remote_id:
                if session is None:
                    raise ProviderError("Project is not attached to an open provider")
                known_ids = (
                    {r.id for r in self.project._provider_project.resources}
                    if self.project._provider_project is not None
                    else {att.id for att in self._items}
                )
                async with session.runtime.activity():
                    session._require_provider()
                    updated = await session._add_project_resource_data(
                        self.project.remote_id, kind, **send_options
                    )
                if not isinstance(updated, ProviderProject):
                    raise ProviderError("Provider returned an invalid project")
                added = [r for r in updated.resources if r.id not in known_ids]
                expected_name = send_options.get("name") or os.path.basename(
                    send_options.get("path", "")
                )
                matching = [r for r in added if r.name == expected_name]
                resource = matching[-1] if matching else added[0] if len(added) == 1 else None
                if resource is None:
                    raise ProviderError("Provider did not confirm the created project attachment")
                result = _merge_attachment(local, resource)
                result.metadata.setdefault("kind", kind)
                self.project._apply_provider_project(updated, replace_resources=True)
                replaced = False
                for idx, entry in enumerate(self._items):
                    if entry.id == result.id:
                        self._items[idx] = result
                        replaced = True
                        break
                if not replaced and _is_project_attachment(result):
                    self._items.append(result)
            else:
                self._items.append(result)
            return result

        if runtime is None:
            return await perform_add()

        context = runtime.new_context(
            "project.attachment.add",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"kind": kind, "options": raw_options},
        )

        async def on_success(ctx) -> None:
            added_att: Attachment = ctx.result
            await runtime.emit(
                "project.attachment.added",
                operation_id=ctx.operation_id,
                resource_id=self.project.remote_id or self.project.id,
                payload={
                    "attachment_id": added_att.id,
                    "kind": added_att.metadata.get("kind", kind),
                },
            )

        return await execute_with_hooks(
            operation_name="project.attachment.add",
            context=context,
            hooks_chain=(runtime.hooks, self.hooks),
            work=lambda _ctx: perform_add(),
            on_success_event=on_success,
        )

    async def remove(self, attachment: Attachment | str) -> bool:
        file_id = attachment.id if isinstance(attachment, Attachment) else str(attachment)
        await self._load_attachments(emit_event=False)
        session = self.project.session
        runtime = session.runtime if session is not None else None
        if not any(att.id == file_id for att in self._items):
            raise RemoteNotFound(f"Project attachment was not found: {file_id}")

        async def perform_remove() -> bool:
            for index, item in enumerate(self._items):
                if item.id != file_id:
                    continue
                if self.project.remote_id:
                    if session is None:
                        raise ProviderError("Project is not attached to an open provider")
                    async with session.runtime.activity():
                        session._require_provider()
                        updated = await session._remove_project_resource_data(
                            self.project.remote_id, file_id
                        )
                    if isinstance(updated, ProviderProject):
                        self.project._provider_project = updated
                    elif isinstance(self.project._provider_project, ProviderProject):
                        self.project._provider_project = replace(
                            self.project._provider_project,
                            resources=tuple(
                                r
                                for r in self.project._provider_project.resources
                                if r.id != file_id
                            ),
                        )
                del self._items[index]
                return True
            return False

        if runtime is None:
            return await perform_remove()

        context = runtime.new_context(
            "project.attachment.remove",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"attachment_id": file_id},
        )

        async def on_success(ctx) -> None:
            if ctx.result:
                await runtime.emit(
                    "project.attachment.removed",
                    operation_id=ctx.operation_id,
                    resource_id=self.project.remote_id or self.project.id,
                    payload={"attachment_id": file_id},
                )

        return await execute_with_hooks(
            operation_name="project.attachment.remove",
            context=context,
            hooks_chain=(runtime.hooks, self.hooks),
            work=lambda _ctx: perform_remove(),
            on_success_event=on_success,
        )

    async def delete(self, attachment: Attachment | str) -> None:
        await self.remove(attachment)

    async def update(self, attachment: Attachment | str, **options: Any) -> Attachment:
        att_id = attachment.id if isinstance(attachment, Attachment) else str(attachment)
        target = await self.get(att_id)
        if self.project.session is not None and self.project.remote_id:
            raise UnsupportedOperationError("Updating remote project attachments is not supported")
        updated = Attachment(
            id=target.id,
            name=str(options.get("name", target.name)),
            media_type=options.get("mime_type", options.get("media_type", target.media_type)),
            remote_url=options.get("remote_url", target.remote_url),
            path=options.get("path", target.path),
            content=options.get("content", target.content),
            metadata={
                **target.metadata,
                **{
                    k: v
                    for k, v in options.items()
                    if k not in {"name", "mime_type", "media_type", "remote_url", "path", "content"}
                },
            },
        )
        for index, existing in enumerate(self._items):
            if existing.id == updated.id:
                self._items[index] = updated
                break
        return updated

    async def download(self, attachment: Attachment | str) -> Any:
        file_id = attachment.id if isinstance(attachment, Attachment) else str(attachment)
        item = await self.get(file_id)
        session = self.project.session
        if item.remote_url and self.project.remote_id:
            if session is None:
                raise ProviderError("Project is not attached to an open provider")
            async with session.runtime.activity():
                session._require_provider()
                source: DownloadSource | None = await session._resolve_download_data(
                    item.remote_url
                )
                return await download_source(source)
        if item.content is not None:
            return item.content
        if item.path is not None:
            return item.path.read_bytes()
        return b""


class ProjectAttachments(_RemoteCollectionFacade[Attachment, Attachment]):
    def __init__(self, owner: Provider, project: AsyncProject) -> None:
        super().__init__(owner, project.attachments)
        self.hooks = SyncOperationHooks(project.attachments.hooks)

    def create(
        self,
        file_or_data: Attachment | str | bytes | os.PathLike[str],
        resource_type: str | None = None,
        *,
        name: str | None = None,
        mime_type: str | None = None,
        **options: Any,
    ) -> Attachment:
        return self._owner._call(
            self._collection.create(
                file_or_data,
                resource_type,
                name=name,
                mime_type=mime_type,
                **options,
            )
        )

    def add(
        self,
        file_or_data: Attachment | str | bytes | os.PathLike[str],
        resource_type: str | None = None,
        *,
        name: str | None = None,
        mime_type: str | None = None,
        **options: Any,
    ) -> Attachment:
        return self._owner._call(
            self._collection.add(
                file_or_data,
                resource_type,
                name=name,
                mime_type=mime_type,
                **options,
            )
        )

    def update(self, attachment: Attachment | str, **options: Any) -> Attachment:
        return self._owner._call(self._collection.update(attachment, **options))

    def remove(self, attachment: Attachment | str) -> bool:
        return self._owner._call(self._collection.remove(attachment))

    def delete(self, attachment: Attachment | str) -> None:
        self._owner._call(self._collection.delete(attachment))

    def download(self, attachment: Attachment | str) -> Any:
        return self._owner._call(self._collection.download(attachment))
