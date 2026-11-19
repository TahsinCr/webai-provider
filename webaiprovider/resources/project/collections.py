from __future__ import annotations

import os
import uuid
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ...collections import (
    AsyncRemoteCollection,
    Page,
    RemoteCollection,
    _publish_collection_change,
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


def _project_from_data(provider: AsyncProvider, item: ProviderProject) -> AsyncProject:
    project = provider._project_type(
        title=item.name,
        remote_id=item.id,
        provider=provider,
    )
    project._apply_provider_project(item)
    return project


class AsyncProjectsCollection(AsyncRemoteCollection[AsyncProject]):
    _collection_name = "projects"

    @property
    def _read_provider(self):
        return self.provider

    def __init__(self, provider: AsyncProvider) -> None:
        self.provider = provider
        self.hooks = OperationHooks()

    async def _fetch_page(
        self,
        cursor: str | None,
        page_limit: int | None,
        **options: Any,
    ) -> Page[AsyncProject]:
        del page_limit, options
        async with self.provider.runtime.activity():
            self.provider._require_provider()
            page = await self.provider._list_projects_data(cursor=cursor)
        if not isinstance(page, Page) or any(
            not isinstance(item, ProviderProject) for item in page.items
        ):
            raise ProviderError("Provider returned an invalid project page")
        return Page(
            tuple(_project_from_data(self.provider, item) for item in page.items),
            page.next_cursor,
        )

    async def _fetch_by_id(self, item_id: str) -> AsyncProject | None:
        async with self.provider.runtime.activity():
            self.provider._require_provider()
            try:
                self._mark_remote_read()
                data = await self.provider._get_project_data(item_id)
            except FeatureUnavailable:
                return None
            if not isinstance(data, ProviderProject):
                return None
            return _project_from_data(self.provider, data)

    async def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
    ) -> AsyncProject:
        return await self._create_project(title, instruction)

    async def _create_project(
        self, title: str, instruction: str | None, **options: Any
    ) -> AsyncProject:
        runtime = self.provider.runtime
        context = runtime.new_context(
            "project.create",
            parameters={"title": title, "instruction": instruction, **options},
        )

        async def work(ctx) -> AsyncProject:
            project = self.provider._project_type(
                title=ctx.parameters["title"], provider=self.provider
            )
            if ctx.parameters["instruction"] is not None:
                project.instruction._text = ctx.parameters["instruction"]
            async with runtime.activity():
                self.provider._require_provider()
                created = await self.provider._create_project_data(
                    project.title,
                    instructions=ctx.parameters["instruction"] or "",
                    **{name: ctx.parameters[name] for name in options},
                )
            project._apply_provider_project(created)
            return project

        async def on_success(ctx) -> None:
            project: AsyncProject = ctx.result
            if project.remote_id:
                await _publish_collection_change(
                    runtime, "projects", "create", project, operation_id=ctx.operation_id
                )
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
            if project.provider is not self.provider:
                raise ValueError("Project belongs to a different provider session")
            target = project
        else:
            target = await self.get(project)

        runtime = self.provider.runtime
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
                    self.provider._require_provider()
                    previous = (target.title, target.instruction._text)
                    updated = await self.provider._patch_project_data(
                        target.remote_id,
                        name=new_title,
                        instructions=new_instruction,
                        current=lambda: target._provider_project,
                    )
                    target._apply_provider_project(updated, replace_resources=False)
                    ctx.metadata["remote_update"] = previous != (
                        target.title,
                        target.instruction._text,
                    )
            else:
                if new_title is not None:
                    target.title = new_title
                if new_instruction is not None:
                    target.instruction._text = new_instruction
            return target

        async def on_success(ctx) -> None:
            if ctx.metadata.get("remote_update"):
                await _publish_collection_change(
                    runtime, "projects", "update", target, operation_id=ctx.operation_id
                )
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
            if project.provider is not self.provider:
                raise ValueError("Project belongs to a different provider session")
            target = project
        else:
            target = await self.get(project)

        runtime = self.provider.runtime
        rid = target.remote_id or target.id
        context = runtime.new_context(
            "project.delete",
            resource_id=rid,
            parameters={"target": target},
        )

        async def work(_ctx) -> None:
            if target.remote_id:
                async with runtime.activity():
                    self.provider._require_provider()
                    await self.provider._delete_project_data(target.remote_id)

        async def on_success(ctx) -> None:
            if target.remote_id:
                await _publish_collection_change(
                    runtime, "projects", "delete", target.remote_id, operation_id=ctx.operation_id
                )
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


class ProjectsCollection(RemoteCollection[AsyncProject, Project]):
    def __init__(self, owner: Provider) -> None:
        super().__init__(owner, owner._provider.projects)
        self.hooks = SyncOperationHooks(self._collection.hooks)

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


def _merge_attachment(
    attachment_type: type[Attachment], prev: Attachment | None, remote: Attachment
) -> Attachment:
    raw_size = remote.metadata.get("size_bytes", remote.metadata.get("size"))
    meta_size = raw_size if isinstance(raw_size, int) and not isinstance(raw_size, bool) else None
    resolved_size = remote.size or meta_size or (prev.size if prev is not None else None) or None
    if prev is None:
        return attachment_type(
            id=remote.id,
            name=remote.name,
            media_type=remote.media_type,
            remote_url=remote.remote_url,
            path=remote.path,
            content=remote.content,
            size=resolved_size,
            metadata=dict(remote.metadata),
        )
    return attachment_type(
        id=remote.id,
        name=remote.name or prev.name,
        media_type=remote.media_type or prev.media_type,
        remote_url=remote.remote_url or prev.remote_url,
        path=remote.path or prev.path,
        content=remote.content if remote.content is not None else prev.content,
        size=resolved_size,
        metadata={**prev.metadata, **remote.metadata},
    )


class AsyncProjectAttachments(AsyncRemoteCollection[Attachment]):
    """Unified file and text attachment collection for a project."""

    _collection_name = "project.attachments"

    @property
    def _read_provider(self):
        return self.project.provider

    @property
    def _scope_id(self):
        return self.project.remote_id

    def __init__(self, project: AsyncProject) -> None:
        super().__init__()
        self.project = project
        self._items: list[Attachment] = []
        self.hooks = OperationHooks()

    @property
    def _attachment_type(self) -> type[Attachment]:
        return getattr(self.project.provider, "_attachment_type", Attachment)

    def _sync_from_resources(self, resources: list[Attachment]) -> None:
        existing = {att.id: att for att in self._items}
        self._items = [
            _merge_attachment(self._attachment_type, existing.get(att.id), att) for att in resources
        ]

    async def _load_attachments(self) -> list[Attachment]:
        provider = self.project.provider
        if self.project.remote_id and provider is not None and provider.runtime.opened:
            async with provider.runtime.activity():
                provider._require_provider()
                self._mark_remote_read()
                remote = await provider._get_project_data(self.project.remote_id)
            if not isinstance(remote, ProviderProject):
                raise ProviderError("Provider returned an invalid project")
            self.project._apply_provider_project(remote, replace_resources=True)
        return list(self._items)

    async def _iter_batches(self, *, limit=None, **options):
        items = await self._load_attachments()
        yield items if limit is None else items[:limit]

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
            local = self._attachment_type(
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
            local = self._attachment_type(
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
        provider = self.project.provider
        runtime = provider.runtime if provider is not None else None
        raw_options = dict(options)
        kind, local, send_options = self._coerce_add_input(
            file_or_data, resource_type, name, mime_type, raw_options
        )

        async def perform_add() -> Attachment:
            result = local
            if self.project.remote_id:
                if provider is None:
                    raise ProviderError("Project is not attached to an open provider")
                known_ids = (
                    {r.id for r in self.project._provider_project.resources}
                    if self.project._provider_project is not None
                    else {att.id for att in self._items}
                )
                async with provider.runtime.activity():
                    provider._require_provider()
                    updated = await provider._add_project_resource_data(
                        self.project.remote_id,
                        kind,
                        current=lambda: self.project._provider_project,
                        **send_options,
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
                result = _merge_attachment(self._attachment_type, local, resource)
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
            if self.project.remote_id:
                await _publish_collection_change(
                    runtime,
                    "project.attachments",
                    "create",
                    added_att,
                    scope_id=self.project.remote_id,
                    operation_id=ctx.operation_id,
                )
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

    async def create_many(
        self,
        *items: Attachment | str | bytes | os.PathLike[str],
    ) -> list[Attachment]:
        if not items:
            return []
        if len(items) == 1:
            return [await self.create(items[0])]
        provider = self.project.provider
        editor = getattr(provider, "editor", None) if provider is not None else None
        batch_add = (
            getattr(editor, "add_sources", None)
            if self.project.remote_id and getattr(editor, "supports_batch_add", False)
            else None
        )
        if not callable(batch_add) or provider is None:
            results: list[Attachment] = []
            for item in items:
                results.append(await self.create(item))
            return results

        coerced = [self._coerce_add_input(item, None, None, None, {}) for item in items]
        runtime = provider.runtime
        context = runtime.new_context(
            "project.attachment.add_many",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"count": len(coerced)},
        )

        async def perform_batch(_ctx) -> list[Attachment]:
            specs = [{"kind": kind, **send_options} for kind, _local, send_options in coerced]
            lock = runtime.project_lock(self.project.remote_id)
            async with runtime.activity():
                async with lock:
                    provider._require_provider()
                    created = await batch_add(self.project.remote_id, specs)
            if not isinstance(created, list) or len(created) != len(coerced):
                raise ProviderError("Provider did not confirm all created project attachments")
            results: list[Attachment] = []
            for (kind, local, _send_options), resource in zip(coerced, created, strict=True):
                merged = _merge_attachment(self._attachment_type, local, resource)
                merged.metadata.setdefault("kind", kind)
                results.append(merged)
                replaced = False
                for idx, entry in enumerate(self._items):
                    if entry.id == merged.id:
                        self._items[idx] = merged
                        replaced = True
                        break
                if not replaced and _is_project_attachment(merged):
                    self._items.append(merged)
            if isinstance(self.project._provider_project, ProviderProject):
                existing = list(self.project._provider_project.resources)
                by_id = {r.id: idx for idx, r in enumerate(existing)}
                for merged in results:
                    if merged.id in by_id:
                        existing[by_id[merged.id]] = merged
                    else:
                        by_id[merged.id] = len(existing)
                        existing.append(merged)
                self.project._provider_project = replace(
                    self.project._provider_project, resources=tuple(existing)
                )
            return results

        async def on_success(ctx) -> None:
            for added_att in ctx.result:
                if self.project.remote_id:
                    await _publish_collection_change(
                        runtime,
                        "project.attachments",
                        "create",
                        added_att,
                        scope_id=self.project.remote_id,
                        operation_id=ctx.operation_id,
                    )
                await runtime.emit(
                    "project.attachment.added",
                    operation_id=ctx.operation_id,
                    resource_id=self.project.remote_id or self.project.id,
                    payload={
                        "attachment_id": added_att.id,
                        "kind": added_att.metadata.get("kind"),
                    },
                )

        return await execute_with_hooks(
            operation_name="project.attachment.add_many",
            context=context,
            hooks_chain=(runtime.hooks, self.hooks),
            work=perform_batch,
            on_success_event=on_success,
        )

    async def delete(self, attachment: Attachment | str) -> None:
        file_id = attachment.id if isinstance(attachment, Attachment) else str(attachment)
        if not any(item.id == file_id for item in self._items):
            await self._load_attachments()
        provider = self.project.provider
        runtime = provider.runtime if provider is not None else None

        async def perform_remove() -> None:
            if not any(item.id == file_id for item in self._items):
                raise RemoteNotFound(f"Project attachment was not found: {file_id}")
            if self.project.remote_id:
                if provider is None:
                    raise ProviderError("Project is not attached to an open provider")
                async with provider.runtime.activity():
                    provider._require_provider()
                    updated = await provider._remove_project_resource_data(
                        self.project.remote_id,
                        file_id,
                        current=lambda: self.project._provider_project,
                    )
                if isinstance(updated, ProviderProject):
                    self.project._provider_project = updated
                elif isinstance(self.project._provider_project, ProviderProject):
                    self.project._provider_project = replace(
                        self.project._provider_project,
                        resources=tuple(
                            r for r in self.project._provider_project.resources if r.id != file_id
                        ),
                    )
            self._items = [item for item in self._items if item.id != file_id]

        if runtime is None:
            await perform_remove()
            return

        context = runtime.new_context(
            "project.attachment.remove",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"attachment_id": file_id},
        )

        async def on_success(ctx) -> None:
            if self.project.remote_id:
                await _publish_collection_change(
                    runtime,
                    "project.attachments",
                    "delete",
                    file_id,
                    scope_id=self.project.remote_id,
                    operation_id=ctx.operation_id,
                )
            await runtime.emit(
                "project.attachment.removed",
                operation_id=ctx.operation_id,
                resource_id=self.project.remote_id or self.project.id,
                payload={"attachment_id": file_id},
            )

        await execute_with_hooks(
            operation_name="project.attachment.remove",
            context=context,
            hooks_chain=(runtime.hooks, self.hooks),
            work=lambda _ctx: perform_remove(),
            on_success_event=on_success,
        )

    async def delete_many(self, *attachments: Attachment | str) -> None:
        if not attachments:
            return
        if len(attachments) == 1:
            await self.delete(attachments[0])
            return
        provider = self.project.provider
        editor = getattr(provider, "editor", None) if provider is not None else None
        batch_remove = (
            getattr(editor, "remove_sources", None)
            if self.project.remote_id and getattr(editor, "supports_batch_remove", False)
            else None
        )
        if not callable(batch_remove) or provider is None:
            for attachment in attachments:
                await self.delete(attachment)
            return

        file_ids = [
            attachment.id if isinstance(attachment, Attachment) else str(attachment)
            for attachment in attachments
        ]
        known = {item.id for item in self._items}
        if any(fid not in known for fid in file_ids):
            await self._load_attachments()
        by_id = {item.id: item for item in self._items}
        missing = [fid for fid in file_ids if fid not in by_id]
        if missing:
            raise RemoteNotFound(f"Project attachment was not found: {missing[0]}")

        runtime = provider.runtime
        context = runtime.new_context(
            "project.attachment.remove_many",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"attachment_ids": tuple(file_ids)},
        )

        async def perform_batch(_ctx) -> None:
            payloads = [{"resource_id": fid, "resource": by_id[fid]} for fid in file_ids]
            lock = runtime.project_lock(self.project.remote_id)
            async with runtime.activity():
                async with lock:
                    provider._require_provider()
                    updated = await batch_remove(
                        self.project.remote_id,
                        payloads,
                        current=self.project._provider_project,
                    )
            removed_set = set(file_ids)
            if isinstance(updated, ProviderProject):
                self.project._provider_project = updated
            elif isinstance(self.project._provider_project, ProviderProject):
                self.project._provider_project = replace(
                    self.project._provider_project,
                    resources=tuple(
                        r
                        for r in self.project._provider_project.resources
                        if r.id not in removed_set
                    ),
                )
            self._items = [item for item in self._items if item.id not in removed_set]

        async def on_success(ctx) -> None:
            for fid in file_ids:
                if self.project.remote_id:
                    await _publish_collection_change(
                        runtime,
                        "project.attachments",
                        "delete",
                        fid,
                        scope_id=self.project.remote_id,
                        operation_id=ctx.operation_id,
                    )
                await runtime.emit(
                    "project.attachment.removed",
                    operation_id=ctx.operation_id,
                    resource_id=self.project.remote_id or self.project.id,
                    payload={"attachment_id": fid},
                )

        await execute_with_hooks(
            operation_name="project.attachment.remove_many",
            context=context,
            hooks_chain=(runtime.hooks, self.hooks),
            work=perform_batch,
            on_success_event=on_success,
        )

    async def update(self, attachment: Attachment | str, **options: Any) -> Attachment:
        att_id = attachment.id if isinstance(attachment, Attachment) else str(attachment)
        target = await self.get(att_id)
        if self.project.provider is not None and self.project.remote_id:
            raise UnsupportedOperationError("Updating remote project attachments is not supported")
        updated = type(target)(
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
        provider = self.project.provider
        if item.remote_url and self.project.remote_id:
            if provider is None:
                raise ProviderError("Project is not attached to an open provider")
            async with provider.runtime.activity():
                provider._require_provider()
                source: DownloadSource | None = await provider._resolve_download_data(
                    item.remote_url
                )
                return await download_source(source)
        if item.content is not None:
            return item.content
        if item.path is not None:
            return item.path.read_bytes()
        return b""


class ProjectAttachments(RemoteCollection[Attachment, Attachment]):
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

    def create_many(
        self,
        *items: Attachment | str | bytes | os.PathLike[str],
    ) -> list[Attachment]:
        return self._owner._call(self._collection.create_many(*items))

    def update(self, attachment: Attachment | str, **options: Any) -> Attachment:
        return self._owner._call(self._collection.update(attachment, **options))

    def delete(self, attachment: Attachment | str) -> None:
        self._owner._call(self._collection.delete(attachment))

    def delete_many(self, *attachments: Attachment | str) -> None:
        self._owner._call(self._collection.delete_many(*attachments))

    def download(self, attachment: Attachment | str) -> Any:
        return self._owner._call(self._collection.download(attachment))
