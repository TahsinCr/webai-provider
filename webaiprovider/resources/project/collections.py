from __future__ import annotations

import os
import uuid
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

from ...collections import (
    AsyncRemoteCollection,
    Page,
    RemoteCollection,
    _publish_collection_change,
    _publish_collection_change_sync,
)
from ...core.async_hooks import AsyncOperationHooks
from ...core.hooks import OperationHooks
from ...errors import (
    FeatureUnavailable,
    ProviderError,
    RemoteNotFound,
    UnsupportedOperationError,
)
from ...transport.downloader import DownloadSource, download_source, download_source_sync
from ..message.attachment import Attachment
from ..operations import execute_with_hooks, execute_with_hooks_sync
from .project import AsyncProject, Project, _is_project_attachment
from .types import ProjectData as ProviderProject

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider


_ProjectT = TypeVar("_ProjectT", bound=Project, covariant=True)
_AsyncProjectT = TypeVar("_AsyncProjectT", bound=AsyncProject, covariant=True)


class _BaseProjectsCollection:
    """Shared project construction, page validation, and local mutation helpers."""

    _collection_name = "projects"
    provider: Any
    _build: Callable[..., Any]

    @property
    def _read_provider(self):
        return self.provider

    def _new(self, **fields: Any) -> Any:
        return self._build(provider=self.provider, **fields)

    def _from_data(self, item: ProviderProject) -> Any:
        project = self._new(title=item.name, remote_id=item.id)
        project._apply_provider_project(item)
        return project

    def _build_projects_page(self, page: Any) -> Page[Any]:
        if not isinstance(page, Page) or any(
            not isinstance(item, ProviderProject) for item in page.items
        ):
            raise ProviderError("Provider returned an invalid project page")
        return Page(tuple(self._from_data(item) for item in page.items), page.next_cursor)

    def _check_owned_project(self, project: Any) -> Any:
        if project.provider is not self.provider:
            raise ValueError("Project belongs to a different provider session")
        return project

    def _build_draft_project(self, ctx: Any) -> Any:
        project = self._new(title=ctx.parameters["title"])
        if ctx.parameters["instruction"] is not None:
            project.instruction._text = ctx.parameters["instruction"]
        return project

    @staticmethod
    def _apply_local_project_update(
        target: Any, new_title: str | None, new_instruction: str | None
    ) -> None:
        if new_title is not None:
            target.title = new_title
        if new_instruction is not None:
            target.instruction._text = new_instruction


class AsyncProjectsCollection(_BaseProjectsCollection, AsyncRemoteCollection[_AsyncProjectT]):
    def __init__(
        self, provider: AsyncProvider[Any, Any], build: Callable[..., _AsyncProjectT]
    ) -> None:
        self.provider = provider
        self._build = build
        self.hooks = AsyncOperationHooks()

    async def _fetch_page(
        self,
        cursor: str | None,
        page_limit: int | None,
        **options: Any,
    ) -> Page[_AsyncProjectT]:
        del page_limit, options
        async with self.provider._activity():
            self.provider._require_provider()
            page = await self.provider._list_projects_data(cursor=cursor)
        return self._build_projects_page(page)

    async def _fetch_by_id(self, item_id: str) -> _AsyncProjectT | None:
        async with self.provider._activity():
            self.provider._require_provider()
            try:
                self._mark_remote_read()
                data = await self.provider._get_project_data(item_id)
            except FeatureUnavailable:
                return None
            if not isinstance(data, ProviderProject):
                return None
            return self._from_data(data)

    async def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
    ) -> _AsyncProjectT:
        return await self._create_project(title, instruction)

    async def _create_project(
        self, title: str, instruction: str | None, **options: Any
    ) -> _AsyncProjectT:
        provider = self.provider
        context = provider._new_context(
            "project.create",
            parameters={"title": title, "instruction": instruction, **options},
        )

        async def work(ctx) -> _AsyncProjectT:
            project = self._build_draft_project(ctx)
            async with provider._activity():
                provider._require_provider()
                created = await provider._create_project_data(
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
                    provider, "projects", "create", project, operation_id=ctx.operation_id
                )
            await provider._emit(
                "project.created",
                operation_id=ctx.operation_id,
                resource_id=project.remote_id or project.id,
                payload={"title": project.title},
            )

        return await execute_with_hooks(
            operation_name="project.create",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
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
        target = (
            self._check_owned_project(project)
            if isinstance(project, AsyncProject)
            else await self.get(project)
        )
        provider = self.provider
        rid = target.remote_id or target.id
        context = provider._new_context(
            "project.update",
            resource_id=rid,
            parameters={"target": target, "title": title, "instruction": instruction},
        )

        async def work(ctx) -> AsyncProject:
            new_title = ctx.parameters["title"]
            new_instruction = ctx.parameters["instruction"]
            if target.remote_id:
                async with provider._activity():
                    provider._require_provider()
                    previous = (target.title, target.instruction._text)
                    updated = await provider._patch_project_data(
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
                self._apply_local_project_update(target, new_title, new_instruction)
            return target

        async def on_success(ctx) -> None:
            if ctx.metadata.get("remote_update"):
                await _publish_collection_change(
                    provider, "projects", "update", target, operation_id=ctx.operation_id
                )
            await provider._emit(
                "project.updated",
                operation_id=ctx.operation_id,
                resource_id=target.remote_id or target.id,
                payload={"title": target.title},
            )

        return await execute_with_hooks(
            operation_name="project.update",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )

    async def delete(self, project: AsyncProject | str) -> None:
        target = (
            self._check_owned_project(project)
            if isinstance(project, AsyncProject)
            else await self.get(project)
        )
        provider = self.provider
        rid = target.remote_id or target.id
        context = provider._new_context(
            "project.delete",
            resource_id=rid,
            parameters={"target": target},
        )

        async def work(_ctx) -> None:
            if target.remote_id:
                async with provider._activity(), provider._project_lock(target.remote_id):
                    provider._require_provider()
                    await provider._delete_project_data(target.remote_id)

        async def on_success(ctx) -> None:
            if target.remote_id:
                await _publish_collection_change(
                    provider, "projects", "delete", target.remote_id, operation_id=ctx.operation_id
                )
            await provider._emit(
                "project.deleted",
                operation_id=ctx.operation_id,
                resource_id=rid,
            )

        await execute_with_hooks(
            operation_name="project.delete",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )


class ProjectsCollection(_BaseProjectsCollection, RemoteCollection[_ProjectT]):
    def __init__(self, provider: Provider[Any, Any], build: Callable[..., _ProjectT]) -> None:
        self.provider = provider
        self._build = build
        self.hooks = OperationHooks()

    def _fetch_page(
        self,
        cursor: str | None,
        page_limit: int | None,
        **options: Any,
    ) -> Page[_ProjectT]:
        del page_limit, options
        with self.provider._activity():
            self.provider._require_provider()
            page = self.provider._list_projects_data(cursor=cursor)
        return self._build_projects_page(page)

    def _fetch_by_id(self, item_id: str) -> _ProjectT | None:
        with self.provider._activity():
            self.provider._require_provider()
            try:
                self._mark_remote_read()
                data = self.provider._get_project_data(item_id)
            except FeatureUnavailable:
                return None
            if not isinstance(data, ProviderProject):
                return None
            return self._from_data(data)

    def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
    ) -> _ProjectT:
        return self._create_project(title, instruction)

    def _create_project(self, title: str, instruction: str | None, **options: Any) -> _ProjectT:
        provider = self.provider
        context = provider._new_context(
            "project.create",
            parameters={"title": title, "instruction": instruction, **options},
        )

        def work(ctx) -> _ProjectT:
            project = self._build_draft_project(ctx)
            with provider._activity():
                provider._require_provider()
                created = provider._create_project_data(
                    project.title,
                    instructions=ctx.parameters["instruction"] or "",
                    **{name: ctx.parameters[name] for name in options},
                )
            project._apply_provider_project(created)
            return project

        def on_success(ctx) -> None:
            project: Project = ctx.result
            if project.remote_id:
                _publish_collection_change_sync(
                    provider, "projects", "create", project, operation_id=ctx.operation_id
                )
            provider._emit(
                "project.created",
                operation_id=ctx.operation_id,
                resource_id=project.remote_id or project.id,
                payload={"title": project.title},
            )

        return execute_with_hooks_sync(
            operation_name="project.create",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )

    def update(
        self,
        project: Project | str,
        *,
        title: str | None = None,
        instruction: str | None = None,
        **fields: Any,
    ) -> _ProjectT:
        if fields:
            raise TypeError(f"Unsupported project fields: {', '.join(fields)}")
        target = (
            self._check_owned_project(project)
            if isinstance(project, Project)
            else self.get(project)
        )
        provider = self.provider
        rid = target.remote_id or target.id
        context = provider._new_context(
            "project.update",
            resource_id=rid,
            parameters={"target": target, "title": title, "instruction": instruction},
        )

        def work(ctx) -> Any:
            new_title = ctx.parameters["title"]
            new_instruction = ctx.parameters["instruction"]
            if target.remote_id:
                with provider._activity():
                    provider._require_provider()
                    previous = (target.title, target.instruction._text)
                    updated = provider._patch_project_data(
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
                self._apply_local_project_update(target, new_title, new_instruction)
            return target

        def on_success(ctx) -> None:
            if ctx.metadata.get("remote_update"):
                _publish_collection_change_sync(
                    provider, "projects", "update", target, operation_id=ctx.operation_id
                )
            provider._emit(
                "project.updated",
                operation_id=ctx.operation_id,
                resource_id=target.remote_id or target.id,
                payload={"title": target.title},
            )

        return execute_with_hooks_sync(
            operation_name="project.update",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )

    def delete(self, project: Project | str) -> None:
        target = (
            self._check_owned_project(project)
            if isinstance(project, Project)
            else self.get(project)
        )
        provider = self.provider
        rid = target.remote_id or target.id
        context = provider._new_context(
            "project.delete",
            resource_id=rid,
            parameters={"target": target},
        )

        def work(_ctx) -> None:
            if target.remote_id:
                with provider._activity(), provider._project_lock(target.remote_id):
                    provider._require_provider()
                    provider._delete_project_data(target.remote_id)

        def on_success(ctx) -> None:
            if target.remote_id:
                _publish_collection_change_sync(
                    provider, "projects", "delete", target.remote_id, operation_id=ctx.operation_id
                )
            provider._emit(
                "project.deleted",
                operation_id=ctx.operation_id,
                resource_id=rid,
            )

        execute_with_hooks_sync(
            operation_name="project.delete",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=work,
            on_success_event=on_success,
        )


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


class _BaseProjectAttachments:
    """Shared attachment coercion, merging, and state updates for sync/async project attachments."""

    _collection_name = "project.attachments"
    project: Any
    _items: list[Attachment]

    @property
    def _read_provider(self):
        return self.project.provider

    @property
    def _scope_id(self):
        return self.project.remote_id

    @property
    def _attachment_type(self) -> type[Attachment]:
        return getattr(self.project.provider, "_attachment_type", Attachment)

    def _sync_from_resources(self, resources: list[Attachment]) -> None:
        existing = {att.id: att for att in self._items}
        self._items = [
            _merge_attachment(self._attachment_type, existing.get(att.id), att) for att in resources
        ]

    def _apply_loaded_project(self, remote: Any) -> None:
        if not isinstance(remote, ProviderProject):
            raise ProviderError("Provider returned an invalid project")
        self.project._apply_provider_project(remote, replace_resources=True)

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

    def _store_created(self, attachments: list[Attachment]) -> None:
        by_id = {item.id: item for item in self._items}
        for attachment in attachments:
            if attachment.id in by_id or _is_project_attachment(attachment):
                by_id[attachment.id] = attachment
        self._items = list(by_id.values())

    def _known_resource_ids(self) -> set[str]:
        if self.project._provider_project is not None:
            return {r.id for r in self.project._provider_project.resources}
        return {att.id for att in self._items}

    def _finalize_remote_add(
        self,
        updated: Any,
        *,
        known_ids: set[str],
        kind: str,
        local: Attachment,
        send_options: dict[str, Any],
    ) -> Attachment:
        if not isinstance(updated, ProviderProject):
            raise ProviderError("Provider returned an invalid project")
        added = [r for r in updated.resources if r.id not in known_ids]
        expected_name = send_options.get("name") or os.path.basename(send_options.get("path", ""))
        matching = [r for r in added if r.name == expected_name]
        resource = matching[-1] if matching else added[0] if len(added) == 1 else None
        if resource is None:
            raise ProviderError("Provider did not confirm the created project attachment")
        result = _merge_attachment(self._attachment_type, local, resource)
        result.metadata.setdefault("kind", kind)
        self.project._apply_provider_project(updated, replace_resources=True)
        self._store_created([result])
        return result

    def _batch_editor_method(self, flag_name: str, method_name: str) -> Any:
        provider = self.project.provider
        editor = getattr(provider, "editor", None) if provider is not None else None
        if self.project.remote_id and getattr(editor, flag_name, False):
            method = getattr(editor, method_name, None)
            if callable(method):
                return method
        return None

    def _finalize_batch_add(
        self,
        coerced: list[tuple[str, Attachment, dict[str, Any]]],
        created: Any,
    ) -> list[Attachment]:
        if not isinstance(created, list) or len(created) != len(coerced):
            raise ProviderError("Provider did not confirm all created project attachments")
        results: list[Attachment] = []
        for (kind, local, _send_options), resource in zip(coerced, created, strict=True):
            merged = _merge_attachment(self._attachment_type, local, resource)
            merged.metadata.setdefault("kind", kind)
            results.append(merged)
        self._store_created(results)
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

    def _apply_removed_ids(self, updated: Any, removed_ids: set[str]) -> None:
        if isinstance(updated, ProviderProject):
            self.project._provider_project = updated
        elif isinstance(self.project._provider_project, ProviderProject):
            self.project._provider_project = replace(
                self.project._provider_project,
                resources=tuple(
                    r for r in self.project._provider_project.resources if r.id not in removed_ids
                ),
            )
        self._items = [item for item in self._items if item.id not in removed_ids]

    def _apply_local_attachment_update(
        self, target: Attachment, options: dict[str, Any]
    ) -> Attachment:
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

    @staticmethod
    def _read_local_attachment(item: Attachment) -> Any:
        if item.content is not None:
            return item.content
        if item.path is not None:
            return item.path.read_bytes()
        return b""


class AsyncProjectAttachments(_BaseProjectAttachments, AsyncRemoteCollection[Attachment]):
    """Unified file and text attachment collection for a project."""

    def __init__(self, project: AsyncProject) -> None:
        super().__init__()
        self.project = project
        self._items: list[Attachment] = []
        self.hooks = AsyncOperationHooks()

    async def _load_attachments(self) -> list[Attachment]:
        provider = self.project.provider
        if self.project.remote_id and provider is not None and provider.opened:
            async with provider._activity():
                provider._require_provider()
                self._mark_remote_read()
                remote = await provider._get_project_data(self.project.remote_id)
            self._apply_loaded_project(remote)
        return list(self._items)

    async def _iter_batches(self, *, limit=None, **options):
        del options
        items = await self._load_attachments()
        yield items if limit is None else items[:limit]

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
        raw_options = dict(options)
        kind, local, send_options = self._coerce_add_input(
            file_or_data, resource_type, name, mime_type, raw_options
        )

        async def perform_add() -> Attachment:
            if not self.project.remote_id:
                self._items.append(local)
                return local
            if provider is None:
                raise ProviderError("Project is not attached to an open provider")
            known_ids = self._known_resource_ids()
            async with provider._activity():
                provider._require_provider()
                updated = await provider._add_project_resource_data(
                    self.project.remote_id,
                    kind,
                    current=lambda: self.project._provider_project,
                    **send_options,
                )
            return self._finalize_remote_add(
                updated, known_ids=known_ids, kind=kind, local=local, send_options=send_options
            )

        if provider is None:
            return await perform_add()

        context = provider._new_context(
            "project.attachment.add",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"kind": kind, "options": raw_options},
        )

        async def on_success(ctx) -> None:
            added_att: Attachment = ctx.result
            if self.project.remote_id:
                await _publish_collection_change(
                    provider,
                    "project.attachments",
                    "create",
                    added_att,
                    scope_id=self.project.remote_id,
                    operation_id=ctx.operation_id,
                )
            await provider._emit(
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
            hooks_chain=(provider.hooks, self.hooks),
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
        batch_add = self._batch_editor_method("supports_batch_add", "add_sources")
        if batch_add is None or provider is None:
            return [await self.create(item) for item in items]

        coerced = [self._coerce_add_input(item, None, None, None, {}) for item in items]
        context = provider._new_context(
            "project.attachment.add_many",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"count": len(coerced)},
        )

        async def perform_batch(_ctx) -> list[Attachment]:
            specs = [{"kind": kind, **send_options} for kind, _local, send_options in coerced]
            lock = provider._project_lock(self.project.remote_id)
            async with provider._activity(), lock:
                provider._require_provider()
                created = await batch_add(self.project.remote_id, specs)
            return self._finalize_batch_add(coerced, created)

        async def on_success(ctx) -> None:
            for added_att in ctx.result:
                if self.project.remote_id:
                    await _publish_collection_change(
                        provider,
                        "project.attachments",
                        "create",
                        added_att,
                        scope_id=self.project.remote_id,
                        operation_id=ctx.operation_id,
                    )
                await provider._emit(
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
            hooks_chain=(provider.hooks, self.hooks),
            work=perform_batch,
            on_success_event=on_success,
        )

    async def delete(self, attachment: Attachment | str) -> None:
        file_id = attachment.id if isinstance(attachment, Attachment) else str(attachment)
        if not any(item.id == file_id for item in self._items):
            await self._load_attachments()
        provider = self.project.provider

        async def perform_remove() -> None:
            if not any(item.id == file_id for item in self._items):
                raise RemoteNotFound(f"Project attachment was not found: {file_id}")
            updated = None
            if self.project.remote_id:
                if provider is None:
                    raise ProviderError("Project is not attached to an open provider")
                async with provider._activity():
                    provider._require_provider()
                    updated = await provider._remove_project_resource_data(
                        self.project.remote_id,
                        file_id,
                        current=lambda: self.project._provider_project,
                    )
            self._apply_removed_ids(updated, {file_id})

        if provider is None:
            await perform_remove()
            return

        context = provider._new_context(
            "project.attachment.remove",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"attachment_id": file_id},
        )

        async def on_success(ctx) -> None:
            if self.project.remote_id:
                await _publish_collection_change(
                    provider,
                    "project.attachments",
                    "delete",
                    file_id,
                    scope_id=self.project.remote_id,
                    operation_id=ctx.operation_id,
                )
            await provider._emit(
                "project.attachment.removed",
                operation_id=ctx.operation_id,
                resource_id=self.project.remote_id or self.project.id,
                payload={"attachment_id": file_id},
            )

        await execute_with_hooks(
            operation_name="project.attachment.remove",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
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
        batch_remove = self._batch_editor_method("supports_batch_remove", "remove_sources")
        if batch_remove is None or provider is None:
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

        context = provider._new_context(
            "project.attachment.remove_many",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"attachment_ids": tuple(file_ids)},
        )

        async def perform_batch(_ctx) -> None:
            payloads = [{"resource_id": fid, "resource": by_id[fid]} for fid in file_ids]
            lock = provider._project_lock(self.project.remote_id)
            async with provider._activity(), lock:
                provider._require_provider()
                updated = await batch_remove(
                    self.project.remote_id,
                    payloads,
                    current=self.project._provider_project,
                )
            self._apply_removed_ids(updated, set(file_ids))

        async def on_success(ctx) -> None:
            for fid in file_ids:
                if self.project.remote_id:
                    await _publish_collection_change(
                        provider,
                        "project.attachments",
                        "delete",
                        fid,
                        scope_id=self.project.remote_id,
                        operation_id=ctx.operation_id,
                    )
                await provider._emit(
                    "project.attachment.removed",
                    operation_id=ctx.operation_id,
                    resource_id=self.project.remote_id or self.project.id,
                    payload={"attachment_id": fid},
                )

        await execute_with_hooks(
            operation_name="project.attachment.remove_many",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=perform_batch,
            on_success_event=on_success,
        )

    async def update(self, attachment: Attachment | str, **options: Any) -> Attachment:
        att_id = attachment.id if isinstance(attachment, Attachment) else str(attachment)
        target = await self.get(att_id)
        return self._apply_local_attachment_update(target, options)

    async def download(self, attachment: Attachment | str) -> Any:
        file_id = attachment.id if isinstance(attachment, Attachment) else str(attachment)
        item = await self.get(file_id)
        provider = self.project.provider
        if item.remote_url and self.project.remote_id:
            if provider is None:
                raise ProviderError("Project is not attached to an open provider")
            async with provider._activity():
                provider._require_provider()
                source: DownloadSource | None = await provider._resolve_download_data(
                    item.remote_url
                )
                return await download_source(source)
        return self._read_local_attachment(item)


class ProjectAttachments(_BaseProjectAttachments, RemoteCollection[Attachment]):
    """Blocking file and text attachment collection for a project."""

    def __init__(self, project: Project) -> None:
        super().__init__()
        self.project = project
        self._items: list[Attachment] = []
        self.hooks = OperationHooks()

    def _load_attachments(self) -> list[Attachment]:
        provider = self.project.provider
        if self.project.remote_id and provider is not None and provider.opened:
            with provider._activity():
                provider._require_provider()
                self._mark_remote_read()
                remote = provider._get_project_data(self.project.remote_id)
            self._apply_loaded_project(remote)
        return list(self._items)

    def _iter_batches(self, *, limit=None, **options):
        del options
        items = self._load_attachments()
        yield items if limit is None else items[:limit]

    def create(
        self,
        file_or_data: Attachment | str | bytes | os.PathLike[str],
        resource_type: str | None = None,
        *,
        name: str | None = None,
        mime_type: str | None = None,
        **options: Any,
    ) -> Attachment:
        provider = self.project.provider
        raw_options = dict(options)
        kind, local, send_options = self._coerce_add_input(
            file_or_data, resource_type, name, mime_type, raw_options
        )

        def perform_add() -> Attachment:
            if not self.project.remote_id:
                self._items.append(local)
                return local
            if provider is None:
                raise ProviderError("Project is not attached to an open provider")
            known_ids = self._known_resource_ids()
            with provider._activity():
                provider._require_provider()
                updated = provider._add_project_resource_data(
                    self.project.remote_id,
                    kind,
                    current=lambda: self.project._provider_project,
                    **send_options,
                )
            return self._finalize_remote_add(
                updated, known_ids=known_ids, kind=kind, local=local, send_options=send_options
            )

        if provider is None:
            return perform_add()

        context = provider._new_context(
            "project.attachment.add",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"kind": kind, "options": raw_options},
        )

        def on_success(ctx) -> None:
            added_att: Attachment = ctx.result
            if self.project.remote_id:
                _publish_collection_change_sync(
                    provider,
                    "project.attachments",
                    "create",
                    added_att,
                    scope_id=self.project.remote_id,
                    operation_id=ctx.operation_id,
                )
            provider._emit(
                "project.attachment.added",
                operation_id=ctx.operation_id,
                resource_id=self.project.remote_id or self.project.id,
                payload={
                    "attachment_id": added_att.id,
                    "kind": added_att.metadata.get("kind", kind),
                },
            )

        return execute_with_hooks_sync(
            operation_name="project.attachment.add",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=lambda _ctx: perform_add(),
            on_success_event=on_success,
        )

    def create_many(
        self,
        *items: Attachment | str | bytes | os.PathLike[str],
    ) -> list[Attachment]:
        if not items:
            return []
        if len(items) == 1:
            return [self.create(items[0])]
        provider = self.project.provider
        batch_add = self._batch_editor_method("supports_batch_add", "add_sources")
        if batch_add is None or provider is None:
            return [self.create(item) for item in items]

        coerced = [self._coerce_add_input(item, None, None, None, {}) for item in items]
        context = provider._new_context(
            "project.attachment.add_many",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"count": len(coerced)},
        )

        def perform_batch(_ctx) -> list[Attachment]:
            specs = [{"kind": kind, **send_options} for kind, _local, send_options in coerced]
            lock = provider._project_lock(self.project.remote_id)
            with provider._activity(), lock:
                provider._require_provider()
                created = batch_add(self.project.remote_id, specs)
            return self._finalize_batch_add(coerced, created)

        def on_success(ctx) -> None:
            for added_att in ctx.result:
                if self.project.remote_id:
                    _publish_collection_change_sync(
                        provider,
                        "project.attachments",
                        "create",
                        added_att,
                        scope_id=self.project.remote_id,
                        operation_id=ctx.operation_id,
                    )
                provider._emit(
                    "project.attachment.added",
                    operation_id=ctx.operation_id,
                    resource_id=self.project.remote_id or self.project.id,
                    payload={
                        "attachment_id": added_att.id,
                        "kind": added_att.metadata.get("kind"),
                    },
                )

        return execute_with_hooks_sync(
            operation_name="project.attachment.add_many",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=perform_batch,
            on_success_event=on_success,
        )

    def delete(self, attachment: Attachment | str) -> None:
        file_id = attachment.id if isinstance(attachment, Attachment) else str(attachment)
        if not any(item.id == file_id for item in self._items):
            self._load_attachments()
        provider = self.project.provider

        def perform_remove() -> None:
            if not any(item.id == file_id for item in self._items):
                raise RemoteNotFound(f"Project attachment was not found: {file_id}")
            updated = None
            if self.project.remote_id:
                if provider is None:
                    raise ProviderError("Project is not attached to an open provider")
                with provider._activity():
                    provider._require_provider()
                    updated = provider._remove_project_resource_data(
                        self.project.remote_id,
                        file_id,
                        current=lambda: self.project._provider_project,
                    )
            self._apply_removed_ids(updated, {file_id})

        if provider is None:
            perform_remove()
            return

        context = provider._new_context(
            "project.attachment.remove",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"attachment_id": file_id},
        )

        def on_success(ctx) -> None:
            if self.project.remote_id:
                _publish_collection_change_sync(
                    provider,
                    "project.attachments",
                    "delete",
                    file_id,
                    scope_id=self.project.remote_id,
                    operation_id=ctx.operation_id,
                )
            provider._emit(
                "project.attachment.removed",
                operation_id=ctx.operation_id,
                resource_id=self.project.remote_id or self.project.id,
                payload={"attachment_id": file_id},
            )

        execute_with_hooks_sync(
            operation_name="project.attachment.remove",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=lambda _ctx: perform_remove(),
            on_success_event=on_success,
        )

    def delete_many(self, *attachments: Attachment | str) -> None:
        if not attachments:
            return
        if len(attachments) == 1:
            self.delete(attachments[0])
            return
        provider = self.project.provider
        batch_remove = self._batch_editor_method("supports_batch_remove", "remove_sources")
        if batch_remove is None or provider is None:
            for attachment in attachments:
                self.delete(attachment)
            return

        file_ids = [
            attachment.id if isinstance(attachment, Attachment) else str(attachment)
            for attachment in attachments
        ]
        known = {item.id for item in self._items}
        if any(fid not in known for fid in file_ids):
            self._load_attachments()
        by_id = {item.id: item for item in self._items}
        missing = [fid for fid in file_ids if fid not in by_id]
        if missing:
            raise RemoteNotFound(f"Project attachment was not found: {missing[0]}")

        context = provider._new_context(
            "project.attachment.remove_many",
            resource_id=self.project.remote_id or self.project.id,
            parameters={"attachment_ids": tuple(file_ids)},
        )

        def perform_batch(_ctx) -> None:
            payloads = [{"resource_id": fid, "resource": by_id[fid]} for fid in file_ids]
            lock = provider._project_lock(self.project.remote_id)
            with provider._activity(), lock:
                provider._require_provider()
                updated = batch_remove(
                    self.project.remote_id,
                    payloads,
                    current=self.project._provider_project,
                )
            self._apply_removed_ids(updated, set(file_ids))

        def on_success(ctx) -> None:
            for fid in file_ids:
                if self.project.remote_id:
                    _publish_collection_change_sync(
                        provider,
                        "project.attachments",
                        "delete",
                        fid,
                        scope_id=self.project.remote_id,
                        operation_id=ctx.operation_id,
                    )
                provider._emit(
                    "project.attachment.removed",
                    operation_id=ctx.operation_id,
                    resource_id=self.project.remote_id or self.project.id,
                    payload={"attachment_id": fid},
                )

        execute_with_hooks_sync(
            operation_name="project.attachment.remove_many",
            context=context,
            hooks_chain=(provider.hooks, self.hooks),
            work=perform_batch,
            on_success_event=on_success,
        )

    def update(self, attachment: Attachment | str, **options: Any) -> Attachment:
        att_id = attachment.id if isinstance(attachment, Attachment) else str(attachment)
        target = self.get(att_id)
        return self._apply_local_attachment_update(target, options)

    def download(self, attachment: Attachment | str) -> Any:
        file_id = attachment.id if isinstance(attachment, Attachment) else str(attachment)
        item = self.get(file_id)
        provider = self.project.provider
        if item.remote_url and self.project.remote_id:
            if provider is None:
                raise ProviderError("Project is not attached to an open provider")
            with provider._activity():
                provider._require_provider()
                source: DownloadSource | None = provider._resolve_download_data(item.remote_url)
                return download_source_sync(source)
        return self._read_local_attachment(item)
