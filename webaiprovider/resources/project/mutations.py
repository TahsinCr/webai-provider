from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ...errors import ProviderError

if TYPE_CHECKING:
    from .types import ProjectData, ProjectOptions


def _project_patch_changes(
    staged: ProjectData,
    *,
    name: str | None = None,
    instructions: str | None = None,
) -> dict[str, Any]:
    changes: dict[str, Any] = {}
    if name is not None and staged.name != name:
        changes["name"] = name
    if instructions is not None and (staged.instructions or "") != instructions:
        changes["instructions"] = instructions
    return changes


def _patch_confirmed(changes: dict[str, Any]):
    def confirmed(project: ProjectData) -> bool:
        return all((getattr(project, field) or "") == value for field, value in changes.items())

    return confirmed


def _finalize_add_resource(
    self_mixin: Any,
    project_id: str,
    before: ProjectData,
    result: Any,
    values: dict[str, Any],
) -> tuple[ProjectData | None, Any]:
    from ..message.attachment import Attachment
    from .types import ProjectData

    ids = {resource.id for resource in before.resources}
    if isinstance(result, Attachment):
        self_mixin._invalidate_project_cache(project_id)
        if not result.id or result.id in ids:
            raise ProviderError("Provider did not confirm a new project attachment")
        return replace(before, resources=(*before.resources, result)), None
    expected_name = values.get("name") or Path(values["path"]).name

    def confirmed(project: ProjectData) -> bool:
        return (
            sum(
                bool(resource.id) and resource.id not in ids and resource.name == expected_name
                for resource in project.resources
            )
            == 1
        )

    if isinstance(result, ProjectData) and result.id == project_id and confirmed(result):
        self_mixin._invalidate_project_cache(project_id)
        return result, None
    return None, confirmed


def _find_project_resource(before: ProjectData, resource_id: str) -> Any:
    source = next(
        (resource for resource in before.resources if resource.id == resource_id),
        None,
    )
    if source is None:
        raise ValueError("Project resource was not found")
    return source


def _finalize_remove_resource(
    self_mixin: Any,
    project_id: str,
    resource_id: str,
    result: Any,
) -> tuple[ProjectData | None, Any]:
    from .types import ProjectData

    def confirmed(project: ProjectData) -> bool:
        return all(resource.id != resource_id for resource in project.resources)

    if isinstance(result, ProjectData) and result.id == project_id and confirmed(result):
        self_mixin._invalidate_project_cache(project_id)
        return result, None
    return None, confirmed


class ProjectMutationsMixin:
    """Shared project mutations backed by a provider editor and concurrency gate."""

    if TYPE_CHECKING:
        editor: Any
        _gate: asyncio.Lock

        async def _get_project_data(self, project_id: str) -> ProjectData: ...

        def _invalidate_project_cache(self, project_id: str) -> None: ...

    async def _project_after_write(
        self, project_id: str, confirmed: Any, *, read=None
    ) -> ProjectData:
        self._invalidate_project_cache(project_id)
        for attempt in range(12):
            if attempt:
                await asyncio.sleep(0.4)
            project = await (self._get_project_data(project_id) if read is None else read())
            if project.id == project_id and confirmed(project):
                return project
        raise ProviderError(
            "Project change could not be confirmed by the server; sync before retrying"
        )

    async def _project_options(self, project_id: str | None = None) -> ProjectOptions:
        from .types import ProjectOptions

        editor = self.editor
        if editor is None:
            return ProjectOptions()
        return await editor.options(project_id)

    async def _patch_project(
        self,
        project_id: str,
        *,
        name: str | None = None,
        instructions: str | None = None,
        current: ProjectData | None = None,
    ) -> ProjectData:
        from .types import ProjectData

        if name is not None and not name.strip():
            raise ValueError("Project name is required")
        staged = (
            current
            if current is not None and current.id == project_id
            else await self._get_project_data(project_id)
        )
        changes = _project_patch_changes(staged, name=name, instructions=instructions)
        if not changes:
            return staged
        editor = self.editor
        result = await editor.update(project_id, current=staged, **changes)
        confirmed = _patch_confirmed(changes)
        if isinstance(result, ProjectData) and result.id == project_id and confirmed(result):
            self._invalidate_project_cache(project_id)
            return result
        read_metadata = getattr(editor, "read_metadata", None)
        return await self._project_after_write(
            project_id,
            confirmed,
            read=(lambda: read_metadata(project_id, staged)) if callable(read_metadata) else None,
        )

    async def _add_project_resource(
        self,
        project_id: str,
        kind: str,
        *,
        current: ProjectData | None = None,
        **values: Any,
    ) -> ProjectData:
        before = (
            current
            if current is not None and current.id == project_id
            else await self._get_project_data(project_id)
        )
        result = await self.editor.add_source(
            project_id,
            kind=kind,
            existing_resources=before.resources,
            **values,
        )
        resolved, confirmed = _finalize_add_resource(self, project_id, before, result, values)
        if resolved is not None:
            return resolved
        return await self._project_after_write(project_id, confirmed)

    async def _remove_project_resource(
        self,
        project_id: str,
        resource_id: str,
        *,
        current: ProjectData | None = None,
    ) -> ProjectData:
        before = (
            current
            if current is not None and current.id == project_id
            else await self._get_project_data(project_id)
        )
        source = _find_project_resource(before, resource_id)
        result = await self.editor.remove_source(
            project_id,
            current=before,
            resource_id=resource_id,
            resource_name=source.name,
            resource=source,
        )
        resolved, confirmed = _finalize_remove_resource(self, project_id, resource_id, result)
        if resolved is not None:
            return resolved
        return await self._project_after_write(project_id, confirmed)


class SyncProjectMutationsMixin:
    """Shared blocking project mutations backed by a synchronous provider editor."""

    if TYPE_CHECKING:
        editor: Any
        _gate: threading.Lock

        def _get_project_data(self, project_id: str) -> ProjectData: ...

        def _invalidate_project_cache(self, project_id: str) -> None: ...

    def _project_after_write(self, project_id: str, confirmed: Any, *, read=None) -> ProjectData:
        self._invalidate_project_cache(project_id)
        for attempt in range(12):
            if attempt:
                time.sleep(0.4)
            project = self._get_project_data(project_id) if read is None else read()
            if project.id == project_id and confirmed(project):
                return project
        raise ProviderError(
            "Project change could not be confirmed by the server; sync before retrying"
        )

    def _project_options(self, project_id: str | None = None) -> ProjectOptions:
        from .types import ProjectOptions

        editor = self.editor
        if editor is None:
            return ProjectOptions()
        return editor.options(project_id)

    def _patch_project(
        self,
        project_id: str,
        *,
        name: str | None = None,
        instructions: str | None = None,
        current: ProjectData | None = None,
    ) -> ProjectData:
        from .types import ProjectData

        if name is not None and not name.strip():
            raise ValueError("Project name is required")
        staged = (
            current
            if current is not None and current.id == project_id
            else self._get_project_data(project_id)
        )
        changes = _project_patch_changes(staged, name=name, instructions=instructions)
        if not changes:
            return staged
        editor = self.editor
        result = editor.update(project_id, current=staged, **changes)
        confirmed = _patch_confirmed(changes)
        if isinstance(result, ProjectData) and result.id == project_id and confirmed(result):
            self._invalidate_project_cache(project_id)
            return result
        read_metadata = getattr(editor, "read_metadata", None)
        return self._project_after_write(
            project_id,
            confirmed,
            read=(lambda: read_metadata(project_id, staged)) if callable(read_metadata) else None,
        )

    def _add_project_resource(
        self,
        project_id: str,
        kind: str,
        *,
        current: ProjectData | None = None,
        **values: Any,
    ) -> ProjectData:
        before = (
            current
            if current is not None and current.id == project_id
            else self._get_project_data(project_id)
        )
        result = self.editor.add_source(
            project_id,
            kind=kind,
            existing_resources=before.resources,
            **values,
        )
        resolved, confirmed = _finalize_add_resource(self, project_id, before, result, values)
        if resolved is not None:
            return resolved
        return self._project_after_write(project_id, confirmed)

    def _remove_project_resource(
        self,
        project_id: str,
        resource_id: str,
        *,
        current: ProjectData | None = None,
    ) -> ProjectData:
        before = (
            current
            if current is not None and current.id == project_id
            else self._get_project_data(project_id)
        )
        source = _find_project_resource(before, resource_id)
        result = self.editor.remove_source(
            project_id,
            current=before,
            resource_id=resource_id,
            resource_name=source.name,
            resource=source,
        )
        resolved, confirmed = _finalize_remove_resource(self, project_id, resource_id, result)
        if resolved is not None:
            return resolved
        return self._project_after_write(project_id, confirmed)
