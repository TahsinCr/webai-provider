from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ...collections import (
    AsyncMutableRemoteCollection,
    Page,
    _MutableRemoteCollectionFacade,
)
from ...core.operations import execute_with_hooks
from ...errors import FeatureUnavailable, ProviderError
from .project import AsyncProject, Project
from .types import ProjectData as ProviderProject

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider


def _project_from_data(session: AsyncProvider, item: ProviderProject) -> AsyncProject:
    project = session._project_type(
        title=item.name,
        remote_id=item.origin.remote_id,
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
