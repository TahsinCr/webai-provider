from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any

from ...collections import AsyncManageableCollection, ManageableCollection, Page, _CollectionFacade
from ...errors import ProviderError
from .project import AsyncProject, Project
from .types import ProjectData as ProviderProject

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider

_MAX_PROJECT_PAGES = 100


class AsyncProjectsCollection(AsyncManageableCollection[AsyncProject]):
    def __init__(self, session: AsyncProvider) -> None:
        super().__init__()
        self.session = session

    async def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
    ) -> AsyncProject:
        project = self.session._project_type(title=title, session=self.session)
        if instruction is not None:
            project.instruction._text = instruction
        created = await self.session._require_provider().create_project(
            project.title,
            instructions=instruction or "",
        )
        project._apply_provider_project(created)
        self._items.insert(0, project)
        return project

    async def refresh(self) -> list[AsyncProject]:
        provider = self.session._require_provider()
        values = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        for _ in range(_MAX_PROJECT_PAGES):
            page = await provider.list_projects(cursor=cursor)
            if not isinstance(page, Page) or any(
                not isinstance(item, ProviderProject) for item in page.items
            ):
                raise ProviderError("Provider returned an invalid project page")
            values.extend(page.items)
            next_cursor = page.next_cursor
            if next_cursor is None:
                break
            if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen_cursors:
                raise ProviderError(
                    "Provider project pagination returned an invalid or repeating cursor"
                )
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        else:
            raise ProviderError(f"Provider project pagination exceeded {_MAX_PROJECT_PAGES} pages")

        projects = []
        for item in values:
            project = self.session._project_type(
                title=item.name,
                remote_id=item.origin.remote_id,
                session=self.session,
            )
            project._apply_provider_project(item)
            projects.append(project)
        self._items = projects
        return list(self._items)

    async def update(
        self,
        project: AsyncProject | str,
        *,
        title: str | None = None,
        instruction: str | None = None,
    ) -> AsyncProject:
        target = project if isinstance(project, AsyncProject) else await self.get(project)
        if target.remote_id:
            provider = self.session._require_provider()
            current = await provider.get_project(target.remote_id)
            if not isinstance(current, ProviderProject):
                raise ProviderError("Provider returned an invalid project")
            updated = await provider.update_project(
                replace(
                    current,
                    name=title if title is not None else current.name,
                    instructions=(instruction if instruction is not None else current.instructions),
                )
            )
            target._apply_provider_project(updated, replace_resources=False)
        else:
            if title is not None:
                target.title = title
            if instruction is not None:
                target.instruction._text = instruction
        return target

    async def delete(self, project: AsyncProject | str) -> None:
        target = project if isinstance(project, AsyncProject) else await self.get(project)
        if target.remote_id:
            await self.session._require_provider().delete_project(target.remote_id)
        self._items = [item for item in self._items if item is not target]


class ProjectsCollection(_CollectionFacade[AsyncProject, Project], ManageableCollection[Project]):
    def __init__(self, owner: Provider) -> None:
        super().__init__(owner, owner._provider.projects)

    def _wrap(self, item: AsyncProject) -> Project:
        return self._owner._wrap_project(item)

    def refresh(self) -> list[Project]:
        return [self._wrap(item) for item in self._owner._call(self._collection.refresh())]

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
