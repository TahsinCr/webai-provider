from __future__ import annotations

from ...core.operations import execute_with_hooks
from ...resources.project.collections import AsyncProjectsCollection
from ...resources.project.project import AsyncProject


class AsyncChatGPTProjectsCollection(AsyncProjectsCollection):
    async def create(
        self,
        title: str,
        *,
        instruction: str | None = None,
        memory_share: bool = True,
    ) -> AsyncProject:
        """Create a project with shared default memory or project-only memory."""
        runtime = self.session.runtime
        context = runtime.new_context(
            "project.create",
            parameters={
                "title": title,
                "instruction": instruction,
                "memory_share": memory_share,
            },
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
                    memory_share=ctx.parameters["memory_share"],
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
