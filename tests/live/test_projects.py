"""Live checks for project listing and a disposable project lifecycle."""

from webaiprovider.base import Capability

from .support.case import LiveAsyncTestCase, LiveSyncTestCase


class ProjectApiTests(LiveSyncTestCase):
    def test_project_metadata_and_disposable_lifecycle(self) -> None:
        capabilities = self.context.session.capabilities
        if Capability.PROJECTS not in capabilities:
            self.skipTest("Provider does not expose projects")
        projects = self.call_api("projects.list_metadata", self.context.session.projects.refresh)
        self.assertIsInstance(projects, list)
        required = {Capability.PROJECT_CREATE, Capability.PROJECT_DELETE}
        if not required.issubset(capabilities):
            self.context.skip("projects.create/delete: unsupported")
            return

        marker = f"{self.context.marker}-p"
        entry = self.context.new_remote("project", marker)
        entry["attempted"] = True
        self.context.save()
        try:
            project = self.call_api(
                "projects.create", lambda: self.context.session.projects.create(marker)
            )
            self.assertTrue(project.remote_id, "Created project has no verifiable remote ID")
            entry["remote_id"] = str(project.remote_id)
            self.context.save()

            if Capability.PROJECT_UPDATE in capabilities:
                renamed = f"{marker}-renamed"
                entry["markers"].append(renamed)
                self.context.save()
                self.call_api("projects.rename", lambda: project.rename(renamed))
                self.call_api(
                    "projects.instructions.edit",
                    lambda: project.instruction.edit(f"Temporary live API test {marker}."),
                )
            else:
                self.context.skip("projects.rename/instructions.edit: unsupported")

            if Capability.RESOURCES in capabilities:
                resource = self.call_api(
                    "projects.resources.add",
                    lambda: project.resources.add(marker, "text", name=f"{marker}-resource"),
                )
                resource_id = resource.get("id") if isinstance(resource, dict) else None
                self.assertTrue(resource_id, "Created project resource has no verifiable ID")
                removed = self.call_api(
                    "projects.resources.remove",
                    lambda: project.resources.remove(str(resource_id)),
                )
                self.assertIs(removed, True, "Project resource removal was not confirmed")
            else:
                self.context.skip("projects.resources.add/remove: unsupported")
        finally:
            self.context.cleanup_entry(entry)


class AsyncProjectApiTests(LiveAsyncTestCase):
    async def test_project_metadata_and_disposable_lifecycle(self) -> None:
        capabilities = self.context.session.capabilities
        if Capability.PROJECTS not in capabilities:
            self.skipTest("Provider does not expose projects")
        projects = await self.call_api(
            "projects.list_metadata", self.context.session.projects.refresh
        )
        self.assertIsInstance(projects, list)
        required = {Capability.PROJECT_CREATE, Capability.PROJECT_DELETE}
        if not required.issubset(capabilities):
            self.context.skip("projects.create/delete: unsupported")
            return

        marker = f"{self.context.marker}-p"
        entry = self.context.new_remote("project", marker)
        entry["attempted"] = True
        self.context.save()
        try:
            project = await self.call_api(
                "projects.create", lambda: self.context.session.projects.create(marker)
            )
            self.assertTrue(project.remote_id, "Created project has no verifiable remote ID")
            entry["remote_id"] = str(project.remote_id)
            self.context.save()

            if Capability.PROJECT_UPDATE in capabilities:
                renamed = f"{marker}-renamed"
                entry["markers"].append(renamed)
                self.context.save()
                await self.call_api("projects.rename", lambda: project.rename(renamed))
                await self.call_api(
                    "projects.instructions.edit",
                    lambda: project.instruction.edit(f"Temporary live API test {marker}."),
                )
            else:
                self.context.skip("projects.rename/instructions.edit: unsupported")

            if Capability.RESOURCES in capabilities:
                resource = await self.call_api(
                    "projects.resources.add",
                    lambda: project.resources.add(marker, "text", name=f"{marker}-resource"),
                )
                resource_id = resource.get("id") if isinstance(resource, dict) else None
                self.assertTrue(resource_id, "Created project resource has no verifiable ID")
                removed = await self.call_api(
                    "projects.resources.remove",
                    lambda: project.resources.remove(str(resource_id)),
                )
                self.assertIs(removed, True, "Project resource removal was not confirmed")
            else:
                self.context.skip("projects.resources.add/remove: unsupported")
        finally:
            await self.context.cleanup_entry_async(entry)
