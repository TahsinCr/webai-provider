"""Project API scenarios for the live API test case."""

from webaiprovider.base import Capability


class ProjectApiTestsMixin:
    def test_04_project_metadata_can_be_listed(self) -> None:
        if Capability.PROJECTS not in self.context.session.capabilities:
            self.skipTest("Provider does not expose projects")
        projects = self.call_api("projects.list_metadata", self.context.session.projects.refresh)
        self.assertIsInstance(projects, list)

    def test_10_temporary_project_lifecycle(self) -> None:
        capabilities = self.context.session.capabilities
        required = {Capability.PROJECT_CREATE, Capability.PROJECT_DELETE}
        if not required.issubset(capabilities):
            self.skipTest("Provider cannot create and safely delete a temporary project")

        marker = f"{self.context.marker}-project"
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
