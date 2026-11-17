"""Live checks for project listing and a disposable project lifecycle."""

from webaiprovider.base import Capability
from webaiprovider.errors import UnsupportedOperationError
from webaiprovider.providers.chatgpt import AsyncChatGPTProvider
from webaiprovider.resources.model.collections import Models

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
                renamed = f"{self.context.marker}-r"
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
                attachments = self.call_api(
                    "projects.attachments.list", lambda: project.attachments.list(limit=None)
                )
                self.assertIsInstance(attachments, list)
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
            project = await self.call_or_skip_rate_limit(
                "projects.create",
                lambda: self.context.session.projects.create(marker),
                entry=entry,
            )
            self.assertTrue(project.remote_id, "Created project has no verifiable remote ID")
            entry["remote_id"] = str(project.remote_id)
            self.context.save()

            if Capability.PROJECT_UPDATE in capabilities:
                renamed = f"{self.context.marker}-r"
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
                attachments = await self.call_api(
                    "projects.attachments.list", lambda: project.attachments.list(limit=None)
                )
                self.assertIsInstance(attachments, list)
                removed = await self.call_api(
                    "projects.resources.remove",
                    lambda: project.resources.remove(str(resource_id)),
                )
                self.assertIs(removed, True, "Project resource removal was not confirmed")
            else:
                self.context.skip("projects.resources.add/remove: unsupported")

            chat_entry = None
            second_project_entry = None
            try:
                if (
                    Capability.PROJECT_MEMBERSHIP in capabilities
                    and Capability.CHAT in capabilities
                    and Capability.DELETE in capabilities
                ):
                    loose_title = f"{self.context.marker}-loose"
                    chat_entry = self.context.new_remote("chat", loose_title)
                    chat_entry["attempted"] = True
                    self.context.save()

                    loose_chat = await self.context.session.chats.create(
                        title=f"{loose_title}-draft"
                    )
                    self.context.record_chat_id(chat_entry, loose_chat)
                    self.assertIsNone(loose_chat.project)
                    op = loose_chat.messages.create(f"{loose_title} Say OK.", model=Models.LOW)
                    await self.call_or_skip_rate_limit(
                        "projects.chat.create_loose",
                        op.start,
                        entry=chat_entry,
                        chat=loose_chat,
                    )
                    self.context.record_chat_id(chat_entry, loose_chat)

                    if Capability.RENAME in capabilities:
                        await self.call_api(
                            "projects.chat.rename_loose",
                            lambda: loose_chat.rename(loose_title),
                        )

                    await self.call_api(
                        "projects.chat.move_to", lambda: loose_chat.move_to(project)
                    )
                    self.assertIsNotNone(loose_chat.project)
                    assert loose_chat.project is not None
                    self.assertEqual(loose_chat.project.remote_id, project.remote_id)

                    with self.assertRaises(UnsupportedOperationError):
                        await loose_chat.move_to(project)

                    without_projects = await self.call_api(
                        "chats.search_without_projects",
                        lambda: self.context.session.chats.search(loose_title),
                    )
                    self.assertEqual(without_projects, [])
                    with_projects = await self.call_api(
                        "chats.search_with_projects",
                        lambda: self.context.session.chats.search(
                            loose_title, include_projects=True
                        ),
                    )
                    found = next(
                        (item for item in with_projects if item.remote_id == loose_chat.remote_id),
                        None,
                    )
                    self.assertIsNotNone(found)
                    assert found is not None and found.project is not None
                    self.assertEqual(found.project.remote_id, project.remote_id)

                    if isinstance(self.context.session, AsyncChatGPTProvider):
                        second_marker = f"{self.context.marker}-p2"
                        second_project_entry = self.context.new_remote("project", second_marker)
                        second_project_entry["attempted"] = True
                        self.context.save()
                        project2 = await self.call_or_skip_rate_limit(
                            "projects.create_second",
                            lambda: self.context.session.projects.create(second_marker),
                            entry=second_project_entry,
                        )
                        second_project_entry["remote_id"] = str(project2.remote_id)
                        self.context.save()
                        await self.call_api(
                            "chatgpt.move_project_chat",
                            lambda: self.context.session.move_project_chat(loose_chat, project2),
                        )
                        self.assertIsNotNone(loose_chat.project)
                        assert loose_chat.project is not None
                        self.assertEqual(loose_chat.project.remote_id, project2.remote_id)
                else:
                    self.context.skip("projects.chat.move_to: unsupported")
            finally:
                if chat_entry is not None:
                    await self.context.cleanup_entry_async(chat_entry)
                if second_project_entry is not None:
                    await self.context.cleanup_entry_async(second_project_entry)
        finally:
            await self.context.cleanup_entry_async(entry)
