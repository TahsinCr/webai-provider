"""Live checks for project listing and a disposable project lifecycle."""

from pathlib import Path
from tempfile import TemporaryDirectory

from webaiprovider.base import Capability
from webaiprovider.errors import RemoteNotFound, UnsupportedOperationError
from webaiprovider.resources.model.types import Models

from .support.case import LiveAsyncTestCase, LiveSyncTestCase


def _project_lifecycle_steps(case):
    capabilities = case.context.session.capabilities
    if Capability.PROJECTS not in capabilities:
        case.skipTest("Provider does not expose projects")
    projects = yield case.call_api(
        "projects.list_metadata", lambda: case.context.session.projects.list(limit=50)
    )
    case.assertIsInstance(projects, list)
    required = {Capability.PROJECT_CREATE, Capability.PROJECT_DELETE}
    if not required.issubset(capabilities):
        case.context.skip("projects.create/delete: unsupported")
        return

    marker = f"{case.context.marker}-p"
    entry = case.context.new_remote("project", marker)
    try:
        project = yield case.call_or_skip_rate_limit(
            "projects.create",
            lambda: case.context.session.projects.create(marker),
            entry=entry,
        )
        case.assertTrue(project.remote_id, "Created project has no verifiable remote ID")
        entry["remote_id"] = str(project.remote_id)
        case.context.save()

        if Capability.PROJECT_UPDATE in capabilities:
            renamed = f"{case.context.marker}-r"
            entry["markers"].append(renamed)
            case.context.save()
            yield case.call_api("projects.rename", lambda: project.rename(renamed))
            yield case.call_api(
                "projects.instructions.edit",
                lambda: project.instruction.edit(f"Temporary live API test {marker}."),
            )
        else:
            case.context.skip("projects.rename/instructions.edit: unsupported")

        if Capability.RESOURCES in capabilities:
            options = yield case.call_or_skip_rate_limit(
                "projects.options",
                lambda: case.context.session.project_options(project.remote_id),
            )
            source_types = {item["id"] for item in options.sources}
            created_ids: set[str] = set()
            if "text" in source_types:
                attachment = yield case.call_or_skip_rate_limit(
                    "projects.attachments.create",
                    lambda: project.attachments.create(marker, "text", name=f"{marker}-resource"),
                )
                case.assertTrue(attachment.id, "Created text source has no verifiable ID")
                created_ids.add(attachment.id)
            if "file" in source_types:
                with TemporaryDirectory() as directory:
                    file = Path(directory) / "sample.txt"
                    file.write_text(marker, encoding="utf-8")
                    attachment = yield case.call_or_skip_rate_limit(
                        "projects.attachments.upload_file",
                        lambda: project.attachments.create(file, "file"),
                    )
                case.assertTrue(attachment.id, "Uploaded project file has no verifiable ID")
                created_ids.add(attachment.id)
            if created_ids:
                attachments = yield case.call_api(
                    "projects.attachments.list", lambda: project.attachments.list(limit=None)
                )
                case.assertLessEqual(created_ids, {item.id for item in attachments})
                for attachment_id in sorted(created_ids):
                    yield case.call_or_skip_rate_limit(
                        "projects.attachments.delete",
                        lambda attachment_id=attachment_id: project.attachments.delete(
                            attachment_id
                        ),
                    )
            else:
                case.context.skip("projects.attachments: no supported source types")
        else:
            case.context.skip("projects.attachments.create/delete: unsupported")

        chat_entry = None
        second_project_entry = None
        try:
            if (
                Capability.PROJECT_MEMBERSHIP in capabilities
                and Capability.CHAT in capabilities
                and Capability.DELETE in capabilities
            ):
                loose_title = f"{case.context.marker}-loose"
                chat_entry = case.context.new_remote("chat", loose_title)

                loose_chat = yield case.context.session.chats.create(title=f"{loose_title}-draft")
                case.context.record_chat_id(chat_entry, loose_chat)
                case.assertIsNone(loose_chat.project)
                op = loose_chat.responses.create(f"{loose_title} Say OK.", model=Models.LOW)
                yield case.call_or_skip_rate_limit(
                    "projects.chat.create_loose",
                    op.start,
                    entry=chat_entry,
                    chat=loose_chat,
                )
                case.context.record_chat_id(chat_entry, loose_chat)

                if Capability.RENAME in capabilities:
                    yield case.call_api(
                        "projects.chat.rename_loose",
                        lambda: loose_chat.rename(loose_title),
                    )

                yield case.call_api("projects.chat.move_to", lambda: loose_chat.move_to(project))
                case.assertIsNotNone(loose_chat.project)
                assert loose_chat.project is not None
                case.assertEqual(loose_chat.project.remote_id, project.remote_id)

                is_chatgpt = case.context.account.provider == "chatgpt"
                if not is_chatgpt:
                    with case.assertRaises(UnsupportedOperationError):
                        yield loose_chat.move_to(project)

                with case.assertRaises(RemoteNotFound):
                    yield case.call_api(
                        "chats.get_without_projects",
                        lambda: case.context.session.chats.get(
                            loose_chat.remote_id, metadata_only=True
                        ),
                        expected_errors=(RemoteNotFound,),
                    )
                found = yield case.call_api(
                    "projects.chats.get", lambda: project.chats.get(loose_chat.remote_id)
                )
                case.assertIsNotNone(found.project)
                case.assertTrue(found.project.remote_id == project.remote_id)

                if is_chatgpt:
                    second_marker = f"{case.context.marker}-p2"
                    second_project_entry = case.context.new_remote("project", second_marker)
                    project2 = yield case.call_or_skip_rate_limit(
                        "projects.create_second",
                        lambda: case.context.session.projects.create(second_marker),
                        entry=second_project_entry,
                    )
                    second_project_entry["remote_id"] = str(project2.remote_id)
                    case.context.save()
                    yield case.call_api(
                        "chatgpt.chat.move_to",
                        lambda: loose_chat.move_to(project2),
                    )
                    case.assertIsNotNone(loose_chat.project)
                    assert loose_chat.project is not None
                    case.assertEqual(loose_chat.project.remote_id, project2.remote_id)
            else:
                case.context.skip("projects.chat.move_to: unsupported")
        finally:
            yield case.cleanup_entries(chat_entry, second_project_entry)
    finally:
        yield case.cleanup_entry(entry)


class AsyncProjectApiTests(LiveAsyncTestCase):
    async def test_project_metadata_and_disposable_lifecycle(self) -> None:
        await self.run_steps(_project_lifecycle_steps(self))


class SyncProjectApiTests(LiveSyncTestCase):
    def test_project_metadata_and_disposable_lifecycle(self) -> None:
        self.run_steps(_project_lifecycle_steps(self))
