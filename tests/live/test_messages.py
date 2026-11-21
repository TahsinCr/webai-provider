"""Live checks for streamed messages, edits, retries, branching, attachments, and incognito mode."""

from contextlib import suppress

from webaiprovider import ArtifactsCollection, Response, SourcesCollection, TimingMetrics
from webaiprovider.base import Capability
from webaiprovider.errors import RemoteNotFound
from webaiprovider.resources.message.attachment import Attachment
from webaiprovider.resources.model.types import Models

from .support.case import LiveAsyncTestCase, LiveSyncTestCase


def _find_streamed_turn(provider, response, history):
    response_id = getattr(response, "remote_id", None) or response.id
    for item in reversed(history):
        if item.remote_id == response_id and item.metadata.get("from_history"):
            return item
    if provider != "claude":
        return None

    from webaiprovider.providers.claude.records import stream_message_uuid

    native_id = stream_message_uuid(response_id)
    if native_id is not None:
        response_text = response.text.rstrip()
        for item in reversed(history):
            text = item.text.strip()
            if (
                item.metadata.get("from_history")
                and item.metadata.get("native_message", {}).get("uuid") == native_id
                and text
                and response_text.endswith(text)
            ):
                return item
    return None


def _assert_streamed_turn_in_history(case, response, history) -> None:
    case.assertTrue(history, "Provider returned an empty response history")
    case.assertTrue(all(isinstance(item, Response) for item in history))
    verified = _find_streamed_turn(case.context.account.provider, response, history)
    case.assertIsNotNone(verified, "Streamed response was not found in provider history")
    if isinstance(response, Response) and verified.timing is not None:
        case.assertIs(verified.timing, response.timing)


def _has_edited_turn(prompt: str, history: list[Response]) -> bool:
    return any(
        item.metadata.get("from_history")
        and item.message is not None
        and item.message.content == prompt
        and item.text.strip()
        for item in history
    )


def _artifact_prompt(provider: str, marker: str) -> str:
    if provider == "claude":
        return (
            f"{marker} Create a downloadable file named hello.txt containing the word hello. "
            "Reply in one short sentence."
        )
    if provider == "chatgpt":
        return (
            f"{marker} Use Python to create a downloadable file at /mnt/data/hello.txt "
            "containing the word hello and link it. Reply in one short sentence."
        )
    return f"{marker} Generate a simple minimal icon image of a blue circle."


class AsyncMessageApiTests(LiveAsyncTestCase):
    async def test_incognito_temporary_chat(self) -> None:
        capabilities = self.context.session.capabilities
        if Capability.CHAT not in capabilities or Capability.TEMPORARY not in capabilities:
            self.context.skip("chats.temporary: unsupported")
            self.skipTest("Provider does not support temporary incognito chats")

        marker = f"{self.context.marker}-incognito"
        chat = await self.context.session.chats.create(title=marker, temporary=True)
        self.assertTrue(chat.temporary)
        try:
            operation = chat.responses.create(f"{marker} Reply with only: OK", model=Models.LOW)
            response = await self.call_or_skip_rate_limit(
                "responses.temporary.send", operation.start, chat=chat
            )
            self.assertTrue(getattr(response, "text", ""), "Temporary chat returned no text")
            chats = await self.call_api(
                "chats.list_after_temporary", lambda: self.context.session.chats.list(limit=10)
            )
            if chat.remote_id:
                self.assertNotIn(
                    chat.remote_id, {item.remote_id for item in chats if item.remote_id}
                )
        finally:
            if chat.remote_id:
                with suppress(RemoteNotFound):
                    await self.call_api(
                        "chats.delete_temporary",
                        chat.delete,
                        expected_errors=(RemoteNotFound,),
                    )

    async def test_temporary_message_stream_history_and_cleanup(self) -> None:
        capabilities = self.context.session.capabilities
        required = {Capability.CHAT, Capability.DELETE, Capability.MODELS}
        if not required.issubset(capabilities):
            self.skipTest("Provider cannot safely send and delete a temporary chat")

        marker = f"{self.context.marker}-chat"
        entry = self.context.new_remote("chat", marker)
        branch_entry = None
        try:
            chat = await self.context.session.chats.create(title=marker)
            self.context.record_chat_id(entry, chat)

            if Capability.ATTACHMENTS in capabilities:
                attachment = Attachment.from_bytes(
                    b"Verification token: ALPHA-42\n",
                    name="note.txt",
                    mime_type="text/plain",
                )
                operation = chat.responses.create(
                    f"{marker} Read the attached file and reply with only the token or OK.",
                    model=Models.LOW,
                    attachments=[attachment],
                )
            else:
                self.context.skip("responses.attachments: unsupported")
                operation = chat.responses.create(
                    f"{marker} Reply with only: OK",
                    model=Models.LOW,
                )

            async def consume_stream() -> int:
                events = 0
                try:
                    async for _event in operation.stream():
                        events += 1
                        if not entry.get("remote_id"):
                            self.context.record_chat_id(entry, chat)
                    return events
                finally:
                    self.context.record_chat_id(entry, chat)

            event_count = await self.call_or_skip_rate_limit(
                "responses.create.stream", consume_stream, entry=entry, chat=chat
            )
            response = await operation.wait()
            self.context.record_chat_id(entry, chat)
            self.assertTrue(chat.remote_id, "Temporary conversation has no verifiable remote ID")
            self.assertTrue(event_count, "Message produced no stream events")
            self.assertTrue(getattr(response, "text", ""), "Message returned no text")
            if isinstance(response, Response):
                self.assertIsInstance(response.timing, TimingMetrics)
                self.assertIsInstance(response.sources, SourcesCollection)
                self.assertIsInstance(response.artifacts, ArtifactsCollection)

            followup_op = chat.responses.create(
                f"{marker} Reply with only the single word: GAMMA", model=Models.LOW
            )
            followup_response = await self.call_or_skip_rate_limit(
                "responses.create.followup", followup_op.start, entry=entry, chat=chat
            )
            self.assertTrue(getattr(followup_response, "text", ""))

            web_op = chat.responses.create(
                f"{marker} Search the web for the official Python 3.13 release date and include at least one source link. Reply in one short sentence.",
                model=Models.LOW,
            )
            web_response = await self.call_or_skip_rate_limit(
                "responses.create.web_followup", web_op.start, entry=entry, chat=chat
            )
            self.assertTrue(getattr(web_response, "text", ""))
            self.assertTrue(web_response.sources, "Web search prompt returned no sources")
            for source in web_response.sources:
                self.assertTrue(source.id)
                self.assertTrue(source.url.startswith(("http://", "https://")))

            responses = await self.call_api(
                "responses.list_temporary_chat", lambda: chat.responses.list(limit=50)
            )
            _assert_streamed_turn_in_history(self, followup_response, responses)

            if Capability.MESSAGE_RETRY in capabilities:
                self.assertGreaterEqual(len(responses), 3)
                retried_last = await self.call_or_skip_rate_limit(
                    "responses.retry.last",
                    responses[-1].retry().start,
                    entry=entry,
                    chat=chat,
                )
                self.assertTrue(getattr(retried_last, "text", ""))
                reopened, responses = await self.call_api(
                    "chats.get_with_responses_after_retry",
                    lambda: self.context.session.chats.get_with_responses(chat.remote_id, limit=50),
                )
                self.assertTrue(reopened.remote_id == chat.remote_id)
                if (
                    _find_streamed_turn(self.context.account.provider, retried_last, responses)
                    is None
                ):
                    responses = await self.call_api(
                        "responses.verify_retry_fresh",
                        lambda: chat.responses.list(limit=50, fresh=True),
                    )
                _assert_streamed_turn_in_history(self, retried_last, responses)
            else:
                self.context.skip("responses.retry: unsupported")

            if Capability.MESSAGE_EDIT in capabilities:
                editable = [r.message for r in responses if r.message is not None]
                self.assertGreaterEqual(len(editable), 2)
                edit_prompt = f"{marker} Reply with only: EDITED"
                edited_last = await self.call_or_skip_rate_limit(
                    "responses.edit.last",
                    editable[-1].edit(edit_prompt).start,
                    entry=entry,
                    chat=chat,
                )
                self.assertTrue(getattr(edited_last, "text", ""))
                responses = await self.call_api(
                    "responses.list_after_edit_last", lambda: chat.responses.list(limit=50)
                )
                if not _has_edited_turn(edit_prompt, responses):
                    responses = await self.call_api(
                        "responses.verify_edit_after_delay",
                        lambda: chat.responses.list(limit=50, fresh=True),
                    )
                self.assertTrue(
                    _has_edited_turn(edit_prompt, responses),
                    "Edited prompt and its answer were not found in provider history",
                )
            else:
                self.context.skip("responses.edit: unsupported")

            if Capability.MESSAGE_RETRY in capabilities and len(responses) >= 2:
                followup_id = getattr(followup_response, "remote_id", None) or followup_response.id
                earlier = next(
                    (item for item in responses[:-1] if item.remote_id == followup_id), None
                )
                self.assertIsNotNone(earlier, "Earlier plain-text turn was not found in history")
                retried_middle = await self.call_or_skip_rate_limit(
                    "responses.retry.middle",
                    earlier.retry().start,
                    entry=entry,
                    chat=chat,
                )
                self.assertTrue(getattr(retried_middle, "text", ""))
                responses = await self.call_api(
                    "responses.list_after_retry_middle", lambda: chat.responses.list(limit=50)
                )

            if Capability.MESSAGE_EDIT in capabilities:
                editable = [r.message for r in responses if r.message is not None]
                if len(editable) >= 2:
                    edited_first = await self.call_or_skip_rate_limit(
                        "responses.edit.middle",
                        editable[0].edit(f"{marker} Reply with only: MIDDLE").start,
                        entry=entry,
                        chat=chat,
                    )
                    self.assertTrue(getattr(edited_first, "text", ""))

            artifact_op = chat.responses.create(
                _artifact_prompt(self.context.account.provider, marker),
                model=Models.LOW,
            )
            artifact_response = await self.call_or_skip_rate_limit(
                "responses.create.artifact_followup",
                artifact_op.start,
                entry=entry,
                chat=chat,
            )
            self.assertTrue(
                artifact_response.artifacts,
                "Artifact generation prompt returned no artifacts",
            )
            for artifact in artifact_response.artifacts:
                self.assertTrue(artifact.id)
                self.assertTrue(artifact.name)
                self.assertTrue(artifact.download_url)

            if Capability.CHAT_BRANCH in capabilities and Capability.RENAME in capabilities:
                responses = await self.call_api(
                    "responses.list_before_branch", lambda: chat.responses.list(limit=50)
                )
                if responses:
                    src_title = f"{marker}-src"
                    entry["markers"].append(src_title)
                    self.context.save()
                    await self.call_api(
                        "chats.rename_before_branch", lambda: chat.rename(src_title)
                    )

                    branch_marker = f"{marker}-branch"
                    branch_entry = self.context.new_remote(
                        "chat",
                        branch_marker,
                        exclude_ids=[chat.remote_id] if chat.remote_id else (),
                    )
                    branch_entry["markers"].extend([src_title, marker])

                    branched = await self.call_or_skip_rate_limit(
                        "chats.branch",
                        lambda: responses[-1].branch(),
                        entry=branch_entry,
                    )
                    if branched.remote_id and branched.remote_id != chat.remote_id:
                        pending_branch = isinstance(
                            (branched.metadata or {}).get("pending_branch"), dict
                        )
                        if pending_branch:
                            excluded = branch_entry.setdefault("exclude_ids", [])
                            preview_id = str(branched.remote_id)
                            if preview_id not in excluded:
                                excluded.append(preview_id)
                            branch_entry["attempted"] = False
                            self.context.save()
                        else:
                            self.context.record_chat_id(branch_entry, branched)
                        branch_op = branched.responses.create(
                            f"{branch_marker} Reply with only: BRANCH",
                            model=Models.LOW,
                        )
                        branch_res = await self.call_or_skip_rate_limit(
                            "responses.create.branched",
                            branch_op.start,
                            entry=branch_entry,
                            chat=branched,
                        )
                        self.context.record_chat_id(branch_entry, branched)
                        self.assertTrue(getattr(branch_res, "text", ""))
                        await self.call_api(
                            "chats.rename_branched", lambda: branched.rename(branch_marker)
                        )
                        self.assertEqual(branched.title, branch_marker)
            elif Capability.CHAT_BRANCH in capabilities:
                self.context.skip("chats.branch: unrecoverable_without_rename")
            else:
                self.context.skip("chats.branch: unsupported")

            if Capability.RENAME in capabilities:
                renamed = f"{marker}-renamed"
                entry["markers"].append(renamed)
                self.context.save()
                await self.call_api("chats.rename_temporary", lambda: chat.rename(renamed))
                self.assertEqual(chat.title, renamed)
            else:
                self.context.skip("chats.rename: unsupported")
        finally:
            await self.context.cleanup_entries_async(branch_entry, entry)


class SyncMessageApiTests(LiveSyncTestCase):
    def test_incognito_temporary_chat(self) -> None:
        capabilities = self.context.session.capabilities
        if Capability.CHAT not in capabilities or Capability.TEMPORARY not in capabilities:
            self.context.skip("chats.temporary: unsupported")
            self.skipTest("Provider does not support temporary incognito chats")

        marker = f"{self.context.marker}-incognito"
        chat = self.context.session.chats.create(title=marker, temporary=True)
        self.assertTrue(chat.temporary)
        try:
            operation = chat.responses.create(f"{marker} Reply with only: OK", model=Models.LOW)
            response = self.call_or_skip_rate_limit(
                "responses.temporary.send", operation.start, chat=chat
            )
            self.assertTrue(getattr(response, "text", ""), "Temporary chat returned no text")
            chats = self.call_api(
                "chats.list_after_temporary", lambda: self.context.session.chats.list(limit=10)
            )
            if chat.remote_id:
                self.assertNotIn(
                    chat.remote_id, {item.remote_id for item in chats if item.remote_id}
                )
        finally:
            if chat.remote_id:
                with suppress(RemoteNotFound):
                    self.call_api(
                        "chats.delete_temporary",
                        chat.delete,
                        expected_errors=(RemoteNotFound,),
                    )

    def test_temporary_message_stream_history_and_cleanup(self) -> None:
        capabilities = self.context.session.capabilities
        required = {Capability.CHAT, Capability.DELETE, Capability.MODELS}
        if not required.issubset(capabilities):
            self.skipTest("Provider cannot safely send and delete a temporary chat")

        marker = f"{self.context.marker}-chat"
        entry = self.context.new_remote("chat", marker)
        branch_entry = None
        try:
            chat = self.context.session.chats.create(title=marker)
            self.context.record_chat_id(entry, chat)

            if Capability.ATTACHMENTS in capabilities:
                attachment = Attachment.from_bytes(
                    b"Verification token: ALPHA-42\n",
                    name="note.txt",
                    mime_type="text/plain",
                )
                operation = chat.responses.create(
                    f"{marker} Read the attached file and reply with only the token or OK.",
                    model=Models.LOW,
                    attachments=[attachment],
                )
            else:
                self.context.skip("responses.attachments: unsupported")
                operation = chat.responses.create(
                    f"{marker} Reply with only: OK",
                    model=Models.LOW,
                )

            def consume_stream() -> int:
                events = 0
                try:
                    for _event in operation.stream():
                        events += 1
                        if not entry.get("remote_id"):
                            self.context.record_chat_id(entry, chat)
                    return events
                finally:
                    self.context.record_chat_id(entry, chat)

            event_count = self.call_or_skip_rate_limit(
                "responses.create.stream", consume_stream, entry=entry, chat=chat
            )
            response = operation.wait()
            self.context.record_chat_id(entry, chat)
            self.assertTrue(chat.remote_id, "Temporary conversation has no verifiable remote ID")
            self.assertTrue(event_count, "Message produced no stream events")
            self.assertTrue(getattr(response, "text", ""), "Message returned no text")
            if isinstance(response, Response):
                self.assertIsInstance(response.timing, TimingMetrics)
                self.assertIsInstance(response.sources, SourcesCollection)
                self.assertIsInstance(response.artifacts, ArtifactsCollection)

            followup_op = chat.responses.create(
                f"{marker} Reply with only the single word: GAMMA", model=Models.LOW
            )
            followup_response = self.call_or_skip_rate_limit(
                "responses.create.followup", followup_op.start, entry=entry, chat=chat
            )
            self.assertTrue(getattr(followup_response, "text", ""))

            web_op = chat.responses.create(
                f"{marker} Search the web for the official Python 3.13 release date and include at least one source link. Reply in one short sentence.",
                model=Models.LOW,
            )
            web_response = self.call_or_skip_rate_limit(
                "responses.create.web_followup", web_op.start, entry=entry, chat=chat
            )
            self.assertTrue(getattr(web_response, "text", ""))
            self.assertTrue(web_response.sources, "Web search prompt returned no sources")
            for source in web_response.sources:
                self.assertTrue(source.id)
                self.assertTrue(source.url.startswith(("http://", "https://")))

            responses = self.call_api(
                "responses.list_temporary_chat", lambda: chat.responses.list(limit=50)
            )
            _assert_streamed_turn_in_history(self, followup_response, responses)

            if Capability.MESSAGE_RETRY in capabilities:
                self.assertGreaterEqual(len(responses), 3)
                retried_last = self.call_or_skip_rate_limit(
                    "responses.retry.last",
                    responses[-1].retry().start,
                    entry=entry,
                    chat=chat,
                )
                self.assertTrue(getattr(retried_last, "text", ""))
                reopened, responses = self.call_api(
                    "chats.get_with_responses_after_retry",
                    lambda: self.context.session.chats.get_with_responses(chat.remote_id, limit=50),
                )
                self.assertTrue(reopened.remote_id == chat.remote_id)
                if (
                    _find_streamed_turn(self.context.account.provider, retried_last, responses)
                    is None
                ):
                    responses = self.call_api(
                        "responses.verify_retry_fresh",
                        lambda: chat.responses.list(limit=50, fresh=True),
                    )
                _assert_streamed_turn_in_history(self, retried_last, responses)
            else:
                self.context.skip("responses.retry: unsupported")

            if Capability.MESSAGE_EDIT in capabilities:
                editable = [r.message for r in responses if r.message is not None]
                self.assertGreaterEqual(len(editable), 2)
                edit_prompt = f"{marker} Reply with only: EDITED"
                edited_last = self.call_or_skip_rate_limit(
                    "responses.edit.last",
                    editable[-1].edit(edit_prompt).start,
                    entry=entry,
                    chat=chat,
                )
                self.assertTrue(getattr(edited_last, "text", ""))
                responses = self.call_api(
                    "responses.list_after_edit_last", lambda: chat.responses.list(limit=50)
                )
                if not _has_edited_turn(edit_prompt, responses):
                    responses = self.call_api(
                        "responses.verify_edit_after_delay",
                        lambda: chat.responses.list(limit=50, fresh=True),
                    )
                self.assertTrue(
                    _has_edited_turn(edit_prompt, responses),
                    "Edited prompt and its answer were not found in provider history",
                )
            else:
                self.context.skip("responses.edit: unsupported")

            if Capability.MESSAGE_RETRY in capabilities and len(responses) >= 2:
                followup_id = getattr(followup_response, "remote_id", None) or followup_response.id
                earlier = next(
                    (item for item in responses[:-1] if item.remote_id == followup_id), None
                )
                self.assertIsNotNone(earlier, "Earlier plain-text turn was not found in history")
                retried_middle = self.call_or_skip_rate_limit(
                    "responses.retry.middle",
                    earlier.retry().start,
                    entry=entry,
                    chat=chat,
                )
                self.assertTrue(getattr(retried_middle, "text", ""))
                responses = self.call_api(
                    "responses.list_after_retry_middle", lambda: chat.responses.list(limit=50)
                )

            if Capability.MESSAGE_EDIT in capabilities:
                editable = [r.message for r in responses if r.message is not None]
                if len(editable) >= 2:
                    edited_first = self.call_or_skip_rate_limit(
                        "responses.edit.middle",
                        editable[0].edit(f"{marker} Reply with only: MIDDLE").start,
                        entry=entry,
                        chat=chat,
                    )
                    self.assertTrue(getattr(edited_first, "text", ""))

            artifact_op = chat.responses.create(
                _artifact_prompt(self.context.account.provider, marker),
                model=Models.LOW,
            )
            artifact_response = self.call_or_skip_rate_limit(
                "responses.create.artifact_followup",
                artifact_op.start,
                entry=entry,
                chat=chat,
            )
            self.assertTrue(
                artifact_response.artifacts,
                "Artifact generation prompt returned no artifacts",
            )
            for artifact in artifact_response.artifacts:
                self.assertTrue(artifact.id)
                self.assertTrue(artifact.name)
                self.assertTrue(artifact.download_url)

            if Capability.CHAT_BRANCH in capabilities and Capability.RENAME in capabilities:
                responses = self.call_api(
                    "responses.list_before_branch", lambda: chat.responses.list(limit=50)
                )
                if responses:
                    src_title = f"{marker}-src"
                    entry["markers"].append(src_title)
                    self.context.save()
                    self.call_api("chats.rename_before_branch", lambda: chat.rename(src_title))

                    branch_marker = f"{marker}-branch"
                    branch_entry = self.context.new_remote(
                        "chat",
                        branch_marker,
                        exclude_ids=[chat.remote_id] if chat.remote_id else (),
                    )
                    branch_entry["markers"].extend([src_title, marker])

                    branched = self.call_or_skip_rate_limit(
                        "chats.branch",
                        lambda: responses[-1].branch(),
                        entry=branch_entry,
                    )
                    if branched.remote_id and branched.remote_id != chat.remote_id:
                        pending_branch = isinstance(
                            (branched.metadata or {}).get("pending_branch"), dict
                        )
                        if pending_branch:
                            excluded = branch_entry.setdefault("exclude_ids", [])
                            preview_id = str(branched.remote_id)
                            if preview_id not in excluded:
                                excluded.append(preview_id)
                            branch_entry["attempted"] = False
                            self.context.save()
                        else:
                            self.context.record_chat_id(branch_entry, branched)
                        branch_op = branched.responses.create(
                            f"{branch_marker} Reply with only: BRANCH",
                            model=Models.LOW,
                        )
                        branch_res = self.call_or_skip_rate_limit(
                            "responses.create.branched",
                            branch_op.start,
                            entry=branch_entry,
                            chat=branched,
                        )
                        self.context.record_chat_id(branch_entry, branched)
                        self.assertTrue(getattr(branch_res, "text", ""))
                        self.call_api(
                            "chats.rename_branched", lambda: branched.rename(branch_marker)
                        )
                        self.assertEqual(branched.title, branch_marker)
            elif Capability.CHAT_BRANCH in capabilities:
                self.context.skip("chats.branch: unrecoverable_without_rename")
            else:
                self.context.skip("chats.branch: unsupported")

            if Capability.RENAME in capabilities:
                renamed = f"{marker}-renamed"
                entry["markers"].append(renamed)
                self.context.save()
                self.call_api("chats.rename_temporary", lambda: chat.rename(renamed))
                self.assertEqual(chat.title, renamed)
            else:
                self.context.skip("chats.rename: unsupported")
        finally:
            self.context.cleanup_entries(branch_entry, entry)
