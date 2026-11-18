"""Live checks for streamed messages, edits, retries, branching, attachments, and incognito mode."""

from contextlib import suppress

from webaiprovider import ArtifactsCollection, Response, SourcesCollection, TimingMetrics
from webaiprovider.base import Capability
from webaiprovider.errors import RemoteNotFound
from webaiprovider.resources.message.attachment import Attachment
from webaiprovider.resources.model.types import Models

from .support.case import LiveAsyncTestCase, LiveSyncTestCase


def _assert_streamed_turn_in_history(case, response, history) -> None:
    case.assertTrue(history, "Provider returned an empty response history")
    case.assertTrue(all(isinstance(item, Response) for item in history))
    response_id = getattr(response, "remote_id", None) or response.id
    verified = next(
        (
            item
            for item in reversed(history)
            if item.remote_id == response_id and item.metadata.get("from_history")
        ),
        None,
    )
    case.assertIsNotNone(verified, "Streamed response was not found in provider history")
    if isinstance(response, Response) and verified.timing is not None:
        case.assertIs(verified.timing, response.timing)


class MessageApiTests(LiveSyncTestCase):
    def test_temporary_message_stream_history_and_cleanup(self) -> None:
        capabilities = self.context.session.capabilities
        required = {Capability.CHAT, Capability.DELETE, Capability.MODELS}
        if not required.issubset(capabilities):
            self.skipTest("Provider cannot safely send and delete a temporary chat")

        marker = f"{self.context.marker}-chat"
        entry = self.context.new_remote("chat", marker)
        entry["attempted"] = True
        self.context.save()
        try:
            chat = self.context.session.chats.create(title=marker)
            self.context.record_chat_id(entry, chat)
            operation = chat.responses.create(f"{marker} Reply with exactly OK.", model=Models.LOW)

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
            responses = self.call_api(
                "responses.list_temporary_chat", lambda: chat.responses.list(limit=50)
            )
            _assert_streamed_turn_in_history(self, response, responses)

            if Capability.RENAME in capabilities:
                renamed = f"{marker}-renamed"
                entry["markers"].append(renamed)
                self.context.save()
                self.call_api("chats.rename_temporary", lambda: chat.rename(renamed))
            else:
                self.context.skip("chats.rename: unsupported")
        finally:
            self.context.cleanup_entry(entry)


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
            operation = chat.responses.create(f"{marker} Reply with exactly OK.", model=Models.LOW)
            response = await self.call_or_skip_rate_limit(
                "responses.temporary.send", operation.start, chat=chat
            )
            self.assertTrue(getattr(response, "text", ""), "Temporary chat returned no text")
            chats = await self.call_api(
                "chats.list_after_temporary", lambda: self.context.session.chats.list(limit=50)
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
        entry["attempted"] = True
        self.context.save()
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
                    f"{marker} Read the attached file and reply with the token or OK.",
                    model=Models.LOW,
                    attachments=[attachment],
                )
            else:
                self.context.skip("responses.attachments: unsupported")
                operation = chat.responses.create(
                    f"{marker} Reply with exactly OK.",
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

            web_op = chat.responses.create(
                f"{marker} Search the web for the official Python website URL and include the link.",
                model=Models.LOW,
            )
            web_response = await self.call_or_skip_rate_limit(
                "responses.create.web_followup", web_op.start, entry=entry, chat=chat
            )
            self.assertTrue(getattr(web_response, "text", ""))
            if not web_response.sources:
                self.context.skip("responses.sources: no sources returned")
            for source in web_response.sources:
                if source.url:
                    self.assertTrue(source.url.startswith(("http://", "https://")))

            followup_op = chat.responses.create(
                f"{marker} Reply with the single word GAMMA.", model=Models.LOW
            )
            followup_response = await self.call_or_skip_rate_limit(
                "responses.create.followup", followup_op.start, entry=entry, chat=chat
            )
            self.assertTrue(getattr(followup_response, "text", ""))

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
                _assert_streamed_turn_in_history(self, retried_last, responses)
            else:
                self.context.skip("responses.retry: unsupported")

            if Capability.MESSAGE_EDIT in capabilities:
                editable = [r.message for r in responses if r.message is not None]
                self.assertGreaterEqual(len(editable), 2)
                edited_last = await self.call_or_skip_rate_limit(
                    "responses.edit.last",
                    editable[-1].edit(f"{marker} Edited last turn: reply with EDITED.").start,
                    entry=entry,
                    chat=chat,
                )
                self.assertTrue(getattr(edited_last, "text", ""))
                responses = await self.call_api(
                    "responses.list_after_edit_last", lambda: chat.responses.list(limit=50)
                )
                _assert_streamed_turn_in_history(self, edited_last, responses)
            else:
                self.context.skip("responses.edit: unsupported")

            if Capability.MESSAGE_RETRY in capabilities and len(responses) >= 2:
                retried_middle = await self.call_or_skip_rate_limit(
                    "responses.retry.middle",
                    responses[-2].retry().start,
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
                        editable[0].edit(f"{marker} Edited earlier turn: reply with MIDDLE.").start,
                        entry=entry,
                        chat=chat,
                    )
                    self.assertTrue(getattr(edited_first, "text", ""))

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
                    branch_entry["attempted"] = True
                    self.context.save()

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
                            f"{branch_marker} Branch check: reply with BRANCH.",
                            model=Models.LOW,
                        )
                        if pending_branch:
                            branch_entry["attempted"] = True
                            self.context.save()
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
