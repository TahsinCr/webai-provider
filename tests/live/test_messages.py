"""Live checks for streamed messages, edits, retries, branching, attachments, and incognito mode."""

from contextlib import suppress

from webaiprovider.base import Capability
from webaiprovider.errors import RemoteNotFound
from webaiprovider.resources.attachment import Attachment
from webaiprovider.resources.model.collections import Models

from .support.case import LiveAsyncTestCase, LiveSyncTestCase


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
            operation = chat.messages.create(f"{marker} Reply with exactly OK.", model=Models.LOW)

            def consume_stream() -> int:
                events = 0
                try:
                    for _event in operation.stream():
                        events += 1
                        self.context.record_chat_id(entry, chat)
                    return events
                finally:
                    self.context.record_chat_id(entry, chat)

            event_count = self.call_or_skip_rate_limit(
                "messages.create.stream", consume_stream, entry=entry, chat=chat
            )
            response = operation.wait()
            self.context.record_chat_id(entry, chat)
            self.assertTrue(chat.remote_id, "Temporary conversation has no verifiable remote ID")
            self.assertTrue(event_count, "Message produced no stream events")
            self.assertTrue(getattr(response.message, "content", ""), "Message returned no text")
            messages = self.call_api("messages.list_temporary_chat", chat.messages.refresh)
            roles = {getattr(message.role, "value", message.role) for message in messages}
            self.assertTrue({"user", "assistant"}.issubset(roles))

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
            operation = chat.messages.create(f"{marker} Reply with exactly OK.", model=Models.LOW)
            response = await self.call_or_skip_rate_limit(
                "messages.temporary.send", operation.start, chat=chat
            )
            self.assertTrue(
                getattr(response.message, "content", ""), "Temporary chat returned no text"
            )
            chats = await self.call_api(
                "chats.list_after_temporary", self.context.session.chats.refresh
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
                operation = chat.messages.create(
                    f"{marker} Read the attached file and reply with the token or OK.",
                    model=Models.LOW,
                    attachments=[attachment],
                )
            else:
                self.context.skip("messages.attachments: unsupported")
                operation = chat.messages.create(
                    f"{marker} Reply with exactly OK.",
                    model=Models.LOW,
                )

            async def consume_stream() -> int:
                events = 0
                try:
                    async for _event in operation.stream():
                        events += 1
                        self.context.record_chat_id(entry, chat)
                    return events
                finally:
                    self.context.record_chat_id(entry, chat)

            event_count = await self.call_or_skip_rate_limit(
                "messages.create.stream", consume_stream, entry=entry, chat=chat
            )
            response = await operation.wait()
            self.context.record_chat_id(entry, chat)
            self.assertTrue(chat.remote_id, "Temporary conversation has no verifiable remote ID")
            self.assertTrue(event_count, "Message produced no stream events")
            self.assertTrue(getattr(response.message, "content", ""), "Message returned no text")

            web_op = chat.messages.create(
                f"{marker} Search the web for the official Python website URL and include the link.",
                model=Models.LOW,
            )
            web_response = await self.call_or_skip_rate_limit(
                "messages.create.web_followup", web_op.start, entry=entry, chat=chat
            )
            self.assertTrue(getattr(web_response.message, "content", ""))
            for source in web_response.sources:
                if source.url:
                    self.assertTrue(source.url.startswith(("http://", "https://")))

            followup_op = chat.messages.create(
                f"{marker} Reply with the single word GAMMA.", model=Models.LOW
            )
            followup_response = await self.call_or_skip_rate_limit(
                "messages.create.followup", followup_op.start, entry=entry, chat=chat
            )
            self.assertTrue(getattr(followup_response.message, "content", ""))

            messages = await self.call_api("messages.list_temporary_chat", chat.messages.refresh)
            roles = {getattr(message.role, "value", message.role) for message in messages}
            self.assertTrue({"user", "assistant"}.issubset(roles))

            if Capability.MESSAGE_RETRY in capabilities:
                assistant_messages = [
                    m for m in messages if getattr(m.role, "value", m.role) == "assistant"
                ]
                self.assertGreaterEqual(len(assistant_messages), 3)
                retried_last = await self.call_or_skip_rate_limit(
                    "messages.retry.last",
                    assistant_messages[-1].retry().start,
                    entry=entry,
                    chat=chat,
                )
                self.assertTrue(getattr(retried_last.message, "content", ""))
                messages = await self.call_api(
                    "messages.refresh_after_retry_last", chat.messages.refresh
                )
            else:
                self.context.skip("messages.retry: unsupported")

            if Capability.MESSAGE_EDIT in capabilities:
                user_messages = [m for m in messages if getattr(m.role, "value", m.role) == "user"]
                self.assertGreaterEqual(len(user_messages), 2)
                edited_last = await self.call_or_skip_rate_limit(
                    "messages.edit.last",
                    user_messages[-1].edit(f"{marker} Edited last turn: reply with EDITED.").start,
                    entry=entry,
                    chat=chat,
                )
                self.assertTrue(getattr(edited_last.message, "content", ""))
                messages = await self.call_api(
                    "messages.refresh_after_edit_last", chat.messages.refresh
                )
            else:
                self.context.skip("messages.edit: unsupported")

            if Capability.MESSAGE_RETRY in capabilities:
                last_user_idx = max(
                    (
                        idx
                        for idx, m in enumerate(messages)
                        if getattr(m.role, "value", m.role) == "user"
                    ),
                    default=len(messages),
                )
                middle_assistants = [
                    m
                    for m in messages[:last_user_idx]
                    if getattr(m.role, "value", m.role) == "assistant"
                ]
                if not middle_assistants:
                    middle_assistants = [
                        m for m in messages if getattr(m.role, "value", m.role) == "assistant"
                    ][:-1]
                if middle_assistants:
                    retried_middle = await self.call_or_skip_rate_limit(
                        "messages.retry.middle",
                        middle_assistants[-1].retry().start,
                        entry=entry,
                        chat=chat,
                    )
                    self.assertTrue(getattr(retried_middle.message, "content", ""))
                    messages = await self.call_api(
                        "messages.refresh_after_retry_middle", chat.messages.refresh
                    )

            if Capability.MESSAGE_EDIT in capabilities:
                user_messages = [m for m in messages if getattr(m.role, "value", m.role) == "user"]
                if len(user_messages) >= 2:
                    edited_first = await self.call_or_skip_rate_limit(
                        "messages.edit.middle",
                        user_messages[0]
                        .edit(f"{marker} Edited earlier turn: reply with MIDDLE.")
                        .start,
                        entry=entry,
                        chat=chat,
                    )
                    self.assertTrue(getattr(edited_first.message, "content", ""))

            if Capability.CHAT_BRANCH in capabilities and Capability.RENAME in capabilities:
                messages = await self.call_api(
                    "messages.refresh_before_branch", chat.messages.refresh
                )
                assistant_messages = [
                    m for m in messages if getattr(m.role, "value", m.role) == "assistant"
                ]
                if assistant_messages:
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
                        lambda: chat.branch(assistant_messages[-1].id),
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
                        branch_op = branched.messages.create(
                            f"{branch_marker} Branch check: reply with BRANCH.",
                            model=Models.LOW,
                        )
                        if pending_branch:
                            branch_entry["attempted"] = True
                            self.context.save()
                        branch_res = await self.call_or_skip_rate_limit(
                            "messages.create.branched",
                            branch_op.start,
                            entry=branch_entry,
                            chat=branched,
                        )
                        self.context.record_chat_id(branch_entry, branched)
                        self.assertTrue(getattr(branch_res.message, "content", ""))
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
            if branch_entry is not None:
                await self.context.cleanup_entry_async(branch_entry)
            await self.context.cleanup_entry_async(entry)
