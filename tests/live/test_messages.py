"""Live checks for one short streamed message and its disposable chat history."""

from webaiprovider.base import Capability
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
            chat = self.call_api(
                "chats.create", lambda: self.context.session.chats.create(title=marker)
            )
            self.context.record_chat_id(entry, chat)
            operation = self.call_api(
                "messages.create",
                lambda: chat.messages.create(f"{marker} Reply with exactly OK.", model=Models.LOW),
            )

            def consume_stream() -> int:
                events = 0
                for _event in operation.stream():
                    events += 1
                    self.context.record_chat_id(entry, chat)
                return events

            event_count = self.call_api("messages.create.stream", consume_stream)
            response = self.call_api("messages.wait", operation.wait)
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
    async def test_temporary_message_stream_history_and_cleanup(self) -> None:
        capabilities = self.context.session.capabilities
        required = {Capability.CHAT, Capability.DELETE, Capability.MODELS}
        if not required.issubset(capabilities):
            self.skipTest("Provider cannot safely send and delete a temporary chat")

        marker = f"{self.context.marker}-chat"
        entry = self.context.new_remote("chat", marker)
        entry["attempted"] = True
        self.context.save()
        try:
            chat = await self.call_api(
                "chats.create", lambda: self.context.session.chats.create(title=marker)
            )
            self.context.record_chat_id(entry, chat)
            operation = await self.call_api(
                "messages.create",
                lambda: chat.messages.create(f"{marker} Reply with exactly OK.", model=Models.LOW),
            )

            async def consume_stream() -> int:
                events = 0
                async for _event in operation.stream():
                    events += 1
                    self.context.record_chat_id(entry, chat)
                return events

            event_count = await self.call_api("messages.create.stream", consume_stream)
            response = await self.call_api("messages.wait", operation.wait)
            self.context.record_chat_id(entry, chat)
            self.assertTrue(chat.remote_id, "Temporary conversation has no verifiable remote ID")
            self.assertTrue(event_count, "Message produced no stream events")
            self.assertTrue(getattr(response.message, "content", ""), "Message returned no text")
            messages = await self.call_api("messages.list_temporary_chat", chat.messages.refresh)
            roles = {getattr(message.role, "value", message.role) for message in messages}
            self.assertTrue({"user", "assistant"}.issubset(roles))

            if Capability.RENAME in capabilities:
                renamed = f"{marker}-renamed"
                entry["markers"].append(renamed)
                self.context.save()
                await self.call_api("chats.rename_temporary", lambda: chat.rename(renamed))
            else:
                self.context.skip("chats.rename: unsupported")
        finally:
            await self.context.cleanup_entry_async(entry)
