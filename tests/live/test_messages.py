"""Live checks for streamed messages, edits, retries, branching, attachments, and incognito mode."""

from contextlib import suppress

from webaiprovider import (
    ArtifactsCollection,
    Response,
    ResponseParts,
    ResponsePartsResolved,
    SourcesCollection,
    TerminalStatus,
    TextPart,
    ThinkingPart,
    TimingMetrics,
)
from webaiprovider.base import Capability
from webaiprovider.errors import RemoteNotFound
from webaiprovider.resources.message.attachment import Attachment
from webaiprovider.resources.model.types import Models

from .support.case import LiveAsyncTestCase, LiveSyncTestCase


def _find_streamed_turn(provider, response: Response, history):
    response_id = response.remote_id or response.id
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
    _assert_response_parts(case, verified)
    if verified.timing is not None:
        case.assertIs(verified.timing, response.timing)


def _assert_response_parts(case, response: Response) -> None:
    case.assertIsInstance(response.parts, ResponseParts)
    case.assertEqual(len({part.id for part in response.parts}), len(response.parts))
    text_parts = [part.text for part in response.parts if isinstance(part, TextPart)]
    case.assertTrue(any(text.strip() for text in text_parts), "Response has no visible text")
    case.assertTrue(
        response.text == "\n".join(text_parts), "Response text does not match its parts"
    )


def _stream_milestone_steps(case, operation, events, response):
    names = [event.event for event in events]
    case.assertEqual(names.count("generation.end"), 1)
    case.assertEqual(names.count("status.terminal"), 1)
    case.assertIsInstance(events[-1], TerminalStatus)
    case.assertEqual(events[-1].status, "completed")
    generation = names.index("generation.end")
    case.assertLessEqual(names.count("verification.begin"), 1)
    if "verification.begin" in names:
        case.assertGreater(names.index("verification.begin"), generation)

    opened = set()
    part_order = {}
    for index, event in enumerate(events):
        channel, _, stage = event.event.partition(".")
        if channel not in ("output", "thinking"):
            continue
        case.assertLess(index, generation, "Content event followed generation completion")
        part_id = event.part_id
        case.assertTrue(part_id, "Content event has no part ID")
        if stage == "begin":
            case.assertFalse(part_id in opened, "Part began before its previous end")
            opened.add(part_id)
            part_order.setdefault(part_id, channel)
        else:
            case.assertTrue(part_id in opened, "Content event has no matching begin")
            case.assertEqual(part_order[part_id], channel)
            if stage == "end":
                opened.remove(part_id)
    case.assertFalse(opened, "Some parts were still open when generation completed")
    case.assertIn("output.begin", names)

    cursor = None
    for event in events:
        if event.event in (
            "output.begin",
            "thinking.begin",
            "generation.end",
            "verification.begin",
            "status.terminal",
        ):
            observed = yield operation.wait_for(event.event, after=cursor, timeout=1)
            case.assertTrue(observed is event, f"wait_for did not return the next {event.event}")
            cursor = event
    observed = yield operation.wait_for("status.terminal", after=cursor, timeout=1)
    case.assertTrue(observed is None, "Terminal cursor returned another event")

    _assert_response_parts(case, response)
    resolved = next(
        (event.parts for event in reversed(events) if isinstance(event, ResponsePartsResolved)),
        (),
    )
    if any(isinstance(part, TextPart) for part in resolved):
        case.assertTrue(tuple(response.parts) == resolved, "Response lost verified parts")
    elif not resolved:
        parts = {
            part.id: "thinking" if isinstance(part, ThinkingPart) else "output"
            for part in response.parts
        }
        case.assertTrue(
            list(parts.items())
            == [(key, channel) for key, channel in part_order.items() if key in parts],
            "Response parts do not preserve streamed order and channels",
        )


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


def _incognito_temporary_chat_steps(case):
    capabilities = case.context.session.capabilities
    if Capability.CHAT not in capabilities or Capability.TEMPORARY not in capabilities:
        case.context.skip("chats.temporary: unsupported")
        case.skipTest("Provider does not support temporary incognito chats")

    marker = f"{case.context.marker}-incognito"
    chat = yield case.context.session.chats.create(title=marker, temporary=True)
    case.assertTrue(chat.temporary)
    try:
        operation = chat.responses.create(f"{marker} Reply with only: OK", model=Models.LOW)
        response = yield case.call_or_skip_rate_limit(
            "responses.temporary.send", operation.start, chat=chat
        )
        case.assertTrue(getattr(response, "text", ""), "Temporary chat returned no text")
        chats = yield case.call_api(
            "chats.list_after_temporary", lambda: case.context.session.chats.list(limit=10)
        )
        if chat.remote_id:
            case.assertNotIn(chat.remote_id, {item.remote_id for item in chats if item.remote_id})
    finally:
        if chat.remote_id:
            with suppress(RemoteNotFound):
                yield case.call_api(
                    "chats.delete_temporary",
                    chat.delete,
                    expected_errors=(RemoteNotFound,),
                )


def _temporary_message_lifecycle_steps(case):
    capabilities = case.context.session.capabilities
    required = {Capability.CHAT, Capability.DELETE, Capability.MODELS}
    if not required.issubset(capabilities):
        case.skipTest("Provider cannot safely send and delete a temporary chat")

    marker = f"{case.context.marker}-chat"
    entry = case.context.new_remote("chat", marker)
    branch_entry = None
    try:
        chat = yield case.context.session.chats.create(title=marker)
        case.context.record_chat_id(entry, chat)

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
            case.context.skip("responses.attachments: unsupported")
            operation = chat.responses.create(
                f"{marker} Reply with only: OK",
                model=Models.LOW,
            )

        events = []

        def record_event(event) -> None:
            events.append(event)
            if not entry.get("remote_id"):
                case.context.record_chat_id(entry, chat)

        yield case.call_or_skip_rate_limit(
            "responses.create.stream",
            lambda: case.consume_stream(operation, record_event),
            entry=entry,
            chat=chat,
        )
        response = yield operation.wait()
        case.assertTrue(chat.remote_id, "Temporary conversation has no verifiable remote ID")
        case.assertIsInstance(response, Response)
        yield from _stream_milestone_steps(case, operation, events, response)
        case.assertIsInstance(response.timing, TimingMetrics)
        case.assertIsInstance(response.sources, SourcesCollection)
        case.assertIsInstance(response.artifacts, ArtifactsCollection)

        remote_id = chat.remote_id
        yield case.call_or_skip_rate_limit("chats.prepare", chat.prepare)
        case.assertTrue(chat.remote_id == remote_id, "Preparation changed the conversation ID")

        followup_op = chat.responses.create(
            f"{marker} Reply with only the single word: GAMMA", model=Models.LOW
        )
        followup_response = yield case.call_or_skip_rate_limit(
            "responses.create.followup", followup_op.start, entry=entry, chat=chat
        )
        case.assertTrue(followup_response.text)

        web_op = chat.responses.create(
            f"{marker} Search the web for the official Python 3.13 release date and include at least one source link. Reply in one short sentence.",
            model=Models.LOW,
        )
        web_response = yield case.call_or_skip_rate_limit(
            "responses.create.web_followup", web_op.start, entry=entry, chat=chat
        )
        case.assertTrue(getattr(web_response, "text", ""))
        case.assertTrue(web_response.sources, "Web search prompt returned no sources")
        for source in web_response.sources:
            case.assertTrue(source.id)
            case.assertTrue(source.url.startswith(("http://", "https://")))

        responses = yield case.call_api(
            "responses.list_temporary_chat", lambda: chat.responses.list(limit=50)
        )
        _assert_streamed_turn_in_history(case, followup_response, responses)

        if Capability.MESSAGE_RETRY in capabilities:
            case.assertGreaterEqual(len(responses), 3)
            retried_last = yield case.call_or_skip_rate_limit(
                "responses.retry.last",
                responses[-1].retry().start,
                entry=entry,
                chat=chat,
            )
            case.assertTrue(getattr(retried_last, "text", ""))
            reopened, responses = yield case.call_api(
                "chats.get_with_responses_after_retry",
                lambda: case.context.session.chats.get_with_responses(chat.remote_id, limit=50),
            )
            case.assertTrue(reopened.remote_id == chat.remote_id)
            if _find_streamed_turn(case.context.account.provider, retried_last, responses) is None:
                responses = yield case.call_api(
                    "responses.verify_retry_fresh",
                    lambda: chat.responses.list(limit=50, fresh=True),
                )
            _assert_streamed_turn_in_history(case, retried_last, responses)
        else:
            case.context.skip("responses.retry: unsupported")

        if Capability.MESSAGE_EDIT in capabilities:
            editable = [r.message for r in responses if r.message is not None]
            case.assertGreaterEqual(len(editable), 2)
            edit_prompt = f"{marker} Reply with only: EDITED"
            edited_last = yield case.call_or_skip_rate_limit(
                "responses.edit.last",
                editable[-1].edit(edit_prompt).start,
                entry=entry,
                chat=chat,
            )
            case.assertTrue(getattr(edited_last, "text", ""))
            responses = yield case.call_api(
                "responses.list_after_edit_last", lambda: chat.responses.list(limit=50)
            )
            if not _has_edited_turn(edit_prompt, responses):
                responses = yield case.call_api(
                    "responses.verify_edit_after_delay",
                    lambda: chat.responses.list(limit=50, fresh=True),
                )
            case.assertTrue(
                _has_edited_turn(edit_prompt, responses),
                "Edited prompt and its answer were not found in provider history",
            )
        else:
            case.context.skip("responses.edit: unsupported")

        if Capability.MESSAGE_RETRY in capabilities and len(responses) >= 2:
            followup_id = followup_response.remote_id or followup_response.id
            earlier = next((item for item in responses[:-1] if item.remote_id == followup_id), None)
            case.assertIsNotNone(earlier, "Earlier plain-text turn was not found in history")
            retried_middle = yield case.call_or_skip_rate_limit(
                "responses.retry.middle",
                earlier.retry().start,
                entry=entry,
                chat=chat,
            )
            case.assertTrue(getattr(retried_middle, "text", ""))
            responses = yield case.call_api(
                "responses.list_after_retry_middle", lambda: chat.responses.list(limit=50)
            )

        if Capability.MESSAGE_EDIT in capabilities:
            editable = [r.message for r in responses if r.message is not None]
            if len(editable) >= 2:
                edited_first = yield case.call_or_skip_rate_limit(
                    "responses.edit.middle",
                    editable[0].edit(f"{marker} Reply with only: MIDDLE").start,
                    entry=entry,
                    chat=chat,
                )
                case.assertTrue(getattr(edited_first, "text", ""))

        artifact_op = chat.responses.create(
            _artifact_prompt(case.context.account.provider, marker),
            model=Models.LOW,
        )
        artifact_response = yield case.call_or_skip_rate_limit(
            "responses.create.artifact_followup",
            artifact_op.start,
            entry=entry,
            chat=chat,
        )
        case.assertTrue(
            artifact_response.artifacts,
            "Artifact generation prompt returned no artifacts",
        )
        for artifact in artifact_response.artifacts:
            case.assertTrue(artifact.id)
            case.assertTrue(artifact.name)
            case.assertTrue(artifact.download_url)

        if Capability.CHAT_BRANCH in capabilities and Capability.RENAME in capabilities:
            responses = yield case.call_api(
                "responses.list_before_branch", lambda: chat.responses.list(limit=50)
            )
            if responses:
                src_title = f"{marker}-src"
                entry["markers"].append(src_title)
                case.context.save()
                yield case.call_api("chats.rename_before_branch", lambda: chat.rename(src_title))

                branch_marker = f"{marker}-branch"
                branch_entry = case.context.new_remote(
                    "chat",
                    branch_marker,
                    exclude_ids=[chat.remote_id] if chat.remote_id else (),
                )
                branch_entry["markers"].extend([src_title, marker])

                branched = yield case.call_or_skip_rate_limit(
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
                        case.context.save()
                    else:
                        case.context.record_chat_id(branch_entry, branched)
                    branch_op = branched.responses.create(
                        f"{branch_marker} Reply with only: BRANCH",
                        model=Models.LOW,
                    )
                    branch_res = yield case.call_or_skip_rate_limit(
                        "responses.create.branched",
                        branch_op.start,
                        entry=branch_entry,
                        chat=branched,
                    )
                    case.context.record_chat_id(branch_entry, branched)
                    case.assertTrue(getattr(branch_res, "text", ""))
                    yield case.call_api(
                        "chats.rename_branched", lambda: branched.rename(branch_marker)
                    )
                    case.assertEqual(branched.title, branch_marker)
        elif Capability.CHAT_BRANCH in capabilities:
            case.context.skip("chats.branch: unrecoverable_without_rename")
        else:
            case.context.skip("chats.branch: unsupported")

        if Capability.RENAME in capabilities:
            renamed = f"{marker}-renamed"
            entry["markers"].append(renamed)
            case.context.save()
            yield case.call_api("chats.rename_temporary", lambda: chat.rename(renamed))
            case.assertEqual(chat.title, renamed)
        else:
            case.context.skip("chats.rename: unsupported")
    finally:
        yield case.cleanup_entries(branch_entry, entry)


class AsyncMessageApiTests(LiveAsyncTestCase):
    async def test_incognito_temporary_chat(self) -> None:
        await self.run_steps(_incognito_temporary_chat_steps(self))

    async def test_temporary_message_stream_history_and_cleanup(self) -> None:
        await self.run_steps(_temporary_message_lifecycle_steps(self))


class SyncMessageApiTests(LiveSyncTestCase):
    def test_incognito_temporary_chat(self) -> None:
        self.run_steps(_incognito_temporary_chat_steps(self))

    def test_temporary_message_stream_history_and_cleanup(self) -> None:
        self.run_steps(_temporary_message_lifecycle_steps(self))
