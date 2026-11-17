from __future__ import annotations

import concurrent.futures
import tempfile
import threading
import unittest
from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path

from eventflow import AsyncEventBus, EventBus, EventEnvelope, EventFilter

import webaiprovider
from webaiprovider import (
    Account,
    AmbiguousMatchError,
    ArtifactsCollection,
    AsyncProvider,
    Attachment,
    Capability,
    HookDispatchResult,
    LifecycleState,
    Provider,
    ProviderError,
    RemoteNotFound,
    Response,
    SourcesCollection,
    SyncOperationHooks,
    TimingMetrics,
    UnsupportedOperationError,
)
from webaiprovider.base import (
    BaseAttachmentAdapter,
    BaseChatAdapter,
    BaseMessageAdapter,
    BaseModelAdapter,
    BaseProjectAdapter,
    BaseProviderAdapter,
    BaseSessionAdapter,
)
from webaiprovider.collections import Page
from webaiprovider.core.operations import OperationCancelledError, OperationContext
from webaiprovider.resources.chat.types import Conversation
from webaiprovider.resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    ProviderRequest,
    ProviderStreamEvent,
    ResponseComplete,
    TextDelta,
)
from webaiprovider.resources.message.types import Content, MessageData
from webaiprovider.resources.model import ModelCatalog, ModelCatalogState
from webaiprovider.resources.project.types import ProjectData


def _account(name: str = "v2-test") -> Account:
    return Account(id="acc-v2", provider="chatgpt", credentials={"cookie": "v2"}, name=name)


class _RecordingSessionAdapter(BaseSessionAdapter):
    def __init__(self, *, fail_prepare: Exception | None = None) -> None:
        self.prepare_calls = 0
        self.close_calls = 0
        self.fail_prepare = fail_prepare

    async def prepare(self) -> None:
        self.prepare_calls += 1
        if self.fail_prepare is not None:
            raise self.fail_prepare

    async def close(self) -> None:
        self.close_calls += 1


class _RecordingModelAdapter(BaseModelAdapter):
    async def load_catalog(self) -> ModelCatalog:
        return ModelCatalog(ModelCatalogState.AVAILABLE)


class _ScriptedChatAdapter(BaseChatAdapter):
    def __init__(self, provider_id: str, account_id: str) -> None:
        self.provider_id = provider_id
        self.account_id = account_id
        self.pages: list[tuple[list[Conversation], str | None]] = []
        self.conversations: dict[str, Conversation] = {}
        self.list_calls: list[tuple[str | None, int | None]] = []
        self.get_chat_calls: list[str] = []
        self.rename_calls: list[tuple[str, str]] = []
        self.delete_calls: list[str] = []
        self.loop_threads: set[int] = set()
        self.page_error_at: int | None = None

    def seed_paginated(self, count: int, *, page_size: int = 50, overlap: bool = False) -> None:
        items = [Conversation(id=f"c-{i}", title=f"Chat {i:03d}") for i in range(count)]
        for conv in items:
            self.conversations[conv.id] = conv
        self.pages.clear()
        for start in range(0, count, page_size):
            end = min(start + page_size, count)
            chunk = list(items[start:end])
            if overlap and start > 0:
                chunk.insert(0, items[start - 1])
            next_cursor = str(end) if end < count else None
            self.pages.append((chunk, next_cursor))

    async def list_chats(self, *, cursor: str | None = None, limit: int = 50) -> Page[Conversation]:
        self.loop_threads.add(threading.get_ident())
        call_index = len(self.list_calls) + 1
        self.list_calls.append((cursor, limit))
        if self.page_error_at is not None and call_index == self.page_error_at:
            raise ProviderError(f"Simulated pagination failure on call {call_index}")
        start = int(cursor) if cursor is not None else 0
        if not self.pages:
            return Page((), None)
        # Match seeded page starting at cursor, slicing to limit if smaller than seeded page
        for chunk, next_cursor in self.pages:
            chunk_ids = [c.id for c in chunk]
            target_id = f"c-{start}"
            if target_id in chunk_ids or (cursor is None and chunk is self.pages[0][0]):
                return Page(tuple(chunk[:limit]), next_cursor)
        return Page((), None)

    async def get_chat(self, chat_id: str) -> Conversation:
        self.get_chat_calls.append(chat_id)
        if chat_id not in self.conversations:
            raise RemoteNotFound(f"Missing chat: {chat_id}")
        return self.conversations[chat_id]

    async def rename_chat(self, chat_id: str, title: str) -> None:
        self.rename_calls.append((chat_id, title))
        conv = self.conversations.get(chat_id)
        if conv is not None:
            self.conversations[chat_id] = replace(conv, title=title)

    async def delete_chat(self, chat_id: str) -> None:
        self.delete_calls.append(chat_id)
        self.conversations.pop(chat_id, None)


class _PagedRecordsChatAdapter(BaseChatAdapter):
    """Chat adapter serving explicit discrete pages for early-stop query tests."""

    def __init__(self, paged_batches: list[list[Conversation]]) -> None:
        self._batches = paged_batches
        self.fetch_calls: list[str | None] = []

    async def list_chats(self, *, cursor: str | None = None, limit: int = 50) -> Page[Conversation]:
        del limit
        self.fetch_calls.append(cursor)
        idx = int(cursor) if cursor is not None else 0
        items = self._batches[idx] if idx < len(self._batches) else []
        next_cursor = str(idx + 1) if idx + 1 < len(self._batches) else None
        return Page(tuple(items), next_cursor)


class _ScriptedProjectAdapter(BaseProjectAdapter):
    def __init__(self, provider_id: str, account_id: str) -> None:
        self.provider_id = provider_id
        self.account_id = account_id
        self.projects: dict[str, ProjectData] = {}
        self.project_chats: dict[str, list[Conversation]] = {}
        self.upload_calls: list[tuple[str, Path]] = []
        self.remove_calls: list[tuple[str, str]] = []

    def seed_project(
        self,
        project_id: str,
        name: str,
        *,
        resources: tuple[Attachment, ...] = (),
        chats: list[Conversation] | None = None,
    ) -> ProjectData:
        data = ProjectData(
            id=project_id,
            name=name,
            resources=resources,
        )
        self.projects[project_id] = data
        if chats is not None:
            self.project_chats[project_id] = list(chats)
        return data

    async def list_projects(
        self, *, cursor: str | None = None, limit: int = 50
    ) -> Page[ProjectData]:
        del cursor, limit
        return Page(tuple(self.projects.values()), None)

    async def get_project(self, project_id: str) -> ProjectData:
        if project_id not in self.projects:
            raise RemoteNotFound(f"Missing project: {project_id}")
        return self.projects[project_id]

    async def list_project_chats(
        self, project_id: str, cursor: str | None = None
    ) -> Page[Conversation]:
        del cursor
        return Page(tuple(self.project_chats.get(project_id, ())), None)

    async def create_project(
        self, name: str, instructions: str = "", **options: object
    ) -> ProjectData:
        del options
        pid = f"p-{len(self.projects) + 1}"
        data = ProjectData(
            id=pid,
            name=name,
            instructions=instructions or None,
        )
        self.projects[pid] = data
        return data

    async def add_project_resource(
        self, project_id: str, kind: str, **values: object
    ) -> ProjectData:
        del kind
        path = Path(str(values["path"]))
        self.upload_calls.append((project_id, path))
        proj = self.projects[project_id]
        res = Attachment(
            id=f"res-{len(proj.resources) + 1}",
            name=path.name,
            media_type="text/plain",
            remote_url=f"https://example.invalid/files/{path.name}",
            metadata={"kind": "file"},
        )
        updated = replace(proj, resources=(*proj.resources, res))
        self.projects[project_id] = updated
        return updated

    async def remove_project_resource(self, project_id: str, resource_id: str) -> ProjectData:
        self.remove_calls.append((project_id, resource_id))
        proj = self.projects[project_id]
        kept = tuple(r for r in proj.resources if r.id != resource_id)
        updated = replace(proj, resources=kept)
        self.projects[project_id] = updated
        return updated


class _ScriptedMessageAdapter(BaseMessageAdapter):
    def __init__(self, chat_adapter: _ScriptedChatAdapter) -> None:
        self.chat_adapter = chat_adapter
        self.received_requests: list[ProviderRequest] = []
        self.stream_error: Exception | None = None

    async def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderStreamEvent]:
        self.received_requests.append(request)
        if self.stream_error is not None:
            raise self.stream_error
        cid = request.conversation_id or f"c-live-{len(self.received_requests)}"
        if cid not in self.chat_adapter.conversations:
            self.chat_adapter.conversations[cid] = Conversation(
                id=cid,
                title="Live Chat",
            )
            yield ConversationBound(cid)
        yield MessageMetadata(
            message_id="m-live-1",
            user_message_id="u-live-1",
            model="gpt-5",
            sources=(
                {"id": "src-live", "url": "https://example.com/live", "title": "Live Source"},
            ),
            files=(
                {
                    "id": "art-live",
                    "name": "output.txt",
                    "media_type": "text/plain",
                    "url": "https://example.com/output.txt",
                },
            ),
        )
        yield TextDelta(f"Echo: {request.prompt}")
        yield ResponseComplete()


def _build_adapter(
    acc: Account,
    *,
    session_adapter: _RecordingSessionAdapter | None = None,
    chat_adapter: BaseChatAdapter | None = None,
    project_adapter: _ScriptedProjectAdapter | None = None,
    message_adapter: BaseMessageAdapter | None = None,
) -> BaseProviderAdapter:
    chats = chat_adapter or _ScriptedChatAdapter(acc.provider, acc.id)
    projects = project_adapter or _ScriptedProjectAdapter(acc.provider, acc.id)
    messages = message_adapter or (
        _ScriptedMessageAdapter(chats)
        if isinstance(chats, _ScriptedChatAdapter)
        else BaseMessageAdapter()
    )
    caps = frozenset(
        {
            Capability.CHAT,
            Capability.HISTORY,
            Capability.DELETE,
            Capability.RENAME,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.PROJECT_DELETE,
            Capability.RESOURCES,
            Capability.MODELS,
        }
    )
    return BaseProviderAdapter(
        provider_id=acc.provider,
        account_id=acc.id,
        capabilities=caps,
        session=session_adapter or _RecordingSessionAdapter(),
        models=_RecordingModelAdapter(),
        chats=chats,
        projects=projects,
        messages=messages,
        attachments=BaseAttachmentAdapter(),
    )


def _build_runtime_provider(
    *,
    account: Account | None = None,
    session_adapter: _RecordingSessionAdapter | None = None,
    chat_adapter: BaseChatAdapter | None = None,
    project_adapter: _ScriptedProjectAdapter | None = None,
    message_adapter: BaseMessageAdapter | None = None,
) -> tuple[AsyncProvider, BaseProviderAdapter]:
    acc = account or _account()
    adapter = _build_adapter(
        acc,
        session_adapter=session_adapter,
        chat_adapter=chat_adapter,
        project_adapter=project_adapter,
        message_adapter=message_adapter,
    )
    return AsyncProvider(acc, adapter=adapter), adapter


class TestPublicSurfaceAndExports(unittest.TestCase):
    def test_page_is_not_exported_on_top_level_and_v2_symbols_exist(self) -> None:
        self.assertNotIn("Page", webaiprovider.__all__)
        self.assertFalse(hasattr(webaiprovider, "Page"))
        expected = {
            "Account",
            "AmbiguousMatchError",
            "Artifact",
            "ArtifactsCollection",
            "AsyncEventBus",
            "EventBus",
            "EventEnvelope",
            "EventFilter",
            "AsyncProvider",
            "HookDispatchResult",
            "LifecycleState",
            "OperationHooks",
            "Provider",
            "Response",
            "Source",
            "SourcesCollection",
            "SyncOperationHooks",
            "TimingMetrics",
        }
        self.assertTrue(expected.issubset(set(webaiprovider.__all__)))


class TestCollectionV2Contract(unittest.IsolatedAsyncioTestCase):
    async def test_list_paginates_limit_120_deduplicates_and_respects_default_zero_and_none(
        self,
    ) -> None:
        acc = _account()
        chat_adapter = _ScriptedChatAdapter(acc.provider, acc.id)
        chat_adapter.seed_paginated(125, page_size=50, overlap=True)
        provider, _ = _build_runtime_provider(account=acc, chat_adapter=chat_adapter)
        try:
            await provider.open()

            # Default limit=50 -> 1 remote call
            default_chats = await provider.chats.list()
            self.assertEqual(len(default_chats), 50)
            self.assertEqual(len(chat_adapter.list_calls), 1)
            self.assertEqual(chat_adapter.list_calls[0], (None, 50))

            # limit=0 -> returns [] with 0 remote calls
            chat_adapter.list_calls.clear()
            zero_chats = await provider.chats.list(limit=0)
            self.assertEqual(zero_chats, [])
            self.assertEqual(chat_adapter.list_calls, [])

            # limit=120 -> fetches multiple pages (50, 50, 20+), deduplicates overlap -> 120 unique
            chat_adapter.list_calls.clear()
            page_120 = await provider.chats.list(limit=120)
            self.assertEqual(len(page_120), 120)
            self.assertEqual(len({c.remote_id for c in page_120}), 120)
            self.assertEqual([c.remote_id for c in page_120[:3]], ["c-0", "c-1", "c-2"])

            # limit=None -> full scope (125 unique)
            chat_adapter.list_calls.clear()
            all_chats = await provider.chats.list(limit=None)
            self.assertEqual(len(all_chats), 125)
            self.assertEqual(len({c.remote_id for c in all_chats}), 125)
            self.assertEqual(len(chat_adapter.list_calls), 3)
        finally:
            await provider.close()

    async def test_invalid_limits_and_mid_pagination_partial_count(self) -> None:
        acc = _account()
        chat_adapter = _ScriptedChatAdapter(acc.provider, acc.id)
        chat_adapter.seed_paginated(125, page_size=50)
        provider, _ = _build_runtime_provider(account=acc, chat_adapter=chat_adapter)
        try:
            await provider.open()

            with self.assertRaises(ValueError):
                await provider.chats.list(limit=-1)
            for invalid in (True, False, "10", 1.5):
                with self.assertRaises(TypeError):
                    await provider.chats.list(limit=invalid)  # type: ignore[arg-type]

            # Failure on 1st page attaches partial_count == 0
            chat_adapter.list_calls.clear()
            chat_adapter.page_error_at = 1
            with self.assertRaises(ProviderError) as err1:
                await provider.chats.list(limit=120)
            self.assertEqual(getattr(err1.exception, "partial_count", None), 0)

            # Failure on 2nd page attaches partial_count == 50
            chat_adapter.list_calls.clear()
            chat_adapter.page_error_at = 2
            with self.assertRaises(ProviderError) as err2:
                await provider.chats.list(limit=120)
            self.assertEqual(getattr(err2.exception, "partial_count", None), 50)
        finally:
            await provider.close()

    async def test_no_stale_remote_cache_and_refresh_emits_event(self) -> None:
        acc = _account()
        chat_adapter = _ScriptedChatAdapter(acc.provider, acc.id)
        chat_adapter.seed_paginated(2, page_size=50)
        provider, _ = _build_runtime_provider(account=acc, chat_adapter=chat_adapter)
        refreshed_events: list[EventEnvelope[str]] = []

        async def record_refreshed(event: EventEnvelope[str]) -> None:
            refreshed_events.append(event)

        await provider.events.subscribe(
            record_refreshed,
            event_filter=EventFilter(
                event_type=EventEnvelope,
                predicate=lambda event: event.event == "collection.refreshed",
            ),
        )
        try:
            await provider.open()
            first_list = await provider.chats.list(limit=None)
            self.assertEqual([c.remote_id for c in first_list], ["c-0", "c-1"])

            # Remote changes without touching local collection: subsequent list reads fresh remote data
            chat_adapter.seed_paginated(4, page_size=50)
            second_list = await provider.chats.list(limit=None)
            self.assertEqual(len(second_list), 4)

            refreshed = await provider.chats.refresh(limit=None)
            self.assertEqual(len(refreshed), 4)
            self.assertEqual(len(refreshed_events), 1)
            self.assertEqual(
                refreshed_events[0].metadata["payload"],
                {"collection": "chats", "count": 4},
            )
        finally:
            await provider.close()

    async def test_first_find_last_search_and_flag_validation(self) -> None:
        acc = _account()

        def conv(cid: str, title: str) -> Conversation:
            return Conversation(id=cid, title=title)

        paged = _PagedRecordsChatAdapter(
            [
                [conv("1", "alpha-one"), conv("2", "beta-only")],
                [conv("3", "alpha-two"), conv("4", "gamma-exact")],
                [conv("5", "alpha-three"), conv("6", "delta-end")],
            ]
        )
        provider, _ = _build_runtime_provider(account=acc, chat_adapter=paged)
        try:
            await provider.open()

            # first() stops immediately on page 1
            paged.fetch_calls.clear()
            first = await provider.chats.first("alpha", startswith=True)
            self.assertEqual(first.remote_id, "1")
            self.assertEqual(len(paged.fetch_calls), 1)

            # last() scans every page (all 3 pages)
            paged.fetch_calls.clear()
            last = await provider.chats.last("alpha", startswith=True)
            self.assertEqual(last.remote_id, "5")
            self.assertEqual(len(paged.fetch_calls), 3)

            # find() with match flag aborts as soon as 2nd match appears (page 2, does not fetch page 3)
            paged.fetch_calls.clear()
            with self.assertRaises(AmbiguousMatchError):
                await provider.chats.find("alpha", startswith=True)
            self.assertEqual(len(paged.fetch_calls), 2)

            # find() unique match succeeds, search() returns all 3 matches
            unique = await provider.chats.find("gamma-exact", exact=True)
            self.assertEqual(unique.remote_id, "4")
            all_alpha = await provider.chats.search("alpha", startswith=True)
            self.assertEqual([c.remote_id for c in all_alpha], ["1", "3", "5"])

            with self.assertRaises(RemoteNotFound):
                await provider.chats.find("non-existent", exact=True)

            # Conflicting search flags rejected
            with self.assertRaises(ValueError):
                await provider.chats.search("alpha", exact=True, startswith=True)
            with self.assertRaises(ValueError):
                await provider.chats.first("alpha", startswith=True, endswith=True)
        finally:
            await provider.close()


class TestMessagesResponsesSourcesArtifacts(unittest.IsolatedAsyncioTestCase):
    async def test_shared_history_fetch_separates_messages_and_responses_with_sources_and_artifacts(
        self,
    ) -> None:
        acc = _account()
        chat_adapter = _ScriptedChatAdapter(acc.provider, acc.id)
        cid = "c-history-1"
        chat_adapter.conversations[cid] = Conversation(
            id=cid,
            title="Research",
            messages=(
                MessageData(
                    id="u-1",
                    role="user",
                    content=(Content(kind="text", text="Summarize report"),),
                ),
                MessageData(
                    id="a-1",
                    role="assistant",
                    content=(
                        Content(kind="text", text="Here is the summary."),
                        Content(
                            kind="file",
                            resource=Attachment(
                                id="art-1",
                                name="report.pdf",
                                media_type="application/pdf",
                                remote_url="https://example.com/report.pdf",
                            ),
                        ),
                    ),
                    metadata={
                        "sources": [
                            {"id": "s-1", "url": "https://example.com/a", "title": "Paper A"},
                            {"id": "s-2", "url": "https://example.com/b", "title": "Paper B"},
                        ]
                    },
                ),
            ),
        )
        msg_adapter = _ScriptedMessageAdapter(chat_adapter)
        provider, _ = _build_runtime_provider(
            account=acc, chat_adapter=chat_adapter, message_adapter=msg_adapter
        )
        try:
            await provider.open()
            chat = await provider.chats.get(cid)
            chat_adapter.get_chat_calls.clear()

            # History-derived responses: single get_chat call, timing is None, sources & artifacts exposed
            responses = await chat.responses.list()
            self.assertEqual(chat_adapter.get_chat_calls, [cid])
            self.assertEqual(len(responses), 1)
            hist_resp = responses[0]
            self.assertIsInstance(hist_resp, Response)
            self.assertIsNone(hist_resp.timing)
            self.assertEqual(hist_resp.text, "Here is the summary.")
            self.assertIsInstance(hist_resp.sources, SourcesCollection)
            self.assertEqual(len(hist_resp.sources), 2)
            self.assertEqual(hist_resp.sources.find("Paper A", exact=True).id, "s-1")
            self.assertIsInstance(hist_resp.artifacts, ArtifactsCollection)
            self.assertEqual(len(hist_resp.artifacts), 1)
            self.assertEqual(hist_resp.artifacts.first("report.pdf").id, "art-1")

            # Assistant Message links back to same response/sources/artifacts
            msgs = await chat.messages.list()
            self.assertEqual([m.role for m in msgs], ["user", "assistant"])
            self.assertIsNotNone(msgs[1].response)
            self.assertIsNone(msgs[1].response.timing)
            self.assertEqual(len(msgs[1].sources), 2)
            self.assertEqual(len(msgs[1].artifacts), 1)

            # Live stream response carries TimingMetrics plus normalized sources and artifacts
            live_resp = await chat.messages.create("Follow up").start()
            self.assertIsInstance(live_resp, Response)
            self.assertIsInstance(live_resp.timing, TimingMetrics)
            self.assertEqual(live_resp.text, "Echo: Follow up")
            self.assertIsInstance(live_resp.sources, SourcesCollection)
            self.assertEqual(live_resp.sources.first("Live Source").url, "https://example.com/live")
            self.assertIsInstance(live_resp.artifacts, ArtifactsCollection)
            self.assertEqual(live_resp.artifacts.first("output.txt").name, "output.txt")
        finally:
            await provider.close()

    async def test_retry_and_edit_carriers_excluded_after_refresh_and_user_id_binds_remote(
        self,
    ) -> None:
        acc = _account()
        chat_adapter = _ScriptedChatAdapter(acc.provider, acc.id)
        cid = "c-retry-edit"

        class _NoUserIdRetryAdapter(_ScriptedMessageAdapter):
            async def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderStreamEvent]:
                self.received_requests.append(request)
                yield MessageMetadata(
                    message_id="a-new",
                    user_message_id=None if request.action is not None else "u-real-1",
                )
                yield TextDelta(f"Reply:{request.action}")
                yield ResponseComplete()

        provider, _ = _build_runtime_provider(
            account=acc,
            chat_adapter=chat_adapter,
            message_adapter=_NoUserIdRetryAdapter(chat_adapter),
        )
        try:
            await provider.open()
            chat = await provider.chats.create("Carrier check")
            chat.remote_id = cid
            chat_adapter.conversations[cid] = Conversation(
                id=cid,
                title="Carrier check",
                messages=(
                    MessageData(
                        id="u-real-1",
                        role="user",
                        content=(Content(kind="text", text="Original prompt"),),
                    ),
                    MessageData(
                        id="a-1",
                        role="assistant",
                        content=(Content(kind="text", text="Initial reply"),),
                    ),
                ),
            )
            op = chat.messages.create("Next prompt")
            await op.start()
            self.assertEqual(op.message.id, "u-real-1")
            self.assertEqual(op.message.remote_id, "u-real-1")

            msgs = await chat.messages.refresh()
            retried = await msgs[-1].retry().start()
            self.assertTrue(retried.text.startswith("Reply:"))
            refreshed = await chat.messages.refresh()
            self.assertEqual([m.id for m in refreshed if m.role == "user"], ["u-real-1"])
        finally:
            await provider.close()


class TestProjectAttachmentsView(unittest.IsolatedAsyncioTestCase):
    async def test_project_attachments_projects_file_resources_and_supports_v2_queries(
        self,
    ) -> None:
        acc = _account()
        project_adapter = _ScriptedProjectAdapter(acc.provider, acc.id)
        project_adapter.seed_project(
            "p-1",
            "Workspace",
            resources=(
                Attachment(
                    id="r-file-1",
                    name="alpha.md",
                    media_type="text/markdown",
                    metadata={"type": "file"},
                ),
                Attachment(
                    id="r-mem-1",
                    name="pin",
                    media_type="application/json",
                    metadata={"kind": "memory"},
                ),
                Attachment(
                    id="r-file-2",
                    name="alpha-notes.md",
                    media_type="text/markdown",
                    metadata={"type": "file"},
                ),
            ),
            chats=[Conversation(id="c-p1", title="In Project")],
        )
        provider, _ = _build_runtime_provider(account=acc, project_adapter=project_adapter)
        try:
            await provider.open()
            project = await provider.projects.get("p-1")

            atts = await project.attachments.list(limit=None)
            self.assertEqual([type(a) for a in atts], [Attachment, Attachment])
            self.assertEqual([a.id for a in atts], ["r-file-1", "r-file-2"])

            self.assertEqual((await project.attachments.first("alpha")).id, "r-file-1")
            self.assertEqual((await project.attachments.last("alpha")).id, "r-file-2")
            with self.assertRaises(AmbiguousMatchError):
                await project.attachments.find("alpha", startswith=True)
            self.assertEqual((await project.attachments.get("r-file-2")).name, "alpha-notes.md")

            with tempfile.TemporaryDirectory() as tmp:
                file_path = Path(tmp) / "spec.txt"
                file_path.write_text("hello", encoding="utf-8")
                added = await project.attachments.add(file_path)
                self.assertIsInstance(added, Attachment)
                self.assertEqual(added.name, "spec.txt")
                self.assertEqual(len(project_adapter.upload_calls), 1)
                removed = await project.attachments.remove(added)
                self.assertTrue(removed)
                self.assertEqual(len(project_adapter.remove_calls), 1)

            proj_chats = await project.chats.list(limit=10)
            self.assertEqual([c.remote_id for c in proj_chats], ["c-p1"])
            self.assertEqual(await project.chats.list(limit=0), [])
        finally:
            await provider.close()


class TestRuntimeHooksEventsAndLifecycle(unittest.IsolatedAsyncioTestCase):
    async def test_before_hooks_abort_and_rewrite_and_lifecycle_errors_are_isolated(self) -> None:
        acc = _account()
        chat_adapter = _ScriptedChatAdapter(acc.provider, acc.id)
        msg_adapter = _ScriptedMessageAdapter(chat_adapter)
        provider, _ = _build_runtime_provider(
            account=acc, chat_adapter=chat_adapter, message_adapter=msg_adapter
        )
        stages: list[str] = []

        @provider.hooks.before("chat.update")
        def _abort_update(ctx: OperationContext) -> None:
            stages.append(f"before:{ctx.name}")
            raise OperationCancelledError("Blocked update")

        @provider.hooks.on_cancelled("chat.update")
        def _update_cancelled(ctx: OperationContext) -> None:
            stages.append(f"cancelled:{ctx.cancelled}")

        @provider.hooks.on_success("chat.update")
        def _update_success(_ctx: OperationContext) -> None:
            stages.append("should_not_run")

        @provider.hooks.on_finally("chat.update")
        def _update_finally(ctx: OperationContext) -> None:
            stages.append(f"finally:{isinstance(ctx.error, OperationCancelledError)}")

        @provider.hooks.before("chat.send")
        def _rewrite_prompt(ctx: OperationContext) -> OperationContext:
            return replace(ctx, prompt=f"[guarded] {ctx.prompt}")

        seen_send_results: list[HookDispatchResult] = []

        @provider.hooks.on_success("chat.send")
        def _failing_success_hook(_ctx: OperationContext) -> None:
            raise RuntimeError("boom in user hook")

        @provider.hooks.on_finally("chat.send")
        def _inspect_results(ctx: OperationContext) -> None:
            res = ctx.metadata.get("hook_results", {}).get("on_success")
            if isinstance(res, HookDispatchResult):
                seen_send_results.append(res)

        try:
            await provider.open()
            chat_adapter.seed_paginated(1, page_size=50)
            chat = await provider.chats.get("c-0")

            with self.assertRaises(OperationCancelledError):
                await provider.chats.update(chat, title="Denied")
            self.assertEqual(chat_adapter.rename_calls, [])
            self.assertEqual(stages, ["before:chat.update", "cancelled:True", "finally:True"])

            # Send prompt rewritten by before hook & isolated failing on_success hook
            resp = await chat.messages.create("hello world").start()
            self.assertEqual(msg_adapter.received_requests[-1].prompt, "[guarded] hello world")
            self.assertEqual(resp.text, "Echo: [guarded] hello world")
            self.assertEqual(len(seen_send_results), 1)
            self.assertEqual(seen_send_results[0].matched, 1)
            self.assertEqual(seen_send_results[0].failed, 1)
            self.assertEqual(len(seen_send_results[0].errors), 1)
        finally:
            await provider.close()

    async def test_lifecycle_transitions_and_ordered_provider_events(self) -> None:
        acc = _account()
        session = _RecordingSessionAdapter(fail_prepare=ProviderError("auth expired"))
        provider, _ = _build_runtime_provider(account=acc, session_adapter=session)
        events: list[EventEnvelope[str]] = []

        async def record_event(event: EventEnvelope[str]) -> None:
            events.append(event)

        await provider.events.subscribe(record_event)

        self.assertEqual(provider.lifecycle_state, LifecycleState.CREATED)
        with self.assertRaises(ProviderError):
            await provider.open()
        self.assertEqual(provider.lifecycle_state, LifecycleState.OPEN_FAILED)

        session.fail_prepare = None
        await provider.open()
        self.assertEqual(provider.lifecycle_state, LifecycleState.OPEN)
        await provider.close()
        self.assertEqual(provider.lifecycle_state, LifecycleState.CLOSED)

        names = [event.event for event in events]
        self.assertEqual(
            names,
            [
                "provider.opening",
                "provider.open_failed",
                "provider.opening",
                "provider.opened",
                "provider.closing",
                "provider.closed",
            ],
        )
        sequences = [event.metadata["sequence"] for event in events]
        self.assertEqual(sequences, sorted(sequences))
        self.assertEqual(len(set(sequences)), len(sequences))


class TestSyncFacadeOffLoopAndConcurrency(unittest.TestCase):
    def test_sync_off_loop_callback_reenters_provider_and_concurrent_threads_succeed(self) -> None:
        acc = _account()
        chat_adapter = _ScriptedChatAdapter(acc.provider, acc.id)
        chat_adapter.seed_paginated(5, page_size=50)
        adapter = _build_adapter(acc, chat_adapter=chat_adapter)
        sync_provider = Provider(acc, adapter=adapter)

        callback_done = threading.Event()
        callback_threads: list[int] = []
        reentered_ids: list[list[str]] = []

        self.assertIsInstance(sync_provider.events, EventBus)
        self.assertIsInstance(sync_provider.hooks, SyncOperationHooks)
        self.assertIsInstance(sync_provider._provider.events, AsyncEventBus)

        def on_opened(_event: EventEnvelope[str]) -> None:
            callback_threads.append(threading.get_ident())
            chats = sync_provider.chats.list(limit=3)
            reentered_ids.append([c.remote_id for c in chats])
            callback_done.set()

        sync_provider.events.subscribe(
            on_opened,
            event_filter=EventFilter(
                event_type=EventEnvelope,
                predicate=lambda event: event.event == "provider.opened",
            ),
        )
        sync_provider.open()
        try:
            self.assertTrue(callback_done.wait(timeout=3.0))
            self.assertEqual(reentered_ids, [["c-0", "c-1", "c-2"]])
            self.assertEqual(len(chat_adapter.loop_threads), 1)
            self.assertNotIn(callback_threads[0], chat_adapter.loop_threads)

            # Multi-thread sync facade concurrency sharing single runtime
            def worker(idx: int) -> str:
                resp = sync_provider.ask(f"ping-{idx}")
                return resp.text

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                outputs = list(pool.map(worker, range(6)))
            self.assertEqual(sorted(outputs), [f"Echo: ping-{i}" for i in range(6)])
        finally:
            sync_provider.close()
            self.assertEqual(sync_provider.lifecycle_state, LifecycleState.CLOSED)


class TestAuditRegressions(unittest.IsolatedAsyncioTestCase):
    async def test_p1_01_find_raises_ambiguous_when_exact_and_partial_match(self) -> None:
        acc = _account()
        chat_adapter = _ScriptedChatAdapter(acc.provider, acc.id)
        c_exact = Conversation(id="c-exact", title="Python")
        c_partial = Conversation(id="c-partial", title="Python Eğitimi")
        chat_adapter.pages = [([c_exact, c_partial], None)]
        chat_adapter.conversations = {"c-exact": c_exact, "c-partial": c_partial}
        provider, _ = _build_runtime_provider(account=acc, chat_adapter=chat_adapter)
        await provider.open()
        try:
            with self.assertRaises(AmbiguousMatchError):
                await provider.chats.find("Python")
            exact = await provider.chats.find("Python", exact=True)
            self.assertEqual(exact.remote_id, "c-exact")
        finally:
            await provider.close()

    async def test_p1_02_chat_prepared_vs_created_on_conversation_bound(self) -> None:
        provider, _ = _build_runtime_provider()
        seen: list[tuple[str, str | None]] = []

        async def record_event(event: EventEnvelope[str]) -> None:
            seen.append((event.event, event.metadata.get("resource_id")))

        await provider.events.subscribe(
            record_event,
            event_filter=EventFilter(
                event_type=EventEnvelope,
                predicate=lambda event: event.event in {"chat.prepared", "chat.created"},
            ),
        )
        await provider.open()
        try:
            chat = await provider.chats.create("Yeni")
            self.assertEqual(seen, [("chat.prepared", chat.id)])

            await chat.messages.create("ilk mesaj")
            self.assertEqual(len(seen), 2)
            self.assertEqual(seen[1][0], "chat.created")
            self.assertEqual(seen[1][1], chat.remote_id)

            await chat.messages.create("ikinci mesaj")
            self.assertEqual(len(seen), 2)
        finally:
            await provider.close()

    async def test_p1_03_async_lifecycle_callbacks_do_not_deadlock(self) -> None:
        acc = _account()
        chat_adapter = _ScriptedChatAdapter(acc.provider, acc.id)
        chat_adapter.seed_paginated(2, page_size=10)
        provider, _ = _build_runtime_provider(account=acc, chat_adapter=chat_adapter)

        results: dict[str, object] = {}

        async def on_opening(_event: EventEnvelope[str]) -> None:
            await provider.open()
            results["opening_open"] = True

        async def on_opened(_event: EventEnvelope[str]) -> None:
            results["opened_chats"] = len(await provider.chats.list())

        async def on_closing(_event: EventEnvelope[str]) -> None:
            try:
                await provider.close()
            except ProviderError as exc:
                results["closing_error"] = str(exc)

        await provider.events.subscribe(
            on_opening,
            event_filter=EventFilter(
                event_type=EventEnvelope, predicate=lambda e: e.event == "provider.opening"
            ),
        )
        await provider.events.subscribe(
            on_opened,
            event_filter=EventFilter(
                event_type=EventEnvelope, predicate=lambda e: e.event == "provider.opened"
            ),
        )
        await provider.events.subscribe(
            on_closing,
            event_filter=EventFilter(
                event_type=EventEnvelope, predicate=lambda e: e.event == "provider.closing"
            ),
        )

        await provider.open()
        self.assertEqual(results.get("opening_open"), True)
        self.assertEqual(results.get("opened_chats"), 2)
        await provider.close()
        self.assertIn("closing_error", results)

    async def test_p1_04_close_failed_preserves_scope_and_allows_retry(self) -> None:
        provider, _ = _build_runtime_provider()
        await provider.open()

        attempts = 0

        async def flaky_cleanup() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("cleanup transient failure")

        provider.runtime._scope.defer(flaky_cleanup)

        with self.assertRaises(Exception):
            await provider.close()
        self.assertEqual(provider.lifecycle_state, LifecycleState.CLOSE_FAILED)

        with self.assertRaises(ProviderError):
            await provider.open()

        await provider.close()
        self.assertEqual(attempts, 2)
        self.assertEqual(provider.lifecycle_state, LifecycleState.CLOSED)

    async def test_p1_05_success_event_error_does_not_fail_mutation_or_trigger_on_error(
        self,
    ) -> None:
        provider, _ = _build_runtime_provider()
        on_error_calls: list[str] = []

        async def broken_listener(_event: EventEnvelope[str]) -> None:
            raise RuntimeError("observer failed")

        async def record_error(ctx) -> None:
            on_error_calls.append(ctx.operation_name)

        await provider.events.subscribe(
            broken_listener,
            event_filter=EventFilter(
                event_type=EventEnvelope, predicate=lambda e: e.event == "chat.prepared"
            ),
        )
        provider.hooks.on_error(record_error)

        await provider.open()
        try:
            chat = await provider.chats.create("Safe Chat")
            self.assertEqual(chat.title, "Safe Chat")
            self.assertEqual(on_error_calls, [])
        finally:
            await provider.close()

    async def test_p2_02_project_attachments_hooks_wire_into_add_and_remove(self) -> None:
        provider, _ = _build_runtime_provider()
        await provider.open()
        try:
            project = await provider.projects.create("Files")
            hook_ops: list[str] = []

            async def before(ctx) -> None:
                hook_ops.append(f"before:{ctx.resource_id}")

            async def after(ctx) -> None:
                hook_ops.append(f"after:{ctx.resource_id}")

            project.attachments.hooks.before(before)
            project.attachments.hooks.on_success(after)

            with tempfile.NamedTemporaryFile(suffix=".txt") as tmp:
                tmp.write(b"hello")
                tmp.flush()
                att = await project.attachments.add(tmp.name)
                removed = await project.attachments.remove(att)
                self.assertTrue(removed)

            target_id = project.remote_id or project.id
            self.assertEqual(
                hook_ops,
                [
                    f"before:{target_id}",
                    f"after:{target_id}",
                    f"before:{target_id}",
                    f"after:{target_id}",
                ],
            )
        finally:
            await provider.close()

    async def test_p2_03_and_p2_04_messages_search_prune_and_remote_delete_guard(self) -> None:
        acc = _account()
        chat_adapter = _ScriptedChatAdapter(acc.provider, acc.id)
        chat_adapter.conversations["c-hist"] = Conversation(
            id="c-hist",
            title="Hist",
            messages=(
                MessageData(
                    id="u-1", role="user", content=(Content(kind="text", text="alpha question"),)
                ),
                MessageData(
                    id="a-1", role="assistant", content=(Content(kind="text", text="beta answer"),)
                ),
            ),
        )
        provider, _ = _build_runtime_provider(account=acc, chat_adapter=chat_adapter)
        await provider.open()
        try:
            chat = await provider.chats.get("c-hist")
            found = await chat.messages.search("beta")
            self.assertEqual([m.id for m in found], ["a-1"])
            self.assertEqual((await chat.messages.find("alpha")).id, "u-1")

            with self.assertRaises(UnsupportedOperationError):
                await chat.messages.delete("a-1")

            from webaiprovider.resources.message.message import AsyncMessage

            local_msg = AsyncMessage(role="user", content="unsent draft", chat=chat)
            chat.messages._track_local_message(local_msg)
            await chat.messages.delete(local_msg)
            self.assertNotIn(local_msg.id, chat.messages._known_by_id)

            chat_adapter.conversations["c-hist"] = Conversation(
                id="c-hist",
                title="Hist",
                messages=(
                    MessageData(
                        id="u-1",
                        role="user",
                        content=(Content(kind="text", text="alpha question"),),
                    ),
                ),
            )
            await chat.messages.refresh()
            self.assertNotIn("a-1", chat.messages._known_by_id)
        finally:
            await provider.close()

    def test_p2_05_derived_collections_have_no_fake_fetch_page(self) -> None:
        from webaiprovider.resources.project.collections import AsyncProjectAttachments
        from webaiprovider.resources.response.collections import AsyncResponsesCollection

        self.assertFalse(hasattr(AsyncResponsesCollection, "_fetch_page"))
        self.assertFalse(hasattr(AsyncProjectAttachments, "_fetch_page"))

    def test_p2_06_provider_type_lives_in_base_without_provider_cycle(self) -> None:
        from webaiprovider.base import ProviderType as BaseProviderType
        from webaiprovider.provider import ProviderType as PublicProviderType

        self.assertIs(BaseProviderType, PublicProviderType)

    async def test_p2_07_sync_event_pool_overflow_records_drop_in_telemetry(self) -> None:
        provider, _ = _build_runtime_provider()
        async_seen: list[str] = []

        async def on_async(event: EventEnvelope[str]) -> None:
            async_seen.append(event.event)

        await provider.events.subscribe(on_async)
        provider.runtime.sync_events.subscribe(lambda _e: None)

        class _FullPool:
            def submit(self, *_args: object, **_kwargs: object) -> None:
                raise RuntimeError("pool full")

        provider.runtime._sync_event_pool = _FullPool()  # type: ignore[assignment]
        await provider.runtime.emit("custom.test")
        self.assertEqual(provider.runtime.telemetry.sync_event_drops, 1)
        self.assertEqual(provider.runtime.telemetry.snapshot()["sync_event_drops"], 1)
        self.assertEqual(async_seen, ["custom.test"])


if __name__ == "__main__":
    unittest.main()
