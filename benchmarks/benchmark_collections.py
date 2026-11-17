"""Synthetic pagination, project fanout and bounded shared workers."""

from __future__ import annotations

import asyncio
from functools import partial

from eventflow import EventEnvelope, EventFilter

from webaiprovider import Account, AsyncProvider, Capability, ProviderType
from webaiprovider.collections import Page
from webaiprovider.core.concurrency import gather_bounded
from webaiprovider.resources.chat.types import Conversation
from webaiprovider.resources.project.types import ProjectData

from ._runner import Case, async_case, time_async


class _PagedProvider(AsyncProvider):
    capabilities = frozenset({Capability.CHAT, Capability.PROJECTS})

    def __init__(self, count: int) -> None:
        super().__init__(Account.from_cookies(ProviderType.CHATGPT, {}))
        self.records = tuple(Conversation(str(index), f"Chat {index}") for index in range(count))
        self.project_records = tuple(ProjectData(f"p{index}", "Project") for index in range(8))

    async def _list_chats_data(self, *, cursor=None, limit=50):
        start = int(cursor or 0)
        end = start + limit
        await asyncio.sleep(0)
        return Page(self.records[start:end], str(end) if end < len(self.records) else None)

    async def _list_projects_data(self, *, cursor=None):
        return Page(self.project_records)

    async def _list_project_chats_data(self, project_id, *, cursor=None):
        await asyncio.sleep(0)
        return Page((Conversation(f"{project_id}-chat", "Project chat"),))


def _pagination(count: int, *, projects: bool = False, observe: bool = False) -> Case:
    async def run(loops: int) -> float:
        async with _PagedProvider(count) as provider:
            observed = 0

            async def record(event):
                nonlocal observed
                if len(event.metadata["payload"]["items"]) != count:
                    raise AssertionError("Unexpected collection event item count")
                observed += 1

            if observe:
                await provider.events.subscribe(
                    record,
                    event_filter=EventFilter(
                        event_type=EventEnvelope,
                        predicate=lambda event: event.event == "collection.read.completed",
                    ),
                )

            async def read():
                result = await provider.chats.list(limit=None, include_projects=projects)
                if len(result) != count + (8 if projects else 0):
                    raise AssertionError("Unexpected paginated result count")

            elapsed = await time_async(loops, read)
            if observe and observed != loops:
                raise AssertionError("Expected one collection event per query")
            return elapsed

    label = f"collections.list.{count}"
    if projects:
        label += ".projects8"
    if observe:
        label += ".observed"
    return Case(label, lambda loops: asyncio.run(run(loops)), {"fixture_chats": count})


async def _work(index: int) -> int:
    await asyncio.sleep(0)
    return index


def cases() -> tuple[Case, ...]:
    return (
        _pagination(100),
        _pagination(1_000),
        _pagination(10_000),
        _pagination(1_000, projects=True),
        _pagination(1_000, observe=True),
        async_case(
            "collections.workers.10000.limit4",
            partial(gather_bounded, range(10_000), _work, limit=4),
            fixture_tasks=10_000,
            worker_limit=4,
        ),
    )
