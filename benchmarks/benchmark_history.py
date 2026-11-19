"""Synchronous and asynchronous chat opening and response reconstruction."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager

from webaiprovider import Account, AsyncProvider, Provider, ProviderType
from webaiprovider.resources.chat.types import Conversation
from webaiprovider.resources.message.types import Content, MessageData

from ._runner import Case, time_async, time_sync

_CHAT_ID = "benchmark-chat"
_RECENT = 25


def _history(messages: int) -> Conversation:
    text = (Content(kind="text", text="Synthetic conversation fixture"),)
    return Conversation(
        _CHAT_ID,
        "Benchmark",
        tuple(
            MessageData(str(index), "assistant" if index % 2 else "user", text)
            for index in range(messages)
        ),
    )


class _AsyncHistoryProvider(AsyncProvider):
    """Use the real public collections and replace the remote read boundary."""

    def __init__(self, account: Account, **options) -> None:
        super().__init__(account, **options)
        self.snapshot: Conversation

    async def _get_chat_data(self, conversation_id: str) -> Conversation:
        if conversation_id != self.snapshot.id:
            raise AssertionError("Unexpected benchmark chat")
        await asyncio.sleep(0)
        return self.snapshot


class _HistoryProvider(Provider):
    async_provider_type = _AsyncHistoryProvider


def _account() -> Account:
    return Account.from_cookies(ProviderType.CHATGPT, {})


@asynccontextmanager
async def _async_provider(messages: int):
    provider = _AsyncHistoryProvider(_account())
    provider.snapshot = _history(messages)
    async with provider:
        yield provider


@contextmanager
def _provider(messages: int):
    provider = _HistoryProvider(_account())
    provider._provider.snapshot = _history(messages)
    with provider:
        yield provider


def _open_case(*, blocking: bool, combined: bool, messages: int = 2_000) -> Case:
    name = (
        f"history.open.{'sync' if blocking else 'async'}.{'combined' if combined else 'separate'}"
    )

    if messages != 2_000:
        name = f"{name}.{messages}"

    async def measure_async(loops: int) -> float:
        async with _async_provider(messages) as provider:

            async def open_chat():
                if combined:
                    _, responses = await provider.chats.get_with_responses(_CHAT_ID, limit=_RECENT)
                else:
                    chat = await provider.chats.get(_CHAT_ID)
                    responses = await chat.responses.list(limit=_RECENT)
                if len(responses) != _RECENT:
                    raise AssertionError("Unexpected response count")

            return await time_async(loops, open_chat)

    def measure(loops: int) -> float:
        if not blocking:
            return asyncio.run(measure_async(loops))
        with _provider(messages) as provider:

            def open_chat():
                if combined:
                    _, responses = provider.chats.get_with_responses(_CHAT_ID, limit=_RECENT)
                else:
                    chat = provider.chats.get(_CHAT_ID)
                    responses = chat.responses.list(limit=_RECENT)
                if len(responses) != _RECENT:
                    raise AssertionError("Unexpected response count")

            return time_sync(loops, open_chat)

    return Case(name, measure, {"fixture_messages": messages, "response_limit": _RECENT})


def _reload_case(*, concurrent: bool, messages: int) -> Case:
    async def run(loops: int) -> float:
        async with _async_provider(messages) as provider:
            chat, _ = await provider.chats.get_with_responses(_CHAT_ID, limit=_RECENT)

            async def overlap():
                await asyncio.gather(*(chat.responses.list(limit=_RECENT) for _ in range(8)))

            return await time_async(
                loops,
                overlap if concurrent else lambda: chat.responses.list(limit=None, fresh=True),
            )

    return Case(
        "history.read.concurrent8" if concurrent else "history.list.fresh.20000",
        lambda loops: asyncio.run(run(loops)),
        {"fixture_messages": messages, "concurrent_reads": 8 if concurrent else 1},
    )


def cases() -> tuple[Case, ...]:
    return (
        *(
            _open_case(blocking=blocking, combined=combined)
            for blocking in (False, True)
            for combined in (False, True)
        ),
        _open_case(blocking=False, combined=True, messages=20_000),
        _reload_case(concurrent=False, messages=20_000),
        _reload_case(concurrent=True, messages=2_000),
    )
