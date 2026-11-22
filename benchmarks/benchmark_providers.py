"""Provider history and message preparation with deterministic remote fixtures."""

from __future__ import annotations

import asyncio

from webaiprovider import Account, AsyncProvider
from webaiprovider.resources.message.protocol import ProviderRequest
from webaiprovider.resources.message.types import MessageAction

from ._runner import Case, time_async

_TURNS = 1_000
_CHAT = "c_fixture"


def _history_fixture(provider, *, rich: bool = False):
    if provider == "gemini":
        if rich:
            turns = []
            for i in reversed(range(_TURNS)):
                candidate: list[object] = [None] * 38
                candidate[0] = f"candidate_{i}"
                candidate[1] = [f"Answer {i} [cite: 1, 2]"]
                candidate[12] = [None] * 44
                candidate[12][43] = [
                    [
                        ["[cite: 1, 2]"],
                        [
                            [
                                None,
                                None,
                                None,
                                [[None, f"https://example.com/a/{i}", f"Title A {i}"]],
                            ],
                            [
                                None,
                                None,
                                None,
                                [[None, f"https://example.com/b/{i}", f"Title B {i}"]],
                            ],
                        ],
                    ]
                ]
                candidate[37] = [
                    [f"Thought summary {i}"],
                    [
                        [
                            None,
                            f"Search query {i}",
                            None,
                            None,
                            None,
                            None,
                            None,
                            [[None, f"https://example.com/s/{i}", f"Search {i}"]],
                        ]
                    ],
                ]
                turns.append([[_CHAT, f"r_{i}"], None, [[f"Question {i}"]], [[candidate]]])
            return [turns]
        turns = [
            [[_CHAT, f"r_{i}"], None, [["Question"]], [[[f"candidate_{i}", ["Answer"]]]]]
            for i in reversed(range(_TURNS))
        ]
        return [turns]
    if provider == "chatgpt":
        return {
            "id": _CHAT,
            "messages": [
                {
                    "id": f"message-{i}",
                    "author": {"role": "assistant" if i % 2 else "user"},
                    "content": {"parts": ["Answer" if i % 2 else "Question"]},
                }
                for i in range(_TURNS * 2)
            ],
        }
    if provider == "claude":
        return {
            "uuid": _CHAT,
            "chat_messages": [
                {
                    "uuid": f"message-{i}",
                    "sender": "assistant" if i % 2 else "human",
                    "text": "Answer" if i % 2 else "Question",
                }
                for i in range(_TURNS * 2)
            ],
        }
    return {
        "responses": [
            {
                "responseId": f"message-{i}",
                "sender": "assistant" if i % 2 else "human",
                "message": "Answer" if i % 2 else "Question",
                "parentResponseId": f"message-{i - 1}" if i else None,
            }
            for i in range(_TURNS * 2)
        ],
        "lastResponseId": f"message-{_TURNS * 2 - 1}",
    }


def _case(provider_name: str, *, retry: bool = False, rich: bool = False) -> Case:
    async def measure(loops: int) -> float:
        provider = AsyncProvider(Account.from_cookies(provider_name, {}))
        fixture = _history_fixture(provider_name, rich=rich)
        requests = 0

        async def fetch(*_args, **_kwargs):
            nonlocal requests
            requests += 1
            return fixture

        if provider_name == "gemini":

            async def rpc(*_args):
                return [await fetch()]

            provider.rpc.call = rpc
        elif provider_name == "chatgpt":
            from contextlib import asynccontextmanager
            from types import SimpleNamespace

            @asynccontextmanager
            async def client_scope():
                yield SimpleNamespace(get_conversation=fetch)

            provider._client_scope = client_scope
        elif provider_name == "claude":
            provider.client.request = fetch
        else:

            async def metadata(_chat):
                nonlocal requests
                requests += 1
                return {"id": _CHAT}

            provider._conversation_metadata = metadata
            provider._history = fetch

        if retry and provider_name == "claude":
            from webaiprovider.providers.claude.records import action_target

            message_id = f"message-{_TURNS + 1}:0"

            async def operation():
                target = action_target(await fetch(), message_id, MessageAction.RETRY)
                if target.id != message_id:
                    raise AssertionError("Unexpected retry target")
        elif retry:
            request = ProviderRequest(
                "",
                conversation_id=_CHAT,
                action=MessageAction.RETRY,
                message_id=f"r_{_TURNS // 2}:model",
            )

            async def operation():
                updated, metadata, _ = await provider._load_chat_continuation(request, None)
                if updated.prompt != "Question" or metadata[1] != f"r_{_TURNS // 2 - 1}":
                    raise AssertionError("Unexpected retry target")
        else:

            async def operation():
                chat = await provider._get_chat_data(_CHAT)
                expected = _TURNS * 3 if rich and provider_name == "gemini" else _TURNS * 2
                if len(chat.messages) != expected:
                    raise AssertionError("Unexpected history length")

        duration = await time_async(loops, operation)
        if requests != loops * (2 if provider_name == "grok" else 1):
            raise AssertionError("Unexpected history requests")
        return duration

    workload = "retry" if retry else ("history_rich" if rich else "history")
    return Case(
        f"providers.{provider_name}.{workload}.{_TURNS}",
        lambda loops: asyncio.run(measure(loops)),
        {"fixture_turns": _TURNS},
    )


def cases() -> tuple[Case, ...]:
    return (
        *(_case(name) for name in ("chatgpt", "gemini", "claude", "grok")),
        _case("gemini", rich=True),
        _case("gemini", retry=True),
        _case("claude", retry=True),
    )
