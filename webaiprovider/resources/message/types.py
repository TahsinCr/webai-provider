from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from ...base import Capability
from ..common import ResourceData


class MessageAction(str, Enum):
    EDIT = "edit"
    RETRY = "retry"
    BRANCH = "branch"

    @property
    def capability(self) -> Capability:
        return {
            self.EDIT: Capability.MESSAGE_EDIT,
            self.RETRY: Capability.MESSAGE_RETRY,
            self.BRANCH: Capability.CHAT_BRANCH,
        }[self]


def actions_for(
    role: str, *, final: bool = True, latest: bool = True, branch: bool = False
) -> tuple[str, ...]:
    if not final:
        return ()
    if role == "user":
        return (MessageAction.EDIT.value,)
    if role != "assistant":
        return ()
    return (
        *((MessageAction.RETRY.value,) if latest else ()),
        *((MessageAction.BRANCH.value,) if branch else ()),
    )


def target_message(
    messages: tuple[MessageData, ...] | list[MessageData], message_id: str, role: str
) -> tuple[int, MessageData]:
    for index, message in enumerate(messages):
        if message.id == message_id:
            if message.role != role or message.channel == "analysis":
                break
            return index, message
    from ...errors import ProviderError

    raise ProviderError("The selected message is no longer on the active conversation path")


@dataclass(frozen=True)
class Content:
    kind: str
    text: str | None = None
    resource: ResourceData | None = None
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class MessageData:
    id: str
    role: str
    content: tuple[Content, ...]
    channel: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)
    actions: tuple[str, ...] = ()
