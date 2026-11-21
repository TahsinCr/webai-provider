"""Ordered content blocks in an assistant response."""

from dataclasses import dataclass

from ...collections import Collection


@dataclass(frozen=True, slots=True)
class ResponsePart:
    id: str


@dataclass(frozen=True, slots=True)
class ThinkingPart(ResponsePart):
    text: str


@dataclass(frozen=True, slots=True)
class TextPart(ResponsePart):
    text: str


class ResponseParts(Collection[ResponsePart]):
    """Read-only local view in the order the blocks first appeared."""

    @classmethod
    def _extract_name(cls, item: ResponsePart) -> str:
        if isinstance(item, (ThinkingPart, TextPart)):
            return item.text
        return super()._extract_name(item)
