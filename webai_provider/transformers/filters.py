from __future__ import annotations

import re

_PROTOCOL_TAG_PATTERN = re.compile(r"<([a-zA-Z0-9_\-]+)(?:\s+[^>]*)?>.*?</\1>", re.DOTALL)
_RENDER_MARKUP_PATTERN = re.compile(r"(\x1b\[[0-9;]*[a-zA-Z]|<!--.*?-->)", re.DOTALL)


class GrokTextFilter:
    def __init__(self):
        self._inside_tag = False
        self._inside_quote: str | None = None
        self._buffer: list[str] = []

    def feed(self, text: str) -> str:
        output: list[str] = []
        for char in text:
            if self._inside_tag:
                if self._inside_quote:
                    if char == self._inside_quote:
                        self._inside_quote = None
                else:
                    if char in ("'", '"'):
                        self._inside_quote = char
                    elif char == ">":
                        self._inside_tag = False
                continue

            if char == "<":
                self._inside_tag = True
                self._inside_quote = None
                continue

            output.append(char)
        return "".join(output)

    def finish(self) -> str:
        self._inside_tag = False
        self._inside_quote = None
        return ""


def strip_protocol_tags(text: str, *tags: str) -> str:
    if not tags:
        return _PROTOCOL_TAG_PATTERN.sub("", text)
    tag_union = "|".join(re.escape(t) for t in tags)
    pattern = re.compile(rf"<({tag_union})(?:\s+[^>]*)?>.*?</\1>", re.DOTALL)
    return pattern.sub("", text)


def clean_render_markup(text: str) -> str:
    cleaned = _RENDER_MARKUP_PATTERN.sub("", text)
    return cleaned.strip()
