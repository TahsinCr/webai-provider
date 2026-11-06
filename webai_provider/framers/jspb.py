from __future__ import annotations

import json
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from typing import Any

from ..errors import StreamError

_ANTI_HIJACK_PREFIX = ")]}'\n"


class JSPBFramer:
    def __init__(self, *, max_frame_bytes: int = 16 * 1024 * 1024):
        self._max_frame_bytes = max_frame_bytes
        self._buffer = ""
        self._prefix_stripped = False

    def feed(self, chunk: str | bytes) -> list[Any]:
        if isinstance(chunk, bytes):
            text = chunk.decode("utf-8", errors="replace")
        else:
            text = chunk

        self._buffer += text
        if not self._prefix_stripped:
            if self._buffer.startswith(_ANTI_HIJACK_PREFIX):
                self._buffer = self._buffer[len(_ANTI_HIJACK_PREFIX) :]
                self._prefix_stripped = True
            elif len(self._buffer) >= len(_ANTI_HIJACK_PREFIX):
                self._prefix_stripped = True

        frames: list[Any] = []
        while self._buffer:
            self._buffer = self._buffer.lstrip("\r\n ")
            if not self._buffer:
                break

            newline_index = self._buffer.find("\n")
            if newline_index != -1:
                first_line = self._buffer[:newline_index].strip()
                if first_line.isdigit():
                    expected_len = int(first_line)
                    start_payload = newline_index + 1
                    if len(self._buffer) < start_payload + expected_len:
                        break
                    payload_str = self._buffer[start_payload : start_payload + expected_len]
                    self._buffer = self._buffer[start_payload + expected_len :]
                    try:
                        parsed = json.loads(payload_str)
                        frames.append(parsed)
                        continue
                    except Exception as exc:
                        raise StreamError(f"Malformed JSPB payload: {exc}") from exc

            try:
                parsed, end_idx = json.JSONDecoder().raw_decode(self._buffer)
                frames.append(parsed)
                self._buffer = self._buffer[end_idx:]
            except json.JSONDecodeError:
                if len(self._buffer) > self._max_frame_bytes:
                    raise StreamError("JSPB frame length exceeded buffer limit")
                break
        return frames

    def finish(self) -> list[Any]:
        frames: list[Any] = []
        self._buffer = self._buffer.lstrip("\r\n ")
        if self._buffer:
            try:
                parsed = json.loads(self._buffer)
                frames.append(parsed)
                self._buffer = ""
            except Exception:
                pass
        return frames


def parse_jspb_chunks(chunks: Iterable[str | bytes]) -> Iterator[Any]:
    framer = JSPBFramer()
    for chunk in chunks:
        for frame in framer.feed(chunk):
            yield frame
    for frame in framer.finish():
        yield frame


async def parse_jspb_async(stream: AsyncIterable[str | bytes]) -> AsyncIterator[Any]:
    framer = JSPBFramer()
    async for chunk in stream:
        for frame in framer.feed(chunk):
            yield frame
    for frame in framer.finish():
        yield frame
