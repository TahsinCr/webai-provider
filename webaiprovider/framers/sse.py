from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from dataclasses import dataclass

from ..errors import StreamError

DEFAULT_MAX_EVENT_BYTES = 2 * 1024 * 1024
DEFAULT_MAX_LINE_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class SSEEvent:
    data: str
    event: str | None = None
    id: str | None = None
    retry: int | None = None


class SSEFramer:
    def __init__(
        self,
        *,
        max_event_bytes: int = DEFAULT_MAX_EVENT_BYTES,
        max_line_bytes: int = DEFAULT_MAX_LINE_BYTES,
    ):
        self._max_event_bytes = max_event_bytes
        self._max_line_bytes = max_line_bytes
        self._data_lines: list[str] = []
        self._event_type: str | None = None
        self._event_id: str | None = None
        self._retry: int | None = None
        self._total_data_bytes = 0
        self._buffer = bytearray()

    def feed(self, chunk: bytes | str) -> list[SSEEvent]:
        if isinstance(chunk, str):
            raw_bytes = chunk.encode("utf-8")
        else:
            raw_bytes = chunk

        self._buffer.extend(raw_bytes)
        events: list[SSEEvent] = []

        while True:
            nl = self._buffer.find(b"\n")
            cr = self._buffer.find(b"\r")
            if nl == -1 and cr == -1:
                if len(self._buffer) >= self._max_line_bytes:
                    raise StreamError("SSE line length exceeded buffer limit")
                break

            if cr != -1 and (nl == -1 or cr < nl):
                if cr == len(self._buffer) - 1:
                    if len(self._buffer) >= self._max_line_bytes:
                        raise StreamError("SSE line length exceeded buffer limit")
                    break
                line = self._buffer[:cr]
                if nl == cr + 1:
                    del self._buffer[: cr + 2]
                else:
                    del self._buffer[: cr + 1]
            else:
                line = self._buffer[:nl]
                del self._buffer[: nl + 1]

            if len(line) >= self._max_line_bytes:
                raise StreamError("SSE line length exceeded buffer limit")

            ev = self._process_line(bytes(line))
            if ev is not None:
                events.append(ev)

        return events

    def finish(self) -> list[SSEEvent]:
        events: list[SSEEvent] = []
        if self._buffer:
            trailing = bytes(self._buffer).rstrip(b"\r\n")
            self._buffer.clear()
            if trailing:
                ev = self._process_line(trailing)
                if ev is not None:
                    events.append(ev)
        if self._data_lines:
            events.append(self._dispatch_event())
        return events

    def _process_line(self, raw: bytes) -> SSEEvent | None:
        if not raw:
            if self._data_lines:
                return self._dispatch_event()
            return None

        if raw.startswith(b":"):
            return None

        line = raw.decode("utf-8", errors="replace")
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]

        if field == "data":
            value_bytes = len(value.encode("utf-8"))
            if self._total_data_bytes + value_bytes > self._max_event_bytes:
                raise StreamError("SSE event payload exceeded buffer limit")
            self._data_lines.append(value)
            self._total_data_bytes += value_bytes
        elif field == "event":
            self._event_type = value
        elif field == "id":
            if "\x00" not in value:
                self._event_id = value
        elif field == "retry":
            try:
                self._retry = int(value.strip())
            except ValueError:
                pass
        return None

    def _dispatch_event(self) -> SSEEvent:
        event = SSEEvent(
            data="\n".join(self._data_lines),
            event=self._event_type,
            id=self._event_id,
            retry=self._retry,
        )
        self._data_lines.clear()
        self._event_type = None
        self._total_data_bytes = 0
        return event


def parse_sse_chunks(chunks: Iterable[bytes | str]) -> Iterator[str]:
    framer = SSEFramer()
    for chunk in chunks:
        for event in framer.feed(chunk):
            if event.data:
                yield event.data
    for event in framer.finish():
        if event.data:
            yield event.data


async def parse_sse_async(chunks: AsyncIterable[bytes | str]) -> AsyncIterator[SSEEvent]:
    framer = SSEFramer()
    async for chunk in chunks:
        for event in framer.feed(chunk):
            yield event
    for event in framer.finish():
        yield event
