"""Bounded incremental Server-Sent Events framing."""

import re
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from typing import NamedTuple

from ...errors import FrameDecodeError

_LINE_END = re.compile(rb"\r\n|\r|\n")


class SSEFrame(NamedTuple):
    data: str
    event: str = "message"
    id: str | None = None
    retry: int | None = None


class SSEFrameDecoder:
    def __init__(
        self,
        *,
        max_event_bytes: int = 2_000_000,
        max_data_lines: int = 65_536,
        utf8_errors: str = "replace",
    ):
        if max_event_bytes <= 0 or max_data_lines <= 0:
            raise ValueError("SSE frame limits must be positive")
        self.max_event_bytes = max_event_bytes
        self.max_data_lines = max_data_lines
        self.utf8_errors = utf8_errors
        self._line = bytearray()
        self._data = []
        self._size = 0
        self._skip_lf = False
        self._first_line = True
        self._event = "message"
        self._id = None
        self._retry = None

    def _append(self, piece):
        length = len(self._line)
        is_data = self._line.startswith(b"data:")
        if not is_data and length < 5:
            is_data = bytes(self._line) + piece[: 5 - length] == b"data:"
        if is_data:
            value_size = max(0, length + len(piece) - 5)
            start = max(0, 5 - length)
            first = self._line[5:6] if length > 5 else piece[start : start + 1]
            value_size -= first == b" "
            if self._size + bool(self._data) + value_size > self.max_event_bytes:
                raise FrameDecodeError("SSE event payload exceeds the size limit")
        if length + len(piece) > self.max_event_bytes + (9 if self._first_line else 6):
            raise FrameDecodeError("SSE line exceeds the size limit")
        self._line.extend(piece)

    def feed_line(self, line: bytes | str) -> Iterator[SSEFrame]:
        """Consume one line without its CR/LF terminator."""
        raw = line.encode("utf-8") if isinstance(line, str) else line
        if self._first_line:
            raw = raw.removeprefix(b"\xef\xbb\xbf")
            self._first_line = False
        if len(raw) > self.max_event_bytes + 6:
            raise FrameDecodeError("SSE line exceeds the size limit")
        if not raw:
            if self._data:
                yield SSEFrame("\n".join(self._data), self._event, self._id, self._retry)
                self._data.clear()
                self._size = 0
            self._event = "message"
            self._retry = None
            return
        if raw.startswith(b":"):
            return
        field, _, value = raw.partition(b":")
        if value.startswith(b" "):
            value = value[1:]
        if field == b"data":
            if len(self._data) >= self.max_data_lines:
                raise FrameDecodeError("SSE event exceeds the data line limit")
            value_size = len(value) + bool(self._data)
            if self._size + value_size > self.max_event_bytes:
                raise FrameDecodeError("SSE event payload exceeds the size limit")
            try:
                self._data.append(value.decode("utf-8", errors=self.utf8_errors))
            except UnicodeDecodeError as error:
                raise FrameDecodeError("SSE stream contains invalid UTF-8") from error
            self._size += value_size
        elif field == b"event":
            self._event = value.decode("utf-8", errors="replace") or "message"
        elif field == b"id" and b"\x00" not in value:
            self._id = value.decode("utf-8", errors="replace")
        elif field == b"retry" and value and all(48 <= char <= 57 for char in value):
            # Bound integer conversion independently of Python's digit limit.
            if len(value) <= 10:
                self._retry = int(value)

    def feed(self, chunk: bytes, *, final: bool = False) -> Iterator[SSEFrame]:
        offset = 0
        if self._skip_lf and chunk:
            self._skip_lf = False
            offset = int(chunk[0] == 10)
        if not final and chunk.find(b"\n", offset) < 0 and chunk.find(b"\r", offset) < 0:
            self._append(chunk[offset:])
            return
        for match in _LINE_END.finditer(chunk, offset):
            self._append(chunk[offset : match.start()])
            raw = bytes(self._line)
            self._line.clear()
            yield from self.feed_line(raw)
            offset = match.end()
            self._skip_lf = chunk[match.start()] == 13 and offset == len(chunk)
        if offset < len(chunk):
            self._append(chunk[offset:])
        if final:
            if self._line:
                raw = bytes(self._line)
                self._line.clear()
                yield from self.feed_line(raw)
            yield from self.feed_line(b"")

    def decode(self, chunks: Iterable[bytes]) -> Iterator[SSEFrame]:
        for chunk in chunks:
            yield from self.feed(chunk)
        yield from self.feed(b"", final=True)

    async def decode_async(self, chunks: AsyncIterable[bytes]) -> AsyncIterator[SSEFrame]:
        async for chunk in chunks:
            for frame in self.feed(chunk):
                yield frame
        for frame in self.feed(b"", final=True):
            yield frame


def iter_bounded_lines(
    chunks: Iterable[bytes],
    *,
    max_line_bytes: int = 1_048_576,
    max_stream_bytes: int = 64 * 1024 * 1024,
    error_label: str = "Stream",
) -> Iterator[str]:
    """Decode UTF-8 lines from byte chunks within strict line and total byte caps."""
    if max_line_bytes <= 0 or max_stream_bytes <= 0:
        raise ValueError("Stream line and byte limits must be positive")
    buffer = bytearray()
    total = 0
    for chunk in chunks:
        total += len(chunk)
        if total > max_stream_bytes:
            raise FrameDecodeError(f"{error_label} response exceeds the size limit")
        start = 0
        while (end := chunk.find(b"\n", start)) >= 0:
            if len(buffer) + end - start > max_line_bytes:
                raise FrameDecodeError(f"{error_label} response exceeds the size limit")
            buffer.extend(chunk[start:end])
            line = bytes(buffer)
            buffer.clear()
            if line.endswith(b"\r"):
                line = line[:-1]
            try:
                yield line.decode("utf-8")
            except UnicodeDecodeError:
                raise FrameDecodeError(f"{error_label} response format has changed") from None
            start = end + 1
        if len(buffer) + len(chunk) - start > max_line_bytes:
            raise FrameDecodeError(f"{error_label} response exceeds the size limit")
        buffer.extend(chunk[start:])
    if buffer:
        try:
            yield bytes(buffer).decode("utf-8")
        except UnicodeDecodeError:
            raise FrameDecodeError(f"{error_label} response format has changed") from None


async def aiter_bounded_lines(
    chunks: AsyncIterable[bytes],
    *,
    max_line_bytes: int = 1_048_576,
    max_stream_bytes: int = 64 * 1024 * 1024,
    error_label: str = "Stream",
) -> AsyncIterator[str]:
    """Decode UTF-8 lines from async byte chunks within strict line and total byte caps."""
    if max_line_bytes <= 0 or max_stream_bytes <= 0:
        raise ValueError("Stream line and byte limits must be positive")
    buffer = bytearray()
    total = 0
    async for chunk in chunks:
        total += len(chunk)
        if total > max_stream_bytes:
            raise FrameDecodeError(f"{error_label} response exceeds the size limit")
        start = 0
        while (end := chunk.find(b"\n", start)) >= 0:
            if len(buffer) + end - start > max_line_bytes:
                raise FrameDecodeError(f"{error_label} response exceeds the size limit")
            buffer.extend(chunk[start:end])
            line = bytes(buffer)
            buffer.clear()
            if line.endswith(b"\r"):
                line = line[:-1]
            try:
                yield line.decode("utf-8")
            except UnicodeDecodeError:
                raise FrameDecodeError(f"{error_label} response format has changed") from None
            start = end + 1
        if len(buffer) + len(chunk) - start > max_line_bytes:
            raise FrameDecodeError(f"{error_label} response exceeds the size limit")
        buffer.extend(chunk[start:])
    if buffer:
        try:
            yield bytes(buffer).decode("utf-8")
        except UnicodeDecodeError:
            raise FrameDecodeError(f"{error_label} response format has changed") from None
