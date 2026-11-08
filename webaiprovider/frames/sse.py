"""Bounded incremental Server-Sent Events framing."""

from dataclasses import dataclass

from ..errors import FrameDecodeError


@dataclass(frozen=True)
class SSEFrame:
    data: str
    event: str = "message"
    id: str | None = None
    retry: int | None = None


class SSEFrameDecoder:
    def __init__(self, *, max_event_bytes=2_000_000, max_data_lines=65_536, utf8_errors="replace"):
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
        prefix = bytes(self._line[:5]) + piece[: max(0, 5 - length)]
        if prefix == b"data:":
            value_size = max(0, length + len(piece) - 5)
            first = self._line[5:6] if length > 5 else piece[max(0, 5 - length) :][:1]
            value_size -= first == b" "
            if self._size + value_size > self.max_event_bytes:
                raise FrameDecodeError("SSE event payload exceeds the size limit")
        if length + len(piece) > self.max_event_bytes + 6:
            raise FrameDecodeError("SSE line exceeds the size limit")
        self._line.extend(piece)

    def feed_line(self, line):
        """Consume one line without its CR/LF terminator."""
        raw = line.encode("utf-8") if isinstance(line, str) else line
        if len(raw) > self.max_event_bytes + 6:
            raise FrameDecodeError("SSE line exceeds the size limit")
        if self._first_line:
            raw = raw.removeprefix(b"\xef\xbb\xbf")
            self._first_line = False
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
            if self._size + len(value) > self.max_event_bytes:
                raise FrameDecodeError("SSE event payload exceeds the size limit")
            try:
                self._data.append(value.decode("utf-8", errors=self.utf8_errors))
            except UnicodeDecodeError as error:
                raise FrameDecodeError("SSE stream contains invalid UTF-8") from error
            self._size += len(value)
        elif field == b"event":
            self._event = value.decode("utf-8", errors="replace") or "message"
        elif field == b"id" and b"\x00" not in value:
            self._id = value.decode("utf-8", errors="replace")
        elif field == b"retry" and value and all(48 <= char <= 57 for char in value):
            # Bound integer conversion independently of Python's digit limit.
            if len(value) <= 10:
                self._retry = int(value)

    def feed(self, chunk, *, final=False):
        offset = 0
        while offset < len(chunk):
            if self._skip_lf:
                self._skip_lf = False
                if chunk[offset] == 10:
                    offset += 1
                    continue
            lf, cr = chunk.find(b"\n", offset), chunk.find(b"\r", offset)
            ends = [index for index in (lf, cr) if index >= 0]
            if not ends:
                self._append(chunk[offset:])
                break
            end = min(ends)
            self._append(chunk[offset:end])
            raw = bytes(self._line)
            self._line.clear()
            yield from self.feed_line(raw)
            offset = end + 1
            if chunk[end] == 13:
                self._skip_lf = True
        if final:
            if self._line:
                raw = bytes(self._line)
                self._line.clear()
                yield from self.feed_line(raw)
            yield from self.feed_line(b"")

    def decode(self, chunks):
        for chunk in chunks:
            yield from self.feed(chunk)
        yield from self.feed(b"", final=True)

    async def decode_lines(self, lines):
        async for line in lines:
            for frame in self.feed_line(line):
                yield frame
        for frame in self.feed(b"", final=True):
            yield frame
