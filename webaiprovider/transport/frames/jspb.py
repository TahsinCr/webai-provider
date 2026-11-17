"""Length-prefixed UTF-16-counted JSON/JSPB framing."""

import codecs
import json
import re
from collections.abc import Iterator
from typing import Any

from ...errors import FrameDecodeError


class JSPBFrameDecoder:
    """Decode length-prefixed JSON/JSPB frames incrementally."""

    _LENGTH_MARKER = re.compile(r"(\d+)\n")

    def __init__(
        self,
        *,
        prefix: str = "",
        max_frame_units: int = 8 * 1024 * 1024,
        length_includes_separator: bool = False,
    ):
        if max_frame_units <= 0:
            raise ValueError("max_frame_units must be positive")
        self.prefix = prefix
        self.max_frame_units = max_frame_units
        self.length_includes_separator = length_includes_separator
        self.decoder = codecs.getincrementaldecoder("utf-8")()
        self.pending = ""
        self.expected_units = None
        self.payload_start = 0
        self.scanned_chars = 0
        self.scanned_units = 0
        self.prefix_checked = False

    def feed(self, data: bytes, final: bool = False) -> Iterator[Any]:
        try:
            self.pending += self.decoder.decode(data, final=final)
        except UnicodeDecodeError as error:
            raise FrameDecodeError("JSPB stream contains invalid UTF-8") from error

        self._strip_prefix()
        while self.expected_units is not None or self._read_length_marker():
            if not self._scan_payload():
                break

            end = self.payload_start + self.scanned_chars
            payload, self.pending = self.pending[self.payload_start : end], self.pending[end:]
            self._reset_frame()
            try:
                rows = json.loads(payload)
            except ValueError as error:
                raise FrameDecodeError("JSPB stream format has changed") from error
            yield rows

        if len(self.pending) > self.max_frame_units + 16:
            raise FrameDecodeError("JSPB stream frame exceeds the size limit")
        if final and self.pending.strip():
            raise FrameDecodeError("JSPB stream ended with an incomplete frame")

    def _strip_prefix(self):
        if self.prefix_checked:
            return
        if len(self.pending) < len(self.prefix) and self.prefix.startswith(self.pending):
            return
        if self.pending.startswith(self.prefix):
            self.pending = self.pending[len(self.prefix) :].lstrip()
        self.prefix_checked = True

    def _read_length_marker(self):
        if not self.prefix_checked:
            return False
        self.pending = self.pending.lstrip()
        if not self.pending:
            return False
        match = self._LENGTH_MARKER.match(self.pending)
        if match is None:
            if self.pending.isdecimal() and len(self.pending) <= 10:
                return False
            if self.pending[0].isdigit() and "\n" not in self.pending:
                raise FrameDecodeError("JSPB stream frame length is invalid")
            raise FrameDecodeError("JSPB stream frame length field is invalid")

        length = int(match.group(1))
        if not 1 <= length <= self.max_frame_units:
            raise FrameDecodeError("JSPB stream frame exceeds the size limit")
        self.expected_units = length
        # Some protocols include the separator in their UTF-16 frame length.
        self.payload_start = match.end() - int(self.length_includes_separator)
        self.scanned_chars = 0
        self.scanned_units = 0
        return True

    def _scan_payload(self) -> bool:
        expected = self.expected_units
        if expected is None:
            raise FrameDecodeError("JSPB frame length is missing")
        index = self.payload_start + self.scanned_chars
        needed_units = expected - self.scanned_units
        remaining_chars = len(self.pending) - index
        if remaining_chars > 0 and needed_units > 0:
            take_candidate = min(remaining_chars, needed_units)
            chunk = self.pending[index : index + take_candidate]
            if max(chunk) <= "\uffff":
                take_chars = len(chunk)
                self.scanned_chars += take_chars
                self.scanned_units += take_chars
                index += take_chars
        while self.scanned_units < expected and index < len(self.pending):
            units = 2 if ord(self.pending[index]) > 0xFFFF else 1
            if self.scanned_units + units > expected:
                raise FrameDecodeError("JSPB stream frame UTF-16 length is invalid")
            self.scanned_chars += 1
            self.scanned_units += units
            index += 1
        return self.scanned_units >= expected

    def _reset_frame(self):
        self.expected_units = None
        self.payload_start = 0
        self.scanned_chars = 0
        self.scanned_units = 0
