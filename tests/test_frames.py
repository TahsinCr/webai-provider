"""Protocol framing parity across byte boundaries and resource limits."""

import json
import unittest

from webaiprovider.errors import FrameDecodeError
from webaiprovider.frames import JSPBFrameDecoder, SSEFrameDecoder


def sse_data(chunks, **limits):
    return [frame.data for frame in SSEFrameDecoder(**limits).decode(chunks)]


class FrameTests(unittest.TestCase):
    def test_jspb_defaults_accept_plain_packets_without_vendor_prefix(self):
        packet = [["hello", "🚀"]]
        payload = json.dumps(packet, ensure_ascii=False)
        wire = f"{len(payload.encode('utf-16-le')) // 2}\n{payload}".encode()
        for split in range(len(wire) + 1):
            with self.subTest(split=split):
                decoder = JSPBFrameDecoder()
                frames = list(decoder.feed(wire[:split]))
                frames.extend(decoder.feed(wire[split:], final=True))
                self.assertEqual(frames, [packet])

    def test_sse_fields_and_utf8_survive_every_chunk_boundary(self):
        wire = (
            "\ufeff:keepalive\r\nid: 123\revent: delta\nretry: 50\ndata: Türkçe 🚀\r\n\r\n".encode()
        )
        for split in range(len(wire) + 1):
            with self.subTest(split=split):
                frames = list(SSEFrameDecoder().decode((wire[:split], wire[split:])))
                self.assertEqual(len(frames), 1)
                self.assertEqual(
                    (frames[0].data, frames[0].event, frames[0].id, frames[0].retry),
                    ("Türkçe 🚀", "delta", "123", 50),
                )

    def test_sse_strict_utf8_and_final_unterminated_data(self):
        self.assertEqual(sse_data((b"data: final",)), ["final"])
        with self.assertRaisesRegex(FrameDecodeError, "UTF-8"):
            list(SSEFrameDecoder(utf8_errors="strict").decode((b"data: \xff\n\n",)))

    def test_frames_handle_utf8_boundaries_lengths_and_multiple_packets(self):
        frames = [
            [["wrb.fr", None, json.dumps(["Türkçe 🛰️"])]],
            [["wrb.fr", None, json.dumps(["ikinci çerçeve"])]],
        ]

        def encode(rows):
            payload = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
            size = len(("\n" + payload).encode("utf-16-le")) // 2
            return f"{size}\n{payload}"

        wire = (")]}'\n\n" + "".join(encode(frame) for frame in frames) + "\n").encode()
        parser = JSPBFrameDecoder(prefix=")]}'", length_includes_separator=True)
        output = []
        for byte in wire:
            output.extend(parser.feed(bytes([byte])))
        output.extend(parser.feed(b"", final=True))
        self.assertEqual(output, frames)
        self.assertEqual(parser.pending, "")

    def test_frame_size_and_invalid_json_are_rejected(self):
        with self.assertRaises(FrameDecodeError):
            list(JSPBFrameDecoder(prefix=")]}'", length_includes_separator=True).feed(b"8388609\n"))
        with self.assertRaises(FrameDecodeError):
            list(
                JSPBFrameDecoder(prefix=")]}'", length_includes_separator=True).feed(
                    b"10\n[[invalid", final=True
                )
            )

    def test_frames_fast_path_and_astral_planes(self):

        def encode(rows):
            payload = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
            size = len(("\n" + payload).encode("utf-16-le")) // 2
            return f"{size}\n{payload}"

        frames = JSPBFrameDecoder(prefix=")]}'", length_includes_separator=True)
        frame = [["test", "Merhaba dünya! 🚀"]]
        wire = (")]}'\n\n" + encode(frame) + "\n").encode()
        parsed = list(frames.feed(wire, final=True))
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0][0][1], "Merhaba dünya! 🚀")

    def test_sse_parser_keeps_valid_json_data_lines_in_one_event(self):

        payload = b'data: {"a":1}\ndata: {"b":2}\n\n'
        self.assertEqual(list(sse_data([payload])), ['{"a":1}\n{"b":2}'])

    def test_sse_event_limit_counts_utf8_bytes(self):
        chunks = (
            b"data: ",
            ("é" * 600_000).encode(),
            b"\n",
            b"data: ",
            ("é" * 400_001).encode(),
            b"\n\n",
        )

        with self.assertRaisesRegex(FrameDecodeError, "size limit"):
            list(sse_data(chunks))

    def test_sse_parser_handles_split_data_prefix_and_crlf(self):
        chunks = (b"da", b'ta:{"text":"hello"}\r', b"\n\r", b"\n")

        self.assertEqual(list(sse_data(chunks)), ['{"text":"hello"}'])

    def test_sse_event_line_limit_bounds_empty_data_lines(self):
        chunks = (b"data:\n" * 4) + b"\n"

        with self.assertRaisesRegex(FrameDecodeError, "data line limit"):
            sse_data((chunks,), max_data_lines=2)

    def test_sse_event_limit_stops_before_consuming_an_unbounded_line(self):
        consumed = 0

        def chunks():
            nonlocal consumed
            yield b"data: "
            for _ in range(100):
                consumed += 1
                yield b"x" * (64 * 1024)

        with self.assertRaisesRegex(FrameDecodeError, "size limit"):
            list(sse_data(chunks()))

        self.assertLess(consumed, 100)
