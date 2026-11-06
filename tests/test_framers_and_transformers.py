from __future__ import annotations

import unittest

from webai_provider.errors import StreamError
from webai_provider.framers.jspb import JSPBFramer, parse_jspb_chunks
from webai_provider.framers.sse import SSEFramer, parse_sse_chunks
from webai_provider.transformers.citations import extract_citations, sources_from
from webai_provider.transformers.filters import (
    GrokTextFilter,
    clean_render_markup,
    strip_protocol_tags,
)
from webai_provider.transformers.reconciler import StreamReconciler, reconcile_stream_text


class SSEFramerTests(unittest.TestCase):
    def test_parses_multiline_data_and_skips_comments(self):
        chunks = [
            b": ping\n",
            b"data: first line\n",
            b"data: second line\n\n",
        ]
        results = list(parse_sse_chunks(chunks))
        self.assertEqual(results, ["first line\nsecond line"])

    def test_parses_event_fields_and_ids(self):
        framer = SSEFramer()
        raw = "event: custom\nid: 12345\nretry: 5000\ndata: payload\n\n"
        events = framer.feed(raw)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event, "custom")
        self.assertEqual(events[0].id, "12345")
        self.assertEqual(events[0].retry, 5000)
        self.assertEqual(events[0].data, "payload")

    def test_enforces_event_size_limit(self):
        framer = SSEFramer(max_event_bytes=20)
        with self.assertRaises(StreamError):
            framer.feed("data: " + ("x" * 50) + "\n\n")


class JSPBFramerTests(unittest.TestCase):
    def test_strips_anti_hijacking_prefix_and_parses_length_prefixed(self):
        payload = b')]}\'\n20\n[[1, "hello", true]]'
        frames = list(parse_jspb_chunks([payload]))
        self.assertEqual(frames, [[[1, "hello", True]]])

    def test_parses_raw_json_chunks(self):
        framer = JSPBFramer()
        frames = framer.feed('[{"key": "val"}]')
        self.assertEqual(frames, [[{"key": "val"}]])


class TransformersTests(unittest.TestCase):
    def test_citations_extraction_and_deduplication(self):
        data = {
            "webSearchResults": [
                {"url": "https://example.com/1", "title": "First"},
                {"url": "https://example.com/1", "title": "Duplicate"},
                {"url": "https://example.com/2", "title": "Second"},
            ]
        }
        citations = extract_citations(data)
        self.assertEqual(len(citations), 2)
        self.assertEqual(citations[0].url, "https://example.com/1")
        self.assertEqual(citations[1].url, "https://example.com/2")

        sources = sources_from(data)
        self.assertEqual(len(sources), 2)
        self.assertEqual(sources[0]["title"], "First")

    def test_filters_grok_tag_preserves_quoted_brackets(self):
        filter_ = GrokTextFilter()
        raw = 'Hello <tag attr="val>ue">world</tag>!'
        filtered = filter_.feed(raw) + filter_.finish()
        self.assertEqual(filtered, "Hello world!")

    def test_protocol_tags_and_render_markup_cleaning(self):
        raw = "Normal text <thought>deep thinking</thought> after."
        cleaned = strip_protocol_tags(raw, "thought")
        self.assertEqual(cleaned, "Normal text  after.")

        ansi_and_comment = "Output \x1b[31mcolored\x1b[0m <!-- hidden -->"
        self.assertEqual(clean_render_markup(ansi_and_comment), "Output colored")

    def test_stream_reconciler_and_delta(self):
        reconciler = StreamReconciler()
        reconciler.append("Hello ")
        reconciler.append("World")
        delta = reconciler.reconcile("Hello World, how are you?")
        self.assertFalse(delta.needs_replace)
        self.assertEqual(delta.text, ", how are you?")

        replace_delta = reconciler.reconcile("Completely different text")
        self.assertTrue(replace_delta.needs_replace)
        self.assertEqual(replace_delta.text, "Completely different text")

        suffix = reconcile_stream_text("abc", "abcdef")
        self.assertEqual(suffix, "def")
