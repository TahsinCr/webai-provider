from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError

from webaiprovider.resources.message.operation import (
    ArtifactDiscovered,
    OutputBegin,
    OutputDelta,
    OutputEnd,
    OutputReplace,
    SourceDiscovered,
    StreamEvent,
    TerminalStatus,
    ThinkingBegin,
    ThinkingDelta,
    ThinkingEnd,
)
from webaiprovider.resources.message.response import (
    Artifact,
    Attribution,
    Source,
    TimingMetrics,
)
from webaiprovider.resources.model.model import (
    EffortRef,
    EffortSet,
    ModelRef,
)


class ContractsTests(unittest.TestCase):
    def test_model_and_effort_ref_immutability(self) -> None:
        model = ModelRef("chatgpt", "model-id", "Test model", efforts=EffortSet)
        effort = EffortRef("chatgpt", "model-id", "medium", "Medium", rank=4)

        self.assertEqual(model.provider, "chatgpt")
        self.assertEqual(model.id, "model-id")
        self.assertEqual(effort.model_id, model.id)
        self.assertEqual(effort.rank, 4)

        with self.assertRaises(FrozenInstanceError):
            model.id = "other"  # type: ignore[misc]

        with self.assertRaises(FrozenInstanceError):
            effort.id = "high"  # type: ignore[misc]

    def test_source_and_attribution_data(self) -> None:
        source = Source("s1", "https://example.com/doc", title="Document", snippet="snippet text")
        attr = Attribution("s1", 10, 25, "relevant excerpt")

        self.assertEqual(source.url, "https://example.com/doc")
        self.assertEqual(attr.source_id, "s1")
        self.assertEqual(attr.start_index, 10)
        self.assertEqual(attr.end_index, 25)

    def test_artifact_properties(self) -> None:
        art = Artifact("a1", "code", "solution.py", mime_type="text/x-python", content=b"print(1)")
        self.assertEqual(art.name, "solution.py")
        self.assertEqual(art.content, b"print(1)")

    def test_stream_events_hierarchy(self) -> None:
        tb = ThinkingBegin()
        td = ThinkingDelta("thought...")
        te = ThinkingEnd("finished thoughts")
        ob = OutputBegin()
        od = OutputDelta("hello")
        orepl = OutputReplace("full replacement")
        oe = OutputEnd("final text")
        sd = SourceDiscovered(Source("s1", "https://example.com"))
        ad = ArtifactDiscovered(Artifact("a1", "file", "data.csv"))
        term = TerminalStatus("completed")

        self.assertIsInstance(tb, StreamEvent)
        self.assertEqual(tb.event, "thinking.begin")
        self.assertEqual(td.delta, "thought...")
        self.assertEqual(te.text, "finished thoughts")
        self.assertEqual(ob.event, "output.begin")
        self.assertEqual(od.delta, "hello")
        self.assertEqual(orepl.text, "full replacement")
        self.assertEqual(oe.text, "final text")
        self.assertEqual(sd.source.id, "s1")
        self.assertEqual(ad.artifact.name, "data.csv")
        self.assertEqual(term.status, "completed")

    def test_timing_metrics_default(self) -> None:
        timing = TimingMetrics(started_at=100.0, duration_ms=250.0)
        self.assertEqual(timing.started_at, 100.0)
        self.assertEqual(timing.duration_ms, 250.0)
        self.assertIsNone(timing.first_token_at)
