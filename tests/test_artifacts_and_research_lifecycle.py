from __future__ import annotations

import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from webaiprovider import (
    Artifact,
    OperationCancelledError,
    OperationEvent,
    ResearchPlan,
    ResearchState,
    ResearchStatus,
    Response,
    Source,
    UnsupportedOperationError,
)
from webaiprovider.resources.response.async_polling import async_poll_until
from webaiprovider.resources.response.collections import _build_artifacts, _build_sources
from webaiprovider.resources.response.polling import poll_until


class ArtifactsAndResearchLifecycleTests(unittest.TestCase):
    def test_inline_markdown_artifact_read_and_save(self) -> None:
        artifact = Artifact(
            id="doc-1",
            kind="document",
            name="report.md",
            mime_type="text/markdown",
            content="# Title\n\nBody",
            title="Report",
        )
        self.assertEqual(artifact.read_text(), "# Title\n\nBody")
        self.assertEqual(artifact.read_bytes(), b"# Title\n\nBody")
        with TemporaryDirectory() as tmpdir:
            saved = artifact.save(Path(tmpdir) / "report.md")
            self.assertEqual(saved.read_text(encoding="utf-8"), "# Title\n\nBody")

        remote_only = Artifact(
            id="img-1",
            kind="image",
            name="img.png",
            mime_type="image/png",
            download_url="https://example.com/img.png",
        )
        with self.assertRaises(ValueError):
            remote_only.read_bytes()

    def test_image_only_response_allows_empty_parts(self) -> None:
        img = Artifact(
            id="img-1",
            kind="image",
            name="cat.png",
            mime_type="image/png",
            width=1024,
            height=1024,
            download_url="https://example.com/cat.png",
        )
        resp = Response(parts=(), artifacts=[img])
        self.assertEqual(list(resp.parts), [])
        self.assertEqual(resp.text, "")
        self.assertEqual(len(resp.artifacts), 1)
        self.assertEqual(resp.artifacts[0].width, 1024)

    def test_build_artifacts_and_sources_preserve_new_fields(self) -> None:
        artifacts = _build_artifacts(
            None,
            [
                {
                    "id": "a1",
                    "kind": "image",
                    "name": "out.jpg",
                    "media_type": "image/jpeg",
                    "width": 800,
                    "height": 600,
                    "title": "Generated",
                    "download_url": "https://lh3.googleusercontent.com/gg/abc",
                }
            ],
            Artifact,
        )
        self.assertEqual(artifacts[0].width, 800)
        self.assertEqual(artifacts[0].height, 600)
        self.assertEqual(artifacts[0].title, "Generated")

        sources = _build_sources(
            [{"url": "https://example.com", "title": "Ex", "cited": False}],
            Source,
        )
        self.assertFalse(sources[0].cited)

    def test_research_events_and_detached_response_error(self) -> None:
        self.assertEqual(OperationEvent.RESEARCH_PLAN_READY.value, "research.plan.ready")
        self.assertEqual(OperationEvent.RESEARCH_PROGRESS.value, "research.progress")
        plan = ResearchPlan(plan_id="p1", title="AI", steps=("Step 1",), countdown_seconds=50.0)
        state = ResearchState(
            status=ResearchStatus.WAITING_FOR_APPROVAL,
            session_id="s1",
            plan=plan,
        )
        resp = Response("Plan ready", research=state)
        self.assertEqual(resp.research.plan.title, "AI")
        with self.assertRaises(UnsupportedOperationError):
            resp.start_research()

    def test_sync_poll_until_succeeds_cancels_and_times_out(self) -> None:
        calls = 0

        def step() -> str | None:
            nonlocal calls
            calls += 1
            return "done" if calls == 2 else None

        self.assertEqual(poll_until(step, timeout=2.0, backoff=(0.001,)), "done")

        cancel_event = threading.Event()
        cancel_event.set()
        with self.assertRaises(OperationCancelledError):
            poll_until(lambda: None, timeout=2.0, cancel_event=cancel_event)

        with self.assertRaises(TimeoutError):
            poll_until(lambda: None, timeout=0.01, backoff=(0.005,))


class AsyncPollingTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_poll_until_succeeds_and_cancels(self) -> None:
        calls = 0

        async def step() -> str | None:
            nonlocal calls
            calls += 1
            return "ok" if calls == 2 else None

        self.assertEqual(await async_poll_until(step, timeout=2.0, backoff=(0.001,)), "ok")

        with self.assertRaises(OperationCancelledError):
            await async_poll_until(
                lambda: None,  # type: ignore[arg-type,return-value]
                timeout=2.0,
                is_cancelled=lambda: True,
            )


if __name__ == "__main__":
    unittest.main()
