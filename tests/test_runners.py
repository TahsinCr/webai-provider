from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

import run_live_tests
import run_tests
from tests.live.support.config import PROVIDERS


def _fake_popen_factory(returncodes: list[int] | None = None):
    codes = list(returncodes or [])
    calls: list[dict[str, object]] = []

    class FakeStdout(io.StringIO):
        def close(self) -> None:
            pass

    class FakePopen:
        def __init__(self, command, **kwargs) -> None:
            calls.append({"args": (command,), "kwargs": kwargs})
            self.stdout = FakeStdout("ok\n")
            self._returncode = codes.pop(0) if codes else 0

        def wait(self) -> int:
            return self._returncode

    FakePopen.calls = calls
    return FakePopen


class TestRunnerTests(unittest.TestCase):
    def test_offline_runner_filters_live_package(self) -> None:
        root = Path(__file__).resolve().parents[1]
        suite = unittest.defaultTestLoader.discover(
            str(root / "tests"), pattern="test_*.py", top_level_dir=str(root)
        )
        offline = run_tests._without_live_tests(suite)

        def test_ids(suite: unittest.TestSuite) -> list[str]:
            return [
                item
                for test in suite
                for item in (
                    test_ids(test) if isinstance(test, unittest.TestSuite) else [test.id()]
                )
            ]

        ids = test_ids(offline)
        self.assertTrue(any(item.startswith("tests.test_") for item in ids))
        self.assertFalse(any(item.startswith("tests.live.") for item in ids))

    def test_default_live_matrix_covers_each_provider_with_async_api(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                run_live_tests._matrix(),
                tuple((provider, "async") for provider in sorted(PROVIDERS)),
            )

    def test_live_runner_runs_offline_suite_before_parallel_provider_matrix(self) -> None:
        result = CompletedProcess([], 0)
        popen_cls = _fake_popen_factory([0] * len(PROVIDERS))
        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.dict(os.environ, {}, clear=True),
                patch("run_live_tests.CLEANUP_FILE", Path(directory) / "absent.json"),
                patch("run_live_tests.subprocess.run", return_value=result) as run,
                patch("run_live_tests.subprocess.Popen", side_effect=popen_cls),
                patch("run_live_tests.time.sleep"),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(run_live_tests.main(), 0)

        self.assertEqual(run.call_count, 1)
        self.assertTrue(run.call_args.args[0][1].endswith("run_tests.py"))
        self.assertEqual(len(popen_cls.calls), len(PROVIDERS))
        first_call = popen_cls.calls[0]
        self.assertEqual(first_call["args"][0][2:6], ["unittest", "discover", "-s", "tests/live"])
        self.assertEqual(first_call["args"][0][-3:], ["-p", "test_*.py", "-v"])
        self.assertEqual(first_call["kwargs"]["env"]["WEBAI_LIVE_TESTS"], "1")
        seen_pairs = {
            (
                call["kwargs"]["env"]["WEBAI_LIVE_PROVIDER"],
                call["kwargs"]["env"]["WEBAI_LIVE_API"],
            )
            for call in popen_cls.calls
        }
        self.assertEqual(seen_pairs, {(provider, "async") for provider in PROVIDERS})

    def test_live_runner_continues_api_matrix_after_failures(self) -> None:
        result = CompletedProcess([], 0)
        popen_cls = _fake_popen_factory([1, 0, 0, 0])
        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.dict(os.environ, {}, clear=True),
                patch("run_live_tests.CLEANUP_FILE", Path(directory) / "absent.json"),
                patch("run_live_tests.subprocess.run", return_value=result),
                patch("run_live_tests.subprocess.Popen", side_effect=popen_cls),
                patch("run_live_tests.time.sleep"),
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(run_live_tests.main(), 1)

        self.assertEqual(len(popen_cls.calls), len(PROVIDERS))
        self.assertNotIn("-f", popen_cls.calls[0]["args"][0])
        seen_pairs = {
            (
                call["kwargs"]["env"]["WEBAI_LIVE_PROVIDER"],
                call["kwargs"]["env"]["WEBAI_LIVE_API"],
            )
            for call in popen_cls.calls
        }
        self.assertEqual(seen_pairs, {(provider, "async") for provider in PROVIDERS})

    def test_live_matrix_can_be_narrowed_to_one_provider_and_api(self) -> None:
        with patch.dict(
            os.environ,
            {"WEBAI_LIVE_PROVIDER": "GROK", "WEBAI_LIVE_API": "SYNC"},
            clear=True,
        ):
            self.assertEqual(run_live_tests._matrix(), (("grok", "sync"),))

    def test_recovery_matrix_requires_and_uses_one_explicit_api(self) -> None:
        with patch.dict(
            os.environ,
            {
                "WEBAI_LIVE_RECOVER": "1",
                "WEBAI_LIVE_PROVIDER": "gemini",
                "WEBAI_LIVE_ACCOUNT": "default-browser",
                "WEBAI_LIVE_API": "async",
            },
            clear=True,
        ):
            self.assertEqual(run_live_tests._matrix(), (("gemini", "async"),))

    def test_live_runner_stops_before_network_when_cleanup_is_pending(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cleanup_file = Path(directory) / "pending.json"
            cleanup_file.touch()
            with (
                patch.dict(os.environ, {}, clear=True),
                patch("run_live_tests.CLEANUP_FILE", cleanup_file),
                patch("run_live_tests.subprocess.run", return_value=CompletedProcess([], 0)) as run,
                patch("run_live_tests.time.sleep"),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(run_live_tests.main(), 2)
        self.assertEqual(run.call_count, 1)


if __name__ == "__main__":
    unittest.main()
