from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

import run_live_tests
import run_tests
from tests.live.support.config import PROVIDERS


def _write_default_report(env: dict[str, str], payload: dict[str, object] | None = None) -> None:
    report_path = env.get("WEBAI_LIVE_REPORT_FILE")
    if not report_path:
        return
    data = payload or {
        "provider": env.get("WEBAI_LIVE_PROVIDER", ""),
        "api": env.get("WEBAI_LIVE_API", ""),
        "tests": [
            {
                "id": "tests.live.test_dummy.DummyTest.test_ok",
                "status": "passed",
                "feature_skips": [],
            }
        ],
    }
    Path(report_path).write_text(json.dumps(data), encoding="utf-8")


def _fake_popen_factory(
    returncodes: list[int] | None = None,
    reports: dict[tuple[str, str], dict[str, object] | None] | None = None,
    *,
    write_report: bool = True,
):
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
            env = kwargs.get("env") or {}
            key = (str(env.get("WEBAI_LIVE_PROVIDER", "")), str(env.get("WEBAI_LIVE_API", "")))
            if reports is not None and key in reports:
                custom = reports[key]
                if custom is not None:
                    _write_default_report(env, custom)
            elif write_report:
                _write_default_report(env)

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

    def test_default_live_matrix_covers_each_provider_with_sync_and_async_apis(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                run_live_tests._matrix(),
                tuple(
                    (provider, api) for provider in sorted(PROVIDERS) for api in ("sync", "async")
                ),
            )

    def test_live_runner_runs_offline_suite_before_parallel_provider_matrix(self) -> None:
        result = CompletedProcess([], 0)
        popen_cls = _fake_popen_factory([0] * (len(PROVIDERS) * 2))
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(os.environ, {}, clear=True),
            patch("run_live_tests.CLEANUP_FILE", Path(directory) / "absent.json"),
            patch("run_live_tests.supports_process_credentials_lock", return_value=True),
            patch("run_live_tests.subprocess.run", return_value=result) as run,
            patch("run_live_tests.subprocess.Popen", side_effect=popen_cls),
            patch("run_live_tests.time.sleep"),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(run_live_tests.main(), 0)

        self.assertEqual(run.call_count, 1)
        self.assertTrue(run.call_args.args[0][1].endswith("run_tests.py"))
        self.assertEqual(len(popen_cls.calls), len(PROVIDERS) * 2)
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
        self.assertEqual(
            seen_pairs,
            {(provider, api) for provider in PROVIDERS for api in ("sync", "async")},
        )
        gate_files_by_provider = {
            p: {
                call["kwargs"]["env"]["WEBAI_LIVE_GATE_FILE"]
                for call in popen_cls.calls
                if call["kwargs"]["env"]["WEBAI_LIVE_PROVIDER"] == p
            }
            for p in PROVIDERS
        }
        for p, paths in gate_files_by_provider.items():
            self.assertEqual(len(paths), 1, f"sync and async for {p} must share one gate state")

    def test_live_runner_continues_api_matrix_after_failures(self) -> None:
        result = CompletedProcess([], 0)
        popen_cls = _fake_popen_factory([1] + [0] * (len(PROVIDERS) * 2 - 1))
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(os.environ, {}, clear=True),
            patch("run_live_tests.CLEANUP_FILE", Path(directory) / "absent.json"),
            patch("run_live_tests.supports_process_credentials_lock", return_value=True),
            patch("run_live_tests.subprocess.run", return_value=result),
            patch("run_live_tests.subprocess.Popen", side_effect=popen_cls),
            patch("run_live_tests.time.sleep"),
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(run_live_tests.main(), 1)

        self.assertEqual(len(popen_cls.calls), len(PROVIDERS) * 2)
        self.assertNotIn("-f", popen_cls.calls[0]["args"][0])
        seen_pairs = {
            (
                call["kwargs"]["env"]["WEBAI_LIVE_PROVIDER"],
                call["kwargs"]["env"]["WEBAI_LIVE_API"],
            )
            for call in popen_cls.calls
        }
        self.assertEqual(
            seen_pairs,
            {(provider, api) for provider in PROVIDERS for api in ("sync", "async")},
        )

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

    def test_live_runner_reports_skips_rate_limits_and_refuses_false_full_pass(self) -> None:
        result = CompletedProcess([], 0)
        reports = {
            ("grok", "async"): {
                "provider": "grok",
                "api": "async",
                "tests": [
                    {
                        "id": "t1",
                        "status": "passed",
                        "feature_skips": ["chats.branch: unsupported"],
                    },
                    {"id": "t2", "status": "skipped_rate_limited", "feature_skips": []},
                    {"id": "t3", "status": "skipped_unsupported", "feature_skips": []},
                ],
            }
        }
        popen_cls = _fake_popen_factory([0, 0], reports=reports)
        stdout = io.StringIO()
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(os.environ, {"WEBAI_LIVE_PROVIDER": "grok"}, clear=True),
            patch("run_live_tests.CLEANUP_FILE", Path(directory) / "absent.json"),
            patch("run_live_tests.supports_process_credentials_lock", return_value=True),
            patch("run_live_tests.subprocess.run", return_value=result),
            patch("run_live_tests.subprocess.Popen", side_effect=popen_cls),
            patch("run_live_tests.time.sleep"),
            contextlib.redirect_stdout(stdout),
        ):
            code = run_live_tests.main()

        output = stdout.getvalue()
        self.assertEqual(code, 0)
        self.assertIn("rate_limited=1", output)
        self.assertIn("unsupported=1", output)
        self.assertIn("passed=1", output)
        self.assertNotIn("all live scenarios passed without skips", output.lower())
        self.assertIn("partial verification only", output.lower())

    def test_live_runner_fails_when_report_is_missing_or_zero_scenarios_ran(self) -> None:
        result = CompletedProcess([], 0)
        for scenario_reports, write_report in (
            (None, False),
            ({("grok", "async"): {"provider": "grok", "api": "async", "tests": []}}, True),
        ):
            with self.subTest(write_report=write_report):
                popen_cls = _fake_popen_factory(
                    [0], reports=scenario_reports, write_report=write_report
                )
                stdout = io.StringIO()
                with (
                    tempfile.TemporaryDirectory() as directory,
                    patch.dict(
                        os.environ,
                        {"WEBAI_LIVE_PROVIDER": "grok", "WEBAI_LIVE_API": "async"},
                        clear=True,
                    ),
                    patch("run_live_tests.CLEANUP_FILE", Path(directory) / "absent.json"),
                    patch("run_live_tests.supports_process_credentials_lock", return_value=True),
                    patch("run_live_tests.subprocess.run", return_value=result),
                    patch("run_live_tests.subprocess.Popen", side_effect=popen_cls),
                    patch("run_live_tests.time.sleep"),
                    contextlib.redirect_stdout(stdout),
                    contextlib.redirect_stderr(io.StringIO()),
                ):
                    code = run_live_tests.main()
                self.assertEqual(code, 1)

    def test_live_runner_serializes_when_process_credentials_lock_is_unsupported(self) -> None:
        from concurrent.futures import ThreadPoolExecutor as RealExecutor

        result = CompletedProcess([], 0)
        popen_cls = _fake_popen_factory([0] * (len(PROVIDERS) * 2))
        recorded_workers: list[int] = []

        def spy_executor(*, max_workers: int):
            recorded_workers.append(max_workers)
            return RealExecutor(max_workers=max_workers)

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(os.environ, {}, clear=True),
            patch("run_live_tests.CLEANUP_FILE", Path(directory) / "absent.json"),
            patch("run_live_tests.supports_process_credentials_lock", return_value=False),
            patch("run_live_tests.ThreadPoolExecutor", side_effect=spy_executor),
            patch("run_live_tests.subprocess.run", return_value=result),
            patch("run_live_tests.subprocess.Popen", side_effect=popen_cls),
            patch("run_live_tests.time.sleep"),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(run_live_tests.main(), 0)
        self.assertEqual(recorded_workers, [1])

    def test_live_runner_detects_legacy_cleanup_journal_for_owning_provider(self) -> None:
        from tests.live.support.journal import write_private_json

        with tempfile.TemporaryDirectory() as directory:
            cfg = Path(directory) / ".config"
            cfg.mkdir(mode=0o700)
            legacy = cfg / "live-cleanup.json"
            run_id = "webai-live-" + "a" * 32
            write_private_json(
                legacy,
                {
                    "version": 1,
                    "provider": "gemini",
                    "account": "personal",
                    "run_id": run_id,
                    "resources": [
                        {
                            "kind": "chat",
                            "markers": [f"{run_id}-chat"],
                            "remote_id": "c_1",
                            "attempted": True,
                        }
                    ],
                },
            )
            with (
                patch("tests.live.support.journal.CONFIG_DIR", cfg),
                patch("tests.live.support.journal.CLEANUP_FILE", legacy),
                patch("tests.live.support.journal._DEFAULT_CLEANUP_FILE", legacy),
                patch("tests.live.support.config._require_ignored"),
                patch("run_live_tests.CLEANUP_FILE", legacy),
                patch("run_live_tests._DEFAULT_CLEANUP_FILE", legacy),
            ):
                self.assertEqual(run_live_tests._pending_cleanup_files(), [legacy])
                self.assertTrue(run_live_tests._provider_cleanup_pending("gemini"))
                self.assertFalse(run_live_tests._provider_cleanup_pending("chatgpt"))


if __name__ == "__main__":
    unittest.main()
