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

    def test_default_live_matrix_covers_each_provider_with_async_api(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                run_live_tests._matrix(),
                tuple((provider, "async") for provider in sorted(PROVIDERS)),
            )

    def test_live_runner_runs_offline_suite_before_parallel_provider_matrix(self) -> None:
        result = CompletedProcess([], 0)
        popen_cls = _fake_popen_factory([0] * (len(PROVIDERS)))
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(os.environ, {}, clear=True),
            patch("tests.live.support.journal.CONFIG_DIR", Path(directory)),
            patch("run_live_tests.supports_process_credentials_lock", return_value=True),
            patch("run_live_tests.subprocess.run", return_value=result) as run,
            patch("run_live_tests.subprocess.Popen", side_effect=popen_cls),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(run_live_tests.main(), 0)

        self.assertEqual(run.call_count, 1)
        self.assertTrue(run.call_args.args[0][1].endswith("run_tests.py"))
        self.assertEqual(len(popen_cls.calls), len(PROVIDERS))
        first_call = popen_cls.calls[0]
        self.assertEqual(first_call["args"][0][2:6], ["unittest", "discover", "-s", "tests/live"])
        self.assertEqual(first_call["args"][0][-4:], ["-p", "test_*.py", "-v", "-c"])
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
            {(provider, "async") for provider in PROVIDERS},
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
            self.assertEqual(len(paths), 1, f"{p} must have one gate state")

    def test_interrupted_live_run_returns_130_without_a_success_summary(self) -> None:
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            patch("run_live_tests.subprocess.run", return_value=CompletedProcess([], 0)),
            patch("run_live_tests._matrix", return_value=(("grok", "async"),)),
            patch("run_live_tests._provider_cleanup_pending", return_value=False),
            patch("run_live_tests._run_provider", side_effect=KeyboardInterrupt),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            self.assertEqual(run_live_tests.main(), 130)
        self.assertIn("Waiting for active scenarios and cleanup", stderr.getvalue())
        self.assertNotIn("Live API scenario summary", stdout.getvalue())

    def test_live_runner_continues_api_matrix_after_failures(self) -> None:
        result = CompletedProcess([], 0)
        popen_cls = _fake_popen_factory([1] + [0] * (len(PROVIDERS) - 1))
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(os.environ, {}, clear=True),
            patch("tests.live.support.journal.CONFIG_DIR", Path(directory)),
            patch("run_live_tests.supports_process_credentials_lock", return_value=True),
            patch("run_live_tests.subprocess.run", return_value=result),
            patch("run_live_tests.subprocess.Popen", side_effect=popen_cls),
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
        self.assertEqual(
            seen_pairs,
            {(provider, "async") for provider in PROVIDERS},
        )

    def test_live_matrix_can_be_narrowed_to_one_provider(self) -> None:
        with patch.dict(
            os.environ,
            {"WEBAI_LIVE_PROVIDER": "GROK"},
            clear=True,
        ):
            self.assertEqual(run_live_tests._matrix(), (("grok", "async"),))

    def test_live_matrix_supports_sync_and_all_apis_with_async_default(self) -> None:
        with patch.dict(
            os.environ,
            {"WEBAI_LIVE_PROVIDER": "claude", "WEBAI_LIVE_API": "sync"},
            clear=True,
        ):
            self.assertEqual(run_live_tests._matrix(), (("claude", "sync"),))
        with patch.dict(
            os.environ,
            {"WEBAI_LIVE_PROVIDER": "claude", "WEBAI_LIVE_API": "all"},
            clear=True,
        ):
            self.assertEqual(
                run_live_tests._matrix(),
                (("claude", "async"), ("claude", "sync")),
            )

    def test_live_runner_uses_combined_gate_limit_for_multiple_apis_per_provider(self) -> None:
        from tests.live.support.actions import DEFAULT_COMBINED_RUN_LIMIT, DEFAULT_SINGLE_API_LIMIT

        for api_setting, expected_limit in (
            ("sync", str(DEFAULT_SINGLE_API_LIMIT)),
            ("all", str(DEFAULT_COMBINED_RUN_LIMIT)),
        ):
            with self.subTest(api_setting=api_setting):
                popen_cls = _fake_popen_factory([0, 0])
                with (
                    tempfile.TemporaryDirectory() as directory,
                    patch.dict(
                        os.environ,
                        {"WEBAI_LIVE_PROVIDER": "claude", "WEBAI_LIVE_API": api_setting},
                        clear=True,
                    ),
                    patch("tests.live.support.journal.CONFIG_DIR", Path(directory)),
                    patch("run_live_tests.supports_process_credentials_lock", return_value=True),
                    patch("run_live_tests.subprocess.run", return_value=CompletedProcess([], 0)),
                    patch("run_live_tests.subprocess.Popen", side_effect=popen_cls),
                    contextlib.redirect_stdout(io.StringIO()),
                ):
                    self.assertEqual(run_live_tests.main(), 0)
                limits = {
                    call["kwargs"]["env"]["WEBAI_LIVE_GATE_LIMIT"] for call in popen_cls.calls
                }
                self.assertEqual(limits, {expected_limit})

    def test_recovery_matrix_requires_an_explicit_account(self) -> None:
        with patch.dict(
            os.environ,
            {
                "WEBAI_LIVE_RECOVER": "1",
                "WEBAI_LIVE_PROVIDER": "gemini",
                "WEBAI_LIVE_ACCOUNT": "default-browser",
            },
            clear=True,
        ):
            self.assertEqual(run_live_tests._matrix(), (("gemini", "async"),))

    def test_live_runner_stops_before_network_when_cleanup_is_pending(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cleanup_file = Path(directory) / "live-cleanup.json"
            cleanup_file.touch()
            with (
                patch.dict(os.environ, {}, clear=True),
                patch("tests.live.support.journal.CONFIG_DIR", Path(directory)),
                patch("tests.live.support.config._require_ignored"),
                patch("run_live_tests.subprocess.run", return_value=CompletedProcess([], 0)) as run,
                contextlib.redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(run_live_tests.main(), 2)
        self.assertEqual(run.call_count, 1)

    def test_pending_cleanup_only_blocks_its_own_provider(self) -> None:
        for selection, expected_code in (("", 2), ("gemini", 0)):
            with self.subTest(selection=selection):
                popen_cls = _fake_popen_factory([0, 0])
                stderr = io.StringIO()
                with (
                    patch.dict(os.environ, {"WEBAI_LIVE_PROVIDER": selection}, clear=True),
                    patch("run_live_tests.subprocess.run", return_value=CompletedProcess([], 0)),
                    patch(
                        "run_live_tests._provider_cleanup_pending",
                        side_effect=lambda p: p in {"chatgpt", "grok"},
                    ),
                    patch("run_live_tests.subprocess.Popen", side_effect=popen_cls),
                    contextlib.redirect_stdout(io.StringIO()),
                    contextlib.redirect_stderr(stderr),
                ):
                    self.assertEqual(run_live_tests.main(), expected_code)
                launched = {
                    call["kwargs"]["env"]["WEBAI_LIVE_PROVIDER"] for call in popen_cls.calls
                }
                self.assertEqual(launched, {"gemini"} if selection else {"claude", "gemini"})
                self.assertEqual("chatgpt, grok" in stderr.getvalue(), not selection)

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
            patch("tests.live.support.journal.CONFIG_DIR", Path(directory)),
            patch("run_live_tests.supports_process_credentials_lock", return_value=True),
            patch("run_live_tests.subprocess.run", return_value=result),
            patch("run_live_tests.subprocess.Popen", side_effect=popen_cls),
            contextlib.redirect_stdout(stdout),
        ):
            code = run_live_tests.main()

        output = stdout.getvalue()
        self.assertEqual(code, 0)
        self.assertIn("rate_limited=1", output)
        self.assertIn("unsupported=1", output)
        self.assertIn("passed=0 partial=1", output)
        self.assertNotIn("all live scenarios passed without skips", output.lower())
        self.assertIn("partial verification only", output.lower())

    def test_live_runner_fails_without_a_verified_scenario(self) -> None:
        result = CompletedProcess([], 0)
        for scenarios in (None, [], [{"id": "t1", "status": "skipped_rate_limited"}]):
            write_report = scenarios is not None
            scenario_reports = (
                {("grok", "async"): {"provider": "grok", "api": "async", "tests": scenarios}}
                if write_report
                else None
            )
            with self.subTest(scenarios=scenarios):
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
                    patch("tests.live.support.journal.CONFIG_DIR", Path(directory)),
                    patch("run_live_tests.supports_process_credentials_lock", return_value=True),
                    patch("run_live_tests.subprocess.run", return_value=result),
                    patch("run_live_tests.subprocess.Popen", side_effect=popen_cls),
                    contextlib.redirect_stdout(stdout),
                    contextlib.redirect_stderr(io.StringIO()),
                ):
                    code = run_live_tests.main()
                self.assertEqual(code, 1)

    def test_live_runner_serializes_when_process_credentials_lock_is_unsupported(self) -> None:
        from concurrent.futures import ThreadPoolExecutor as RealExecutor

        result = CompletedProcess([], 0)
        popen_cls = _fake_popen_factory([0] * (len(PROVIDERS)))
        recorded_workers: list[int] = []

        def spy_executor(*, max_workers: int):
            recorded_workers.append(max_workers)
            return RealExecutor(max_workers=max_workers)

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(os.environ, {}, clear=True),
            patch("tests.live.support.journal.CONFIG_DIR", Path(directory)),
            patch("run_live_tests.supports_process_credentials_lock", return_value=False),
            patch("run_live_tests.ThreadPoolExecutor", side_effect=spy_executor),
            patch("run_live_tests.subprocess.run", return_value=result),
            patch("run_live_tests.subprocess.Popen", side_effect=popen_cls),
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
                patch("tests.live.support.config._require_ignored"),
            ):
                self.assertTrue(run_live_tests._provider_cleanup_pending("gemini"))
                self.assertFalse(run_live_tests._provider_cleanup_pending("chatgpt"))


if __name__ == "__main__":
    unittest.main()
