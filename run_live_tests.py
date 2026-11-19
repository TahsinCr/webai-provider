"""Run offline tests first, then the opt-in live provider API matrix."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path

from tests.live.support.actions import DEFAULT_SINGLE_API_LIMIT
from tests.live.support.config import PROVIDERS, supports_process_credentials_lock
from tests.live.support.errors import SafetyError
from tests.live.support.journal import (
    CLEANUP_FILE,
    cleanup_file,
    cleanup_journal_provider,
)

_DEFAULT_CLEANUP_FILE = CLEANUP_FILE


@dataclass(frozen=True)
class _PhaseOutcome:
    provider: str
    api: str
    returncode: int
    total: int = 0
    passed: int = 0
    partial: int = 0
    skipped_unsupported: int = 0
    skipped_rate_limited: int = 0
    skipped_setup_failed: int = 0
    failed: int = 0
    feature_skips: int = 0
    report_missing: bool = False
    cleanup_pending: bool = False

    @property
    def is_failure(self) -> bool:
        return (
            self.returncode != 0
            or self.failed > 0
            or self.skipped_setup_failed > 0
            or self.report_missing
            or self.cleanup_pending
            or self.passed + self.partial == 0
        )

    @property
    def has_skips(self) -> bool:
        return (
            self.partial > 0
            or self.skipped_unsupported > 0
            or self.skipped_rate_limited > 0
            or self.skipped_setup_failed > 0
            or self.feature_skips > 0
        )


def _provider_cleanup_pending(provider: str) -> bool:
    if CLEANUP_FILE != _DEFAULT_CLEANUP_FILE:
        if not (CLEANUP_FILE.exists() or CLEANUP_FILE.is_symlink()):
            return False
        try:
            owner = cleanup_journal_provider(CLEANUP_FILE)
        except SafetyError:
            return True
        return owner is None or owner == provider

    provider_path = cleanup_file(provider)
    if provider_path.exists() or provider_path.is_symlink():
        return True
    if CLEANUP_FILE.exists() or CLEANUP_FILE.is_symlink():
        try:
            owner = cleanup_journal_provider(CLEANUP_FILE)
        except SafetyError:
            return True
        return owner == provider
    return False


def _matrix() -> tuple[tuple[str, str], ...] | None:
    provider = os.environ.get("WEBAI_LIVE_PROVIDER", "").casefold()
    recovering = os.environ.get("WEBAI_LIVE_RECOVER") == "1"

    if provider and provider not in PROVIDERS:
        print("WEBAI_LIVE_PROVIDER must be chatgpt, gemini, claude, or grok.", file=sys.stderr)
        return None
    if recovering and (not provider or not os.environ.get("WEBAI_LIVE_ACCOUNT")):
        print(
            "Recovery requires WEBAI_LIVE_PROVIDER and WEBAI_LIVE_ACCOUNT.",
            file=sys.stderr,
        )
        return None

    providers = (provider,) if provider else tuple(sorted(PROVIDERS))
    return tuple((current_provider, "async") for current_provider in providers)


def _live_command(recovering: bool) -> list[str]:
    if recovering:
        return [sys.executable, "-m", "unittest", "tests.live.test_cleanup", "-v", "-c"]
    return [
        sys.executable,
        "-m",
        "unittest",
        "discover",
        "-s",
        "tests/live",
        "-t",
        ".",
        "-p",
        "test_*.py",
        "-v",
        "-c",
    ]


def _read_phase_report(
    provider: str, api: str, report_path: Path, returncode: int
) -> _PhaseOutcome:
    if not report_path.exists() or report_path.is_symlink():
        return _PhaseOutcome(provider, api, returncode, report_missing=True)
    try:
        data = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return _PhaseOutcome(provider, api, returncode, report_missing=True)
    tests = data.get("tests") if isinstance(data, dict) else None
    if not isinstance(tests, list):
        return _PhaseOutcome(provider, api, returncode, report_missing=True)

    counts = {
        "passed": 0,
        "partial": 0,
        "skipped_unsupported": 0,
        "skipped_rate_limited": 0,
        "skipped_setup_failed": 0,
        "failed": 0,
    }
    feature_skips = 0
    for item in tests:
        if not isinstance(item, dict):
            return _PhaseOutcome(provider, api, returncode, report_missing=True)
        status = item.get("status")
        if not isinstance(status, str) or status not in counts:
            return _PhaseOutcome(provider, api, returncode, report_missing=True)
        skips = item.get("feature_skips")
        if status == "passed" and skips:
            status = "partial"
        counts[status] += 1
        if isinstance(skips, list):
            feature_skips += len(skips)
    return _PhaseOutcome(
        provider=provider,
        api=api,
        returncode=returncode,
        total=len(tests),
        passed=counts["passed"],
        partial=counts["partial"],
        skipped_unsupported=counts["skipped_unsupported"],
        skipped_rate_limited=counts["skipped_rate_limited"],
        skipped_setup_failed=counts["skipped_setup_failed"],
        failed=counts["failed"],
        feature_skips=feature_skips,
        report_missing=False,
    )


def _run_provider(
    provider: str,
    *,
    project_root: Path,
    runtime_dir: Path,
    recovering: bool,
    output_lock: threading.Lock,
) -> _PhaseOutcome:
    api = "async"
    command = _live_command(recovering)
    gate_file = runtime_dir / f"gate-{provider}.json"
    with output_lock:
        print(f"\n[{provider}] Live provider API: {provider} ({api})", flush=True)
    report_file = runtime_dir / f"report-{provider}-{api}.json"
    environment = os.environ.copy()
    environment["WEBAI_LIVE_TESTS"] = "1"
    environment["WEBAI_LIVE_PROVIDER"] = provider
    environment["WEBAI_LIVE_API"] = api
    environment["WEBAI_LIVE_GATE_FILE"] = str(gate_file)
    environment["WEBAI_LIVE_GATE_LIMIT"] = str(DEFAULT_SINGLE_API_LIMIT)
    environment["WEBAI_LIVE_REPORT_FILE"] = str(report_file)
    environment["PYTHONUNBUFFERED"] = "1"
    process = subprocess.Popen(
        command,
        cwd=project_root,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None
    try:
        for line in process.stdout:
            with output_lock:
                sys.stdout.write(f"[{provider}] {line}")
                sys.stdout.flush()
    finally:
        process.stdout.close()
    returncode = process.wait()
    cleanup_pending = _provider_cleanup_pending(provider)
    outcome = _read_phase_report(provider, api, report_file, returncode)
    if cleanup_pending:
        outcome = replace(outcome, cleanup_pending=True)
    if cleanup_pending:
        with output_lock:
            print(
                f"[{provider}] A cleanup journal remains; recover this account before another run.",
                file=sys.stderr,
                flush=True,
            )
    return outcome


def main() -> int:
    project_root = Path(__file__).resolve().parent
    normal = subprocess.run(
        [sys.executable, str(project_root / "run_tests.py")],
        cwd=project_root,
        check=False,
    )
    if normal.returncode:
        return normal.returncode

    matrix = _matrix()
    if matrix is None:
        return 2

    recovering = os.environ.get("WEBAI_LIVE_RECOVER") == "1"
    providers = [provider for provider, _ in matrix]
    blocked = (
        []
        if recovering
        else [provider for provider in providers if _provider_cleanup_pending(provider)]
    )
    if blocked:
        print(
            "Live checks blocked by pending cleanup for: "
            + ", ".join(blocked)
            + ". Run cleanup recovery for these providers before testing them.",
            file=sys.stderr,
            flush=True,
        )
        providers = [provider for provider in providers if provider not in blocked]
        if not providers:
            return 2

    output_lock = threading.Lock()
    with tempfile.TemporaryDirectory() as runtime_raw:
        runtime_dir = Path(runtime_raw)
        options = {
            "project_root": project_root,
            "runtime_dir": runtime_dir,
            "recovering": recovering,
            "output_lock": output_lock,
        }
        workers = (
            len(providers) if len(providers) > 1 and supports_process_credentials_lock() else 1
        )
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_run_provider, provider, **options) for provider in providers]
            try:
                outcomes = [future.result() for future in futures]
            except KeyboardInterrupt:
                for future in futures:
                    future.cancel()
                print(
                    "\nLive run interrupted. Waiting for active scenarios and cleanup.",
                    file=sys.stderr,
                    flush=True,
                )
                return 130

    outcomes.sort(key=lambda item: (item.provider, item.api))
    print("\nLive API scenario summary:", flush=True)
    for item in outcomes:
        if item.report_missing:
            print(
                f"  {item.provider}/{item.api}: unverified (missing scenario report; rc={item.returncode})",
                flush=True,
            )
            continue
        extra = f" feature_skips={item.feature_skips}" if item.feature_skips else ""
        print(
            f"  {item.provider}/{item.api}: scenarios={item.total} passed={item.passed} partial={item.partial} "
            f"unsupported={item.skipped_unsupported} rate_limited={item.skipped_rate_limited} "
            f"setup_skipped={item.skipped_setup_failed} failed={item.failed}{extra}",
            flush=True,
        )

    failures = [item for item in outcomes if item.is_failure]
    if failures:
        failed = ", ".join(f"{item.provider}/{item.api}" for item in failures)
        print(f"\nLive API checks failed or were unverified: {failed}", file=sys.stderr)
        return failures[0].returncode or 1
    if blocked:
        print("\nLive API checks incomplete because cleanup is pending.", file=sys.stderr)
        return 2
    if any(item.has_skips for item in outcomes):
        print(
            "\nOffline tests passed; live API checks completed with skipped scenarios (partial verification only)."
        )
        return 0
    print("\nOffline tests passed; all live scenarios passed without skips.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130) from None
