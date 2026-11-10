"""Run offline tests first, then the opt-in live provider API matrix."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

from tests.live.support.config import PROVIDERS
from tests.live.support.journal import CLEANUP_FILE


def _matrix() -> tuple[tuple[str, str], ...] | None:
    provider = os.environ.get("WEBAI_LIVE_PROVIDER", "").casefold()
    api = os.environ.get("WEBAI_LIVE_API", "").casefold()
    recovering = os.environ.get("WEBAI_LIVE_RECOVER") == "1"

    if provider and provider not in PROVIDERS:
        print("WEBAI_LIVE_PROVIDER must be chatgpt, gemini, claude, or grok.", file=sys.stderr)
        return None
    if api and api not in {"sync", "async"}:
        print("WEBAI_LIVE_API must be sync or async.", file=sys.stderr)
        return None
    if recovering and (not provider or not api or not os.environ.get("WEBAI_LIVE_ACCOUNT")):
        print(
            "Recovery requires WEBAI_LIVE_PROVIDER, WEBAI_LIVE_ACCOUNT, and WEBAI_LIVE_API.",
            file=sys.stderr,
        )
        return None

    if recovering:
        return ((provider, api),)

    providers = (provider,) if provider else tuple(sorted(PROVIDERS))
    apis = (api,) if api else ("async", "sync")
    return tuple(
        (current_provider, current_api) for current_provider in providers for current_api in apis
    )


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
    if (CLEANUP_FILE.exists() or CLEANUP_FILE.is_symlink()) and not recovering:
        print(
            "A cleanup journal remains; run the selected cleanup recovery before live tests.",
            file=sys.stderr,
        )
        return 2

    failures: list[tuple[str, str, int]] = []
    failed_providers: set[str] = set()
    started = False
    for provider, api in matrix:
        if provider in failed_providers:
            continue
        if started:
            time.sleep(1)
        started = True
        print(f"\nLive provider API: {provider} ({api})", flush=True)
        environment = os.environ.copy()
        environment["WEBAI_LIVE_TESTS"] = "1"
        environment["WEBAI_LIVE_PROVIDER"] = provider
        environment["WEBAI_LIVE_API"] = api
        command = (
            [sys.executable, "-m", "unittest", "tests.live.test_cleanup", "-v"]
            if recovering
            else [
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
                "-f",
                "-v",
            ]
        )
        result = subprocess.run(
            command,
            cwd=project_root,
            env=environment,
            check=False,
        )
        if result.returncode:
            failures.append((provider, api, result.returncode))
            failed_providers.add(provider)
            if api == "async" and (provider, "sync") in matrix:
                print("Skipping this provider's sync run after its async run failed.")
        if CLEANUP_FILE.exists() or CLEANUP_FILE.is_symlink():
            print(
                "A cleanup journal remains; stopping the live matrix so its account can be recovered first.",
                file=sys.stderr,
            )
            return result.returncode or 1

    if failures:
        failed = ", ".join(f"{provider}/{api}" for provider, api, _ in failures)
        print(f"\nLive API checks failed: {failed}", file=sys.stderr)
        return failures[0][2]
    print("\nOffline tests and the selected live API matrix passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
