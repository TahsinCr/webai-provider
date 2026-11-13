"""Run offline tests first, then the opt-in live provider API matrix."""

from __future__ import annotations

import concurrent.futures
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

from tests.live.support.config import PROVIDERS
from tests.live.support.journal import CLEANUP_FILE, cleanup_file, pending_cleanup_files

_DEFAULT_CLEANUP_FILE = CLEANUP_FILE


def _pending_cleanup_files() -> list[Path]:
    if CLEANUP_FILE != _DEFAULT_CLEANUP_FILE:
        return [CLEANUP_FILE] if CLEANUP_FILE.exists() or CLEANUP_FILE.is_symlink() else []
    return pending_cleanup_files()


def _provider_cleanup_pending(provider: str) -> bool:
    path = CLEANUP_FILE if CLEANUP_FILE != _DEFAULT_CLEANUP_FILE else cleanup_file(provider)
    return path.exists() or path.is_symlink()


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
    apis = (api,) if api else ("async",)
    return tuple(
        (current_provider, current_api) for current_provider in providers for current_api in apis
    )


def _live_command(recovering: bool) -> list[str]:
    if recovering:
        return [sys.executable, "-m", "unittest", "tests.live.test_cleanup", "-v"]
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
    ]


def _run_provider(
    provider: str,
    apis: list[str],
    *,
    project_root: Path,
    recovering: bool,
    output_lock: threading.Lock,
) -> list[tuple[str, str, int]]:
    failures: list[tuple[str, str, int]] = []
    command = _live_command(recovering)
    for index, api in enumerate(apis):
        if index > 0:
            time.sleep(1)
        with output_lock:
            print(f"\n[{provider}] Live provider API: {provider} ({api})", flush=True)
        environment = os.environ.copy()
        environment["WEBAI_LIVE_TESTS"] = "1"
        environment["WEBAI_LIVE_PROVIDER"] = provider
        environment["WEBAI_LIVE_API"] = api
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
        if returncode:
            failures.append((provider, api, returncode))
        if _provider_cleanup_pending(provider):
            with output_lock:
                print(
                    f"[{provider}] A cleanup journal remains; stopping {provider} so its account can be recovered first.",
                    file=sys.stderr,
                    flush=True,
                )
            if not failures:
                failures.append((provider, api, returncode or 1))
            return failures
    return failures


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
    if _pending_cleanup_files() and not recovering:
        print(
            "A cleanup journal remains; run the selected cleanup recovery before live tests.",
            file=sys.stderr,
        )
        return 2

    by_provider: dict[str, list[str]] = {}
    for provider, api in matrix:
        by_provider.setdefault(provider, []).append(api)

    output_lock = threading.Lock()
    failures: list[tuple[str, str, int]] = []
    if len(by_provider) == 1:
        provider, apis = next(iter(by_provider.items()))
        failures.extend(
            _run_provider(
                provider,
                apis,
                project_root=project_root,
                recovering=recovering,
                output_lock=output_lock,
            )
        )
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(by_provider)) as pool:
            future_map = {
                pool.submit(
                    _run_provider,
                    provider,
                    apis,
                    project_root=project_root,
                    recovering=recovering,
                    output_lock=output_lock,
                ): provider
                for provider, apis in by_provider.items()
            }
            for future in concurrent.futures.as_completed(future_map):
                failures.extend(future.result())

    if failures:
        failures.sort(key=lambda item: (item[0], item[1]))
        failed = ", ".join(f"{provider}/{api}" for provider, api, _ in failures)
        print(f"\nLive API checks failed: {failed}", file=sys.stderr)
        return failures[0][2]
    print("\nOffline tests and the selected live API matrix passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
