"""Common pyperf timing and result provenance."""

from __future__ import annotations

import asyncio
import hashlib
import subprocess
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from importlib.metadata import version
from pathlib import Path
from time import perf_counter

from webaiprovider import __version__

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Case:
    name: str
    measure: Callable[[int], float]
    metadata: dict[str, int | str] = field(default_factory=dict)


def time_sync(loops: int, operation: Callable[[], object]) -> float:
    started = perf_counter()
    for _ in range(loops):
        operation()
    return perf_counter() - started


async def time_async(loops: int, operation: Callable[[], Awaitable[object]]) -> float:
    started = perf_counter()
    for _ in range(loops):
        await operation()
    return perf_counter() - started


def async_case(name: str, operation: Callable[[], Awaitable[object]], **metadata) -> Case:
    def measure(loops: int) -> float:
        return asyncio.run(time_async(loops, operation))

    return Case(name, measure, metadata)


def benchmark_metadata() -> dict[str, str]:
    revision = dirty = "unknown"
    try:
        revision = subprocess.check_output(
            ["git", "rev-parse", "--verify", "HEAD"], cwd=ROOT, stderr=subprocess.DEVNULL, text=True
        ).strip()
        changes = subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=ROOT,
            stderr=subprocess.DEVNULL,
        )
        dirty = str(bool(changes)).lower()
    except (OSError, subprocess.CalledProcessError):
        pass

    paths = [ROOT / name for name in ("pyproject.toml", "setup.py", "run_benchmarks.py")]
    paths.extend((ROOT / "benchmarks").glob("*.py"))
    paths.extend(
        path
        for path in (ROOT / "webaiprovider").rglob("*")
        if path.is_file() and path.suffix in {".py", ".c", ".h", ".so", ".pyd"}
    )
    digest = hashlib.sha256()
    for path in sorted(paths):
        relative = path.relative_to(ROOT).as_posix().encode()
        digest.update(len(relative).to_bytes(4, "big") + relative)
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    gil = getattr(sys, "_is_gil_enabled", None)
    return {
        "webai_revision": revision,
        "webai_worktree_dirty": dirty,
        "webai_checkout_sha256": digest.hexdigest(),
        "webai_provider_version": __version__,
        "webai_flowevent_version": version("flowevent"),
        "webai_curl_cffi_version": version("curl-cffi"),
        "webai_pyperf_version": version("pyperf"),
        "webai_network": "offline fixtures",
        "webai_python_gil": "unknown" if gil is None else str(gil()).lower(),
    }
