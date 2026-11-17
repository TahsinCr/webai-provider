"""Run the offline WebAI Provider performance suite with pyperf."""

from __future__ import annotations

import importlib
import sys
from fnmatch import fnmatchcase
from pathlib import Path

from benchmarks._runner import benchmark_metadata

SUITES = ("history", "collections", "transport")


def _forward_selection(command, args) -> None:
    command.extend(("--suite", args.suite, "--match", args.match))


def main() -> int:
    try:
        import pyperf
    except ModuleNotFoundError as error:
        if error.name != "pyperf":
            raise
        print(
            'Install benchmark dependencies with: python -m pip install -e ".[benchmark]"',
            file=sys.stderr,
        )
        return 2

    runner = pyperf.Runner(
        program_args=(str(Path(__file__).resolve()),),
        add_cmdline_args=_forward_selection,
    )
    runner.argparser.description = __doc__
    runner.argparser.set_defaults(inherit_environ=("PYTHON_GIL",))
    runner.argparser.add_argument("--suite", choices=("all", *SUITES), default="all")
    runner.argparser.add_argument("--match", default="*", help="Glob matching benchmark names")
    runner.argparser.add_argument("--list-cases", action="store_true", help="Print matching names")
    args = runner.parse_args()
    suites = SUITES if args.suite == "all" else (args.suite,)
    cases = [
        case
        for suite in suites
        for case in importlib.import_module(f"benchmarks.benchmark_{suite}").cases()
        if fnmatchcase(case.name, args.match)
    ]
    if not cases:
        runner.argparser.error("No benchmark matches the selected suite and pattern")
    if args.list_cases:
        print("\n".join(case.name for case in cases))
        return 0
    runner.metadata.update(benchmark_metadata())
    for case in cases:
        runner.bench_time_func(case.name, case.measure, metadata=case.metadata)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
