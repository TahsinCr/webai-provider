"""Run the offline unittest suite without collecting live provider tests."""

from __future__ import annotations

import unittest
from pathlib import Path


def _without_live_tests(suite: unittest.TestSuite) -> unittest.TestSuite:
    filtered = unittest.TestSuite()
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            nested = _without_live_tests(test)
            if nested.countTestCases():
                filtered.addTest(nested)
        elif not test.id().startswith("tests.live."):
            filtered.addTest(test)
    return filtered


def main() -> int:
    project_root = Path(__file__).resolve().parent
    suite = unittest.defaultTestLoader.discover(
        start_dir=str(project_root / "tests"),
        pattern="test_*.py",
        top_level_dir=str(project_root),
    )
    result = unittest.TextTestRunner(verbosity=2).run(_without_live_tests(suite))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
