"""Fail-before regression tests: invalid --day-start/--day-end values must be
rejected with a clean CLI error (exit 1, 'Error:' message), not a traceback.

Run: python -m unittest discover -s tests -v   (from the repo root)
"""
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MAIN_PY = REPO_ROOT / "main.py"


def run_scan(tmp: Path, *extra: str) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("FS_HUNTER_")}
    return subprocess.run(
        [sys.executable, str(MAIN_PY), "scan", "-d", str(tmp),
         "-o", str(tmp / "out"), "--off-hash", *extra],
        capture_output=True, text=True, cwd=REPO_ROOT, env=env,
        timeout=180,
    )


class TestDayFilterValidation(unittest.TestCase):
    def test_invalid_day_start_is_clean_error(self):
        with tempfile.TemporaryDirectory() as td:
            proc = run_scan(Path(td), "--day-start", "notatime")
        combined = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 1)
        self.assertIn("Error:", combined)
        self.assertNotIn("Traceback", combined)

    def test_invalid_day_end_is_clean_error(self):
        with tempfile.TemporaryDirectory() as td:
            proc = run_scan(Path(td), "--day-end", "25:99")
        combined = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 1)
        self.assertIn("Error:", combined)
        self.assertNotIn("Traceback", combined)


if __name__ == "__main__":
    unittest.main()
