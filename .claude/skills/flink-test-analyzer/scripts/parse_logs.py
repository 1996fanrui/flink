"""Parse Flink test log files and extract structured test results.

Usage:
    python parse_logs.py file1.log file2.log ...

Reads log files produced by Flink's TestLogger, identifies individual test
cases by their start/end markers, and outputs a JSON summary to stdout.

Each test case entry includes: test_class, full_class_name, method,
parameters (if parameterized), status (success/failed), and error_message.
"""

import argparse
import json
import re
import sys
from pathlib import Path

# Regex for the "is running" marker.
# Captures: method, optional [params], full.class.Name
_RUNNING_RE = re.compile(
    r"^Test\s+(\w+)"           # method name
    r"(?:\[([^\]]*)\])?"       # optional [parameters]
    r"\(([^)]+)\)"             # (full.class.Name)
    r"\s+is running\.\s*$"
)

# Regex for the "successfully run" marker.
_SUCCESS_RE = re.compile(
    r"^Test\s+\w+"
    r"(?:\[[^\]]*\])?"
    r"\([^)]+\)"
    r"\s+successfully run\.\s*$"
)

# Regex for the "failed with:" marker.
_FAILED_RE = re.compile(
    r"^Test\s+\w+"
    r"(?:\[[^\]]*\])?"
    r"\([^)]+\)"
    r"\s+failed with:\s*$"
)

_SEPARATOR_RE = re.compile(r"^={60,}\s*$")


def _short_class_name(full_class_name: str) -> str:
    """Extract the simple class name from a fully qualified name."""
    return full_class_name.rsplit(".", 1)[-1]


def parse_log_content(content: str) -> list[dict]:
    """Parse raw log text and return a list of test result dicts.

    Each dict contains:
        test_class, full_class_name, method, parameters, status, error_message
    """
    results: list[dict] = []
    lines = content.splitlines()
    i = 0

    while i < len(lines):
        line = lines[i].strip()
        m = _RUNNING_RE.match(line)
        if not m:
            i += 1
            continue

        method = m.group(1)
        parameters = m.group(2)  # None when not parameterized
        full_class_name = m.group(3)
        test_class = _short_class_name(full_class_name)

        # Scan forward to find the outcome marker
        status = "failed"
        error_message = None
        i += 1

        while i < len(lines):
            cur = lines[i].strip()

            if _SUCCESS_RE.match(cur):
                status = "success"
                i += 1
                break

            if _FAILED_RE.match(cur):
                status = "failed"
                # Collect error lines until the next separator
                error_lines: list[str] = []
                i += 1
                while i < len(lines):
                    err_line = lines[i]
                    if _SEPARATOR_RE.match(err_line.strip()):
                        break
                    error_lines.append(err_line)
                    i += 1
                error_message = "\n".join(error_lines) if error_lines else None
                break

            i += 1

        results.append({
            "test_class": test_class,
            "full_class_name": full_class_name,
            "method": method,
            "parameters": parameters,
            "status": status,
            "error_message": error_message,
        })

    return results


def parse_log_files(file_paths: list[str]) -> dict:
    """Parse multiple log files and return grouped results.

    Returns a dict with structure:
        { "files": { "<filename>": { "tests": [...], "summary": {...} } } }
    """
    result: dict = {"files": {}}

    for path_str in file_paths:
        path = Path(path_str)
        content = path.read_text(errors="replace")
        tests = parse_log_content(content)

        passed = sum(1 for t in tests if t["status"] == "success")
        failed = sum(1 for t in tests if t["status"] == "failed")

        result["files"][path.name] = {
            "tests": tests,
            "summary": {
                "total": len(tests),
                "passed": passed,
                "failed": failed,
            },
        }

    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse Flink test log files and output JSON results."
    )
    parser.add_argument(
        "log_files",
        nargs="+",
        help="One or more log file paths to parse.",
    )
    args = parser.parse_args()
    result = parse_log_files(args.log_files)
    json.dump(result, sys.stdout, indent=2)
    print()  # trailing newline


if __name__ == "__main__":
    main()
