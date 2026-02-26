"""Split failed test case logs from Flink log files into individual files.

Usage:
    python split_failure_logs.py [--output-dir DIR] file1.log file2.log ...

For each failed test case found in the input logs, this script extracts the
complete log block (from "is running" through "failed with:" and its stack
trace) and writes it to a separate file under the output directory.

Filename format:
    {TestClass}_{method}[{params}]_from_{original_log_name}.log

If no --output-dir is given, files are written to ./split_failures/.
"""

import argparse
import re
import sys
from pathlib import Path

_RUNNING_RE = re.compile(
    r"^Test\s+(\w+)"           # method
    r"(?:\[([^\]]*)\])?"       # optional [params]
    r"\(([^)]+)\)"             # (full.class.Name)
    r"\s+is running\.\s*$"
)

_SUCCESS_RE = re.compile(
    r"^Test\s+\w+(?:\[[^\]]*\])?\([^)]+\)\s+successfully run\.\s*$"
)

_FAILED_RE = re.compile(
    r"^Test\s+\w+(?:\[[^\]]*\])?\([^)]+\)\s+failed with:\s*$"
)

_SEPARATOR_RE = re.compile(r"^={60,}\s*$")


def sanitize_filename(name: str) -> str:
    """Replace filesystem-unsafe characters with underscores.

    Preserves brackets, equals, dots, hyphens, and underscores which are
    common in Flink test names.
    """
    return re.sub(r'[/<>:"|?*\\]', "_", name)


def _short_class_name(full_class_name: str) -> str:
    return full_class_name.rsplit(".", 1)[-1]


def split_failure_logs(log_file_path: str, output_dir: str) -> list[str]:
    """Extract failed test blocks from a log file into individual files.

    Returns a list of paths to the created split files.
    """
    log_path = Path(log_file_path)
    content = log_path.read_text(errors="replace")
    lines = content.splitlines(keepends=True)
    source_name = log_path.stem  # filename without extension

    out_dir = Path(output_dir)
    created_files: list[str] = []

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        m = _RUNNING_RE.match(line)
        if not m:
            i += 1
            continue

        method = m.group(1)
        parameters = m.group(2)
        full_class_name = m.group(3)
        test_class = _short_class_name(full_class_name)

        # Record the start of this test block (include separator before if present)
        block_start = i
        if i > 0 and _SEPARATOR_RE.match(lines[i - 1].strip()):
            block_start = i - 1

        # Scan for outcome
        i += 1
        is_failure = False
        block_end = i

        while i < len(lines):
            cur = lines[i].strip()

            if _SUCCESS_RE.match(cur):
                i += 1
                break

            if _FAILED_RE.match(cur):
                is_failure = True
                # Continue past error lines until separator
                i += 1
                while i < len(lines):
                    if _SEPARATOR_RE.match(lines[i].strip()):
                        block_end = i + 1  # include the closing separator
                        i += 1
                        break
                    i += 1
                break

            i += 1

        if not is_failure:
            continue

        # Build the block content
        block_content = "".join(lines[block_start:block_end])

        # Build filename
        if parameters is not None:
            base_name = f"{test_class}_{method}[{parameters}]_from_{source_name}.log"
        else:
            base_name = f"{test_class}_{method}_from_{source_name}.log"
        safe_name = sanitize_filename(base_name)

        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / safe_name
        out_path.write_text(block_content)
        created_files.append(str(out_path))

    return created_files


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Split failed test case logs into individual files."
    )
    parser.add_argument(
        "--output-dir",
        default="./split_failures",
        help="Directory to write split log files (default: ./split_failures).",
    )
    parser.add_argument(
        "log_files",
        nargs="+",
        help="One or more log file paths to process.",
    )
    args = parser.parse_args()

    total_created: list[str] = []
    for log_file in args.log_files:
        created = split_failure_logs(log_file, args.output_dir)
        total_created.extend(created)

    print(f"Extracted {len(total_created)} failure log(s):")
    for f in total_created:
        print(f"  {f}")


if __name__ == "__main__":
    main()
