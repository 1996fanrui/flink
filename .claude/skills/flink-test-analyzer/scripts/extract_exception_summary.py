"""Extract a by-exception summary from per-test split failure logs.

For each split failure log produced by ``split_failure_logs.py``, this script
identifies:
  - ``top_exception``: the first fully-qualified exception class that appears
    after the ``failed with:`` marker (i.e. what JUnit ultimately reported).
  - ``root_cause``: the FQCN found on the deepest ``Caused by:`` line. Falls
    back to ``top_exception`` when no ``Caused by:`` chain is present.

Outputs in ``--output-dir``:
  - ``exception_summary.json``: machine-readable per-failure rows plus counts
    grouped both by top-level exception and by root cause.
  - ``exception_summary.md``: human-readable Markdown summary, sorted by
    descending count.

This is intended to run AFTER ``split_failure_logs.py`` and BEFORE any
root-cause analysis. Even when there are zero failures, the script writes
the output files so downstream phases have a stable contract.

Usage:
    python extract_exception_summary.py \\
        --split-dir <dir of per-test failure logs> \\
        --output-dir <dir to write summary files> \\
        [--source-log <path to original full log, for provenance>]
"""

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

_FAILED_RE = re.compile(r"^Test\s+.+\s+failed with:\s*$")
_FQCN_EXC_RE = re.compile(
    r"([a-zA-Z_][\w.$]*\.[A-Z][\w$]*(?:Exception|Error|AssertionFailedError|Throwable))"
)
_CAUSED_BY_RE = re.compile(r"^\s*Caused by:\s*(.*)$")


def _find_exception_in_line(text: str) -> str | None:
    m = _FQCN_EXC_RE.search(text)
    return m.group(1) if m else None


def extract_from_file(file_path: Path) -> dict:
    """Return a dict describing the top-level and root-cause exceptions."""
    lines = file_path.read_text(errors="replace").splitlines()

    start = None
    for idx, line in enumerate(lines):
        if _FAILED_RE.match(line.strip()):
            start = idx + 1
            break
    if start is None:
        return {
            "top_exception": "<no-failed-marker>",
            "top_first_line": None,
            "root_cause": "<no-failed-marker>",
            "root_first_line": None,
            "caused_by_depth": 0,
        }

    tail = lines[start:]

    top_line = next((line for line in tail if line.strip()), None)
    top_exc = _find_exception_in_line(top_line or "") or "<unrecognized>"

    last_cb_line = None
    cb_count = 0
    for line in tail:
        m = _CAUSED_BY_RE.match(line)
        if m:
            cb_count += 1
            last_cb_line = m.group(1)

    if last_cb_line is not None:
        root_exc = _find_exception_in_line(last_cb_line) or "<unrecognized-cause>"
        root_first = last_cb_line.strip()
    else:
        root_exc = top_exc
        root_first = (top_line or "").strip()

    return {
        "top_exception": top_exc,
        "top_first_line": (top_line or "").strip() if top_line else None,
        "root_cause": root_exc,
        "root_first_line": root_first,
        "caused_by_depth": cb_count,
    }


def _truncate(text: str, limit: int = 160) -> str:
    text = text.replace("|", "\\|")
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _table_row(exc: str, items: list[dict], first_line_key: str) -> str:
    sample = items[0].get(first_line_key) or ""
    return f"| `{exc}` | {len(items)} | {_truncate(sample)} |"


def build_summary(split_dir: Path) -> tuple[list[dict], dict[str, list[dict]], dict[str, list[dict]]]:
    rows: list[dict] = []
    by_top: dict[str, list[dict]] = defaultdict(list)
    by_root: dict[str, list[dict]] = defaultdict(list)
    for log in sorted(split_dir.glob("*.log")):
        info = extract_from_file(log)
        entry = {"file": log.name, **info}
        rows.append(entry)
        by_top[info["top_exception"]].append(entry)
        by_root[info["root_cause"]].append(entry)
    return rows, by_top, by_root


def render_markdown(
    rows: list[dict],
    by_top: dict[str, list[dict]],
    by_root: dict[str, list[dict]],
    split_dir: Path,
    source_log: str | None,
) -> str:
    md: list[str] = []
    md.append("# Failure Exception Summary\n")
    if source_log:
        md.append(f"- Source log: `{source_log}`")
    md.append(f"- Split directory: `{split_dir}`")
    md.append(f"- Total failed test cases: **{len(rows)}**")
    md.append(f"- Distinct top-level exception classes: **{len(by_top)}**")
    md.append(f"- Distinct root-cause classes (deepest `Caused by:`): **{len(by_root)}**\n")

    md.append("## By root cause (deepest `Caused by:`)\n")
    md.append("| Root cause | Count | Sample first line |")
    md.append("| --- | ---: | --- |")
    for exc, items in sorted(by_root.items(), key=lambda kv: -len(kv[1])):
        md.append(_table_row(exc, items, "root_first_line"))
    md.append("")

    md.append("## By top-level exception (what JUnit reported)\n")
    md.append("| Top exception | Count | Sample first line |")
    md.append("| --- | ---: | --- |")
    for exc, items in sorted(by_top.items(), key=lambda kv: -len(kv[1])):
        md.append(_table_row(exc, items, "top_first_line"))
    md.append("")

    md.append("## Test cases grouped by root cause\n")
    for exc, items in sorted(by_root.items(), key=lambda kv: -len(kv[1])):
        md.append(f"### `{exc}` ({len(items)})\n")
        for it in items:
            md.append(f"- `{it['file']}`")
            md.append(f"    - top: `{it['top_exception']}`")
            md.append(f"    - root first line: {it['root_first_line']}")
        md.append("")

    return "\n".join(md)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--split-dir", required=True, type=Path,
                        help="Directory containing per-test split failure logs.")
    parser.add_argument("--output-dir", required=True, type=Path,
                        help="Directory to write exception_summary.{md,json}.")
    parser.add_argument("--source-log", default=None,
                        help="Optional path to the original full log, recorded in the summary header.")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if not args.split_dir.exists():
        raise SystemExit(f"split-dir does not exist: {args.split_dir}")

    rows, by_top, by_root = build_summary(args.split_dir)

    payload = {
        "source_log": args.source_log,
        "split_dir": str(args.split_dir),
        "total_failures": len(rows),
        "by_top_exception": {
            exc: {"count": len(items), "tests": [it["file"] for it in items]}
            for exc, items in by_top.items()
        },
        "by_root_cause": {
            exc: {"count": len(items), "tests": [it["file"] for it in items]}
            for exc, items in by_root.items()
        },
        "per_failure": rows,
    }
    (args.output_dir / "exception_summary.json").write_text(json.dumps(payload, indent=2))

    md = render_markdown(rows, by_top, by_root, args.split_dir, args.source_log)
    (args.output_dir / "exception_summary.md").write_text(md)

    print(f"Wrote {args.output_dir / 'exception_summary.json'}")
    print(f"Wrote {args.output_dir / 'exception_summary.md'}")
    print(f"Total failures: {len(rows)}")
    print(f"  top-level groups: {len(by_top)}")
    print(f"  root-cause groups: {len(by_root)}")


if __name__ == "__main__":
    main()
