"""Generate a Markdown test report with dual-granularity statistics.

Usage:
    python parse_logs.py *.log | python generate_report.py [OPTIONS]

Options:
    --commit-hash       Git commit hash for the tested code
    --branch-name       Git branch name
    --test-request      Description of the test request
    --split-dir         Path to split_failures directory (for log references)
    --output-dir        Directory to write report.md and failure_details.md
                        (if omitted, prints summary report to stdout)

Reads JSON produced by parse_logs.py from stdin and writes a Markdown
report to stdout or to files in the output directory.
"""

import argparse
import json
import os
import sys
from collections import defaultdict


def calculate_coarse_stats(parse_result: dict) -> dict:
    """Calculate iteration-level (coarse-grained) statistics.

    An "iteration" corresponds to one log file. An iteration is "fully
    successful" only if every test in that file passed.
    """
    files = parse_result.get("files", {})
    total = len(files)
    fully_successful = sum(
        1 for f in files.values() if f["summary"]["failed"] == 0
    )
    partially_failed = total - fully_successful

    return {
        "total_iterations": total,
        "fully_successful": fully_successful,
        "partially_failed": partially_failed,
        "success_rate": fully_successful / total if total > 0 else 0.0,
    }


def calculate_fine_stats(
    parse_result: dict,
) -> dict[tuple[str, str, str | None], dict]:
    """Calculate per test_class + method + parameters statistics.

    Returns a dict keyed by (test_class, method, parameters) with values
    {"total": int, "passed": int, "failed": int}.
    """
    stats: dict[tuple[str, str, str | None], dict] = defaultdict(
        lambda: {"total": 0, "passed": 0, "failed": 0}
    )

    for file_data in parse_result.get("files", {}).values():
        for test in file_data["tests"]:
            key = (test["test_class"], test["method"], test["parameters"])
            stats[key]["total"] += 1
            if test["status"] == "success":
                stats[key]["passed"] += 1
            else:
                stats[key]["failed"] += 1

    return dict(stats)


def _collect_failures(parse_result: dict) -> list[dict]:
    """Collect all failed tests with source file info."""
    failures: list[dict] = []
    for filename, file_data in parse_result.get("files", {}).items():
        for test in file_data["tests"]:
            if test["status"] == "failed":
                failures.append({**test, "source_file": filename})
    return failures


def generate_report(
    parse_result: dict,
    *,
    commit_hash: str | None = None,
    branch_name: str | None = None,
    test_request: str | None = None,
) -> str:
    """Generate a Markdown summary report (without failure details)."""
    lines: list[str] = []

    coarse = calculate_coarse_stats(parse_result)
    fine = calculate_fine_stats(parse_result)
    failures = _collect_failures(parse_result)

    lines.append("# Test Report")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    if commit_hash:
        lines.append(f"- **Commit**: `{commit_hash}`")
    if branch_name:
        lines.append(f"- **Branch**: `{branch_name}`")
    if test_request:
        lines.append(f"- **Test Request**: {test_request}")
    lines.append(f"- **Total Iterations**: {coarse['total_iterations']}")
    lines.append(
        f"- **Overall Success Rate**: "
        f"{coarse['success_rate']:.1%}"
    )
    lines.append("")

    lines.append("## Coarse-Grained Statistics (per iteration)")
    lines.append("")
    lines.append("| Metric | Count |")
    lines.append("|--------|-------|")
    lines.append(f"| Total Iterations | {coarse['total_iterations']} |")
    lines.append(f"| Fully Successful | {coarse['fully_successful']} |")
    lines.append(f"| Partially Failed | {coarse['partially_failed']} |")
    lines.append(
        f"| Success Rate | {coarse['success_rate']:.1%} |"
    )
    lines.append("")

    lines.append("## Fine-Grained Statistics (per test case)")
    lines.append("")

    if fine:
        total_executions = sum(s["total"] for s in fine.values())
        total_passed = sum(s["passed"] for s in fine.values())
        total_failed = sum(s["failed"] for s in fine.values())
        total_unique = len(fine)

        lines.append(f"- **Total Executions**: {total_executions}")
        lines.append(
            f"- **Passed**: {total_passed} "
            f"({total_passed / total_executions:.1%})" if total_executions > 0
            else f"- **Passed**: {total_passed}"
        )
        lines.append(
            f"- **Failed**: {total_failed} "
            f"({total_failed / total_executions:.1%})" if total_executions > 0
            else f"- **Failed**: {total_failed}"
        )
        lines.append("")

        failed_keys = [k for k in fine if fine[k]["failed"] > 0]
        failed_keys.sort(
            key=lambda k: fine[k]["failed"] / fine[k]["total"] if fine[k]["total"] > 0 else 0,
            reverse=True,
        )

        if failed_keys:
            lines.append("| Test Class | Method | Parameters | Total | Passed | Failed | Failure Rate |")
            lines.append("|------------|--------|------------|-------|--------|--------|--------------|")

            for key in failed_keys:
                cls, method, params = key
                s = fine[key]
                rate = s["failed"] / s["total"] if s["total"] > 0 else 0
                param_display = params if params is not None else "-"
                lines.append(
                    f"| {cls} | {method} | {param_display} | "
                    f"{s['total']} | {s['passed']} | {s['failed']} | {rate:.1%} |"
                )
            lines.append("")

        always_passed = total_unique - len(failed_keys)
        lines.append(
            f"*Total: {total_unique} unique test cases "
            f"({always_passed} always passed, {len(failed_keys)} had failures)*"
        )
        lines.append("")
    else:
        lines.append("No test data available.")
        lines.append("")

    if failures:
        lines.append(
            "> See failure_details.md for complete failure information including stack traces."
        )
        lines.append("")

    lines.append("## Quick Failure Location")
    lines.append("")
    lines.append(
        "Use the following command to quickly locate failures in log files:"
    )
    lines.append("```bash")
    lines.append(
        "awk '/\\[ERROR\\] Errors:/{if(!found) print \"---\"; found=1} "
        "found{print FILENAME\": \"$0} "
        "/Tests run:/{if(found) found=0}' ./202*.log"
    )
    lines.append("```")
    lines.append("")

    return "\n".join(lines)


def generate_failure_details(
    parse_result: dict,
    *,
    split_dir: str | None = None,
) -> str:
    """Generate failure details markdown with stack traces and log paths."""
    lines: list[str] = []
    failures = _collect_failures(parse_result)

    lines.append("# Failure Details")
    lines.append("")

    if failures:
        for i, f in enumerate(failures, 1):
            params_str = f"[{f['parameters']}]" if f["parameters"] else ""
            lines.append(
                f"### {i}. {f['test_class']}.{f['method']}{params_str}"
            )
            lines.append(f"- **Source**: `{f['source_file']}`")
            if split_dir:
                cls = f["test_class"]
                method = f["method"]
                src_stem = f["source_file"].rsplit(".", 1)[0]
                if f["parameters"]:
                    split_name = f"{cls}_{method}[{f['parameters']}]_from_{src_stem}.log"
                else:
                    split_name = f"{cls}_{method}_from_{src_stem}.log"
                lines.append(f"- **Split Log**: `{split_dir}/{split_name}`")
            if f["error_message"]:
                lines.append(f"- **Error**:")
                lines.append("```")
                lines.append(f["error_message"].strip())
                lines.append("```")
            lines.append("")
    else:
        lines.append("No failures detected.")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a Markdown test report from parse_logs.py output."
    )
    parser.add_argument("--commit-hash", default=None, help="Git commit hash.")
    parser.add_argument("--branch-name", default=None, help="Git branch name.")
    parser.add_argument(
        "--test-request", default=None, help="Test request description."
    )
    parser.add_argument(
        "--split-dir", default=None,
        help="Path to split_failures directory for log references.",
    )
    parser.add_argument(
        "--output-dir", default=None,
        help="Directory to write report.md and failure_details.md.",
    )
    args = parser.parse_args()

    parse_result = json.load(sys.stdin)

    report = generate_report(
        parse_result,
        commit_hash=args.commit_hash,
        branch_name=args.branch_name,
        test_request=args.test_request,
    )

    if args.output_dir:
        os.makedirs(args.output_dir, exist_ok=True)

        report_path = os.path.join(args.output_dir, "report.md")
        with open(report_path, "w") as f:
            f.write(report)

        details = generate_failure_details(
            parse_result,
            split_dir=args.split_dir,
        )
        details_path = os.path.join(args.output_dir, "failure_details.md")
        with open(details_path, "w") as f:
            f.write(details)
    else:
        print(report)


if __name__ == "__main__":
    main()
