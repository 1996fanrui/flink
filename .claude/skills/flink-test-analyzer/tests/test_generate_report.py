"""Tests for generate_report.py - Markdown test report generator."""

import json
import sys
from pathlib import Path
from unittest.mock import patch
from io import StringIO

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from generate_report import (
    calculate_coarse_stats,
    calculate_fine_stats,
    generate_failure_details,
    generate_report,
    main,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_parse_result(*file_specs) -> dict:
    """Build a parse_logs-style result dict.

    Each file_spec is (filename, [(status, class, method, params, error), ...]).
    """
    files = {}
    for filename, tests in file_specs:
        test_list = []
        for status, cls, method, params, error in tests:
            test_list.append({
                "test_class": cls,
                "full_class_name": f"org.example.{cls}",
                "method": method,
                "parameters": params,
                "status": status,
                "error_message": error,
            })
        passed = sum(1 for t in test_list if t["status"] == "success")
        failed = sum(1 for t in test_list if t["status"] == "failed")
        files[filename] = {
            "tests": test_list,
            "summary": {"total": len(test_list), "passed": passed, "failed": failed},
        }
    return {"files": files}


ALL_PASS = _make_parse_result(
    ("run1.log", [
        ("success", "TestA", "m1", "p1", None),
        ("success", "TestA", "m1", "p2", None),
    ]),
    ("run2.log", [
        ("success", "TestA", "m1", "p1", None),
        ("success", "TestA", "m1", "p2", None),
    ]),
)

MIXED = _make_parse_result(
    ("run1.log", [
        ("success", "TestA", "m1", "p1", None),
        ("failed", "TestA", "m1", "p2", "Error in p2"),
    ]),
    ("run2.log", [
        ("success", "TestA", "m1", "p1", None),
        ("success", "TestA", "m1", "p2", None),
    ]),
    ("run3.log", [
        ("success", "TestA", "m1", "p1", None),
        ("success", "TestA", "m1", "p2", None),
    ]),
)

ALL_FAIL = _make_parse_result(
    ("run1.log", [
        ("failed", "TestX", "exec", "a", "err1"),
    ]),
)


class TestCalculateCoarseStats:
    def test_all_pass(self):
        stats = calculate_coarse_stats(ALL_PASS)
        assert stats["total_iterations"] == 2
        assert stats["fully_successful"] == 2
        assert stats["partially_failed"] == 0
        assert stats["success_rate"] == pytest.approx(1.0)

    def test_mixed(self):
        stats = calculate_coarse_stats(MIXED)
        assert stats["total_iterations"] == 3
        assert stats["fully_successful"] == 2
        assert stats["partially_failed"] == 1
        assert stats["success_rate"] == pytest.approx(2 / 3)

    def test_all_fail(self):
        stats = calculate_coarse_stats(ALL_FAIL)
        assert stats["total_iterations"] == 1
        assert stats["fully_successful"] == 0
        assert stats["partially_failed"] == 1
        assert stats["success_rate"] == pytest.approx(0.0)

    def test_empty(self):
        stats = calculate_coarse_stats({"files": {}})
        assert stats["total_iterations"] == 0
        assert stats["fully_successful"] == 0
        assert stats["partially_failed"] == 0
        assert stats["success_rate"] == pytest.approx(0.0)


class TestCalculateFineStats:
    def test_basic(self):
        stats = calculate_fine_stats(MIXED)
        # TestA.m1[p1] appeared 3 times, all success
        key_p1 = ("TestA", "m1", "p1")
        assert stats[key_p1]["total"] == 3
        assert stats[key_p1]["passed"] == 3
        assert stats[key_p1]["failed"] == 0

        # TestA.m1[p2] appeared 3 times, 1 failed
        key_p2 = ("TestA", "m1", "p2")
        assert stats[key_p2]["total"] == 3
        assert stats[key_p2]["passed"] == 2
        assert stats[key_p2]["failed"] == 1

    def test_non_parameterized(self):
        data = _make_parse_result(
            ("f.log", [("success", "B", "run", None, None)]),
        )
        stats = calculate_fine_stats(data)
        key = ("B", "run", None)
        assert stats[key]["total"] == 1
        assert stats[key]["passed"] == 1

    def test_empty(self):
        stats = calculate_fine_stats({"files": {}})
        assert stats == {}


class TestGenerateReport:
    def test_contains_summary_section(self):
        report = generate_report(MIXED)
        assert "# Test Report" in report
        assert "Summary" in report

    def test_contains_coarse_stats(self):
        report = generate_report(MIXED)
        assert "Coarse" in report
        assert "3" in report  # total iterations
        assert "2" in report  # fully successful

    def test_contains_fine_stats(self):
        report = generate_report(MIXED)
        assert "Fine" in report
        assert "TestA" in report
        assert "m1" in report

    def test_summary_references_failure_details(self):
        """Summary report should reference failure_details.md, not contain stack traces."""
        report = generate_report(MIXED)
        assert "failure_details.md" in report
        # The actual error message should NOT be in the summary report
        assert "Error in p2" not in report

    def test_includes_commit_hash(self):
        report = generate_report(MIXED, commit_hash="abc123")
        assert "abc123" in report

    def test_includes_branch_name(self):
        report = generate_report(MIXED, branch_name="feature/test")
        assert "feature/test" in report

    def test_includes_test_request(self):
        report = generate_report(MIXED, test_request="Run checkpoint tests")
        assert "Run checkpoint tests" in report

    def test_includes_awk_command(self):
        report = generate_report(MIXED)
        assert "awk" in report

    def test_all_pass_no_failure_section_content(self):
        report = generate_report(ALL_PASS)
        # When all tests pass, there should be no reference to failure_details.md
        assert "failure_details.md" not in report

    def test_empty_input(self):
        report = generate_report({"files": {}})
        assert "# Test Report" in report

    def test_fine_stats_sorted_by_failure_rate(self):
        """Higher failure rate tests should appear before lower ones."""
        data = _make_parse_result(
            ("f1.log", [
                ("failed", "A", "m", "p1", "errA"),
                ("failed", "B", "m", "p1", "errB"),
            ]),
            ("f2.log", [
                ("success", "A", "m", "p1", None),
                ("failed", "B", "m", "p1", "errB"),
            ]),
        )
        report = generate_report(data)
        # B has 100% failure rate, A has 50%. B should appear first.
        pos_b = report.find("| B ")
        pos_a = report.find("| A ")
        assert pos_b < pos_a

    def test_fine_stats_only_shows_failed_items(self):
        """Fine-grained table should only contain test cases that have failures."""
        report = generate_report(MIXED)
        # p2 (has failures) should be in table
        assert "p2" in report
        # p1 (always passes) should NOT be in the fine-grained table rows
        lines = report.split("\n")
        table_rows = [l for l in lines if l.startswith("| TestA")]
        assert len(table_rows) == 1  # only the p2 row
        assert "p2" in table_rows[0]

    def test_fine_stats_summary_totals(self):
        """Fine-grained section should have a summary line with totals."""
        report = generate_report(MIXED)
        assert "Total:" in report
        assert "unique test cases" in report

    def test_fine_stats_execution_overview(self):
        """Fine-grained section should start with total execution overview."""
        report = generate_report(MIXED)
        # MIXED has 2 test cases × 3 iterations = 6 total executions
        # 5 passed, 1 failed
        assert "Total Executions" in report
        assert "6" in report
        assert "Passed" in report
        assert "Failed" in report

    def test_fine_stats_execution_overview_counts(self):
        """Verify exact execution overview numbers."""
        data = _make_parse_result(
            ("r1.log", [
                ("success", "A", "m1", "p1", None),
                ("failed", "A", "m1", "p2", "err"),
                ("success", "B", "run", None, None),
            ]),
            ("r2.log", [
                ("success", "A", "m1", "p1", None),
                ("success", "A", "m1", "p2", None),
                ("failed", "B", "run", None, "err2"),
            ]),
        )
        report = generate_report(data)
        # 3 unique test cases × 2 iterations = 6 executions, 4 passed, 2 failed
        assert "**Total Executions**: 6" in report
        assert "**Passed**: 4" in report
        assert "**Failed**: 2" in report


class TestGenerateFailureDetails:
    def test_basic(self):
        details = generate_failure_details(MIXED)
        assert "# Failure Details" in details
        assert "Error in p2" in details
        assert "TestA" in details

    def test_with_split_dir(self):
        details = generate_failure_details(MIXED, split_dir="/tmp/splits")
        assert "/tmp/splits" in details

    def test_no_failures(self):
        details = generate_failure_details(ALL_PASS)
        assert "No failures" in details


class TestMainCli:
    def test_reads_stdin_json(self):
        input_json = json.dumps(MIXED)

        with patch("sys.stdin", StringIO(input_json)):
            with patch("sys.argv", ["generate_report.py"]):
                buf = StringIO()
                with patch("sys.stdout", buf):
                    main()

        output = buf.getvalue()
        assert "# Test Report" in output

    def test_with_options(self):
        input_json = json.dumps(MIXED)

        with patch("sys.stdin", StringIO(input_json)):
            with patch("sys.argv", [
                "generate_report.py",
                "--commit-hash", "deadbeef",
                "--branch-name", "main",
            ]):
                buf = StringIO()
                with patch("sys.stdout", buf):
                    main()

        output = buf.getvalue()
        assert "deadbeef" in output
        assert "main" in output

    def test_output_dir_writes_files(self, tmp_path):
        input_json = json.dumps(MIXED)
        output_dir = str(tmp_path / "output")

        with patch("sys.stdin", StringIO(input_json)):
            with patch("sys.argv", [
                "generate_report.py",
                "--output-dir", output_dir,
                "--split-dir", "/tmp/splits",
            ]):
                main()

        report_path = tmp_path / "output" / "report.md"
        details_path = tmp_path / "output" / "failure_details.md"
        assert report_path.exists()
        assert details_path.exists()
        assert "# Test Report" in report_path.read_text()
        assert "# Failure Details" in details_path.read_text()
