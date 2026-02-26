"""Tests for parse_logs.py - Flink test log parser."""

import json
import sys
from pathlib import Path
from unittest.mock import patch
from io import StringIO

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from parse_logs import parse_log_content, parse_log_files, main


# ---------------------------------------------------------------------------
# Sample log fragments used across tests
# ---------------------------------------------------------------------------

SINGLE_SUCCESS_LOG = """\
================================================================================
Test execute[pipeline with local channels, p = 1, timeout = 0](org.apache.flink.test.checkpointing.UnalignedCheckpointITCase) is running.
--------------------------------------------------------------------------------
some test output line 1
some test output line 2
--------------------------------------------------------------------------------
Test execute[pipeline with local channels, p = 1, timeout = 0](org.apache.flink.test.checkpointing.UnalignedCheckpointITCase) successfully run.
================================================================================
"""

SINGLE_FAILURE_LOG = """\
================================================================================
Test execute[p=1](org.apache.flink.TestA) is running.
--------------------------------------------------------------------------------
detailed log content here
--------------------------------------------------------------------------------
Test execute[p=1](org.apache.flink.TestA) failed with:
java.lang.AssertionError: expected:<1> but was:<2>
    at org.junit.Assert.fail(Assert.java:88)
    at org.junit.Assert.failNotEquals(Assert.java:834)
================================================================================
"""

MIXED_LOG = """\
================================================================================
Test execute[p=1](org.apache.flink.TestA) is running.
--------------------------------------------------------------------------------
test output A p=1
--------------------------------------------------------------------------------
Test execute[p=1](org.apache.flink.TestA) failed with:
java.lang.AssertionError: expected:<1> but was:<2>
    at org.junit.Assert.fail(Assert.java:88)
================================================================================
================================================================================
Test execute[p=2](org.apache.flink.TestA) is running.
--------------------------------------------------------------------------------
test output A p=2
--------------------------------------------------------------------------------
Test execute[p=2](org.apache.flink.TestA) successfully run.
================================================================================
================================================================================
Test run(org.apache.flink.TestB) is running.
--------------------------------------------------------------------------------
test output B
--------------------------------------------------------------------------------
Test run(org.apache.flink.TestB) successfully run.
================================================================================
"""

NO_PARAMS_LOG = """\
================================================================================
Test testMethod(org.apache.flink.streaming.tests.SomeTest) is running.
--------------------------------------------------------------------------------
output
--------------------------------------------------------------------------------
Test testMethod(org.apache.flink.streaming.tests.SomeTest) successfully run.
================================================================================
"""

FAILURE_NO_PARAMS_LOG = """\
================================================================================
Test testMethod(org.apache.flink.streaming.tests.SomeTest) is running.
--------------------------------------------------------------------------------
output
--------------------------------------------------------------------------------
Test testMethod(org.apache.flink.streaming.tests.SomeTest) failed with:
java.lang.NullPointerException
    at com.example.Foo.bar(Foo.java:10)
================================================================================
"""


class TestParseLogContent:
    """Tests for parse_log_content function."""

    def test_single_success(self):
        results = parse_log_content(SINGLE_SUCCESS_LOG)
        assert len(results) == 1
        t = results[0]
        assert t["test_class"] == "UnalignedCheckpointITCase"
        assert t["full_class_name"] == "org.apache.flink.test.checkpointing.UnalignedCheckpointITCase"
        assert t["method"] == "execute"
        assert t["parameters"] == "pipeline with local channels, p = 1, timeout = 0"
        assert t["status"] == "success"
        assert t["error_message"] is None

    def test_single_failure(self):
        results = parse_log_content(SINGLE_FAILURE_LOG)
        assert len(results) == 1
        t = results[0]
        assert t["test_class"] == "TestA"
        assert t["full_class_name"] == "org.apache.flink.TestA"
        assert t["method"] == "execute"
        assert t["parameters"] == "p=1"
        assert t["status"] == "failed"
        assert "expected:<1> but was:<2>" in t["error_message"]

    def test_mixed_results(self):
        results = parse_log_content(MIXED_LOG)
        assert len(results) == 3

        failed = [r for r in results if r["status"] == "failed"]
        passed = [r for r in results if r["status"] == "success"]
        assert len(failed) == 1
        assert len(passed) == 2

        assert failed[0]["parameters"] == "p=1"
        assert failed[0]["test_class"] == "TestA"

    def test_non_parameterized_test_success(self):
        results = parse_log_content(NO_PARAMS_LOG)
        assert len(results) == 1
        t = results[0]
        assert t["test_class"] == "SomeTest"
        assert t["full_class_name"] == "org.apache.flink.streaming.tests.SomeTest"
        assert t["method"] == "testMethod"
        assert t["parameters"] is None
        assert t["status"] == "success"

    def test_non_parameterized_test_failure(self):
        results = parse_log_content(FAILURE_NO_PARAMS_LOG)
        assert len(results) == 1
        t = results[0]
        assert t["parameters"] is None
        assert t["status"] == "failed"
        assert "NullPointerException" in t["error_message"]

    def test_empty_log(self):
        results = parse_log_content("")
        assert results == []

    def test_log_without_test_markers(self):
        results = parse_log_content("random log output\nno test markers here\n")
        assert results == []

    def test_error_message_captures_stack_trace(self):
        results = parse_log_content(SINGLE_FAILURE_LOG)
        err = results[0]["error_message"]
        assert "at org.junit.Assert.fail" in err

    def test_multiple_failures_in_one_log(self):
        log = """\
================================================================================
Test m1[a](org.example.X) is running.
--------------------------------------------------------------------------------
out1
--------------------------------------------------------------------------------
Test m1[a](org.example.X) failed with:
Error1
================================================================================
================================================================================
Test m2[b](org.example.Y) is running.
--------------------------------------------------------------------------------
out2
--------------------------------------------------------------------------------
Test m2[b](org.example.Y) failed with:
Error2
================================================================================
"""
        results = parse_log_content(log)
        assert len(results) == 2
        assert all(r["status"] == "failed" for r in results)
        assert results[0]["error_message"].strip() == "Error1"
        assert results[1]["error_message"].strip() == "Error2"


class TestParseLogFiles:
    """Tests for parse_log_files function."""

    def test_single_file(self, tmp_path):
        log_file = tmp_path / "test1.log"
        log_file.write_text(SINGLE_SUCCESS_LOG)

        result = parse_log_files([str(log_file)])
        assert "test1.log" in result["files"]
        file_data = result["files"]["test1.log"]
        assert file_data["summary"]["total"] == 1
        assert file_data["summary"]["passed"] == 1
        assert file_data["summary"]["failed"] == 0

    def test_multiple_files(self, tmp_path):
        f1 = tmp_path / "run1.log"
        f1.write_text(SINGLE_SUCCESS_LOG)

        f2 = tmp_path / "run2.log"
        f2.write_text(SINGLE_FAILURE_LOG)

        result = parse_log_files([str(f1), str(f2)])
        assert len(result["files"]) == 2
        assert result["files"]["run1.log"]["summary"]["passed"] == 1
        assert result["files"]["run2.log"]["summary"]["failed"] == 1

    def test_mixed_file(self, tmp_path):
        log_file = tmp_path / "mixed.log"
        log_file.write_text(MIXED_LOG)

        result = parse_log_files([str(log_file)])
        summary = result["files"]["mixed.log"]["summary"]
        assert summary["total"] == 3
        assert summary["passed"] == 2
        assert summary["failed"] == 1

    def test_empty_file(self, tmp_path):
        log_file = tmp_path / "empty.log"
        log_file.write_text("")

        result = parse_log_files([str(log_file)])
        summary = result["files"]["empty.log"]["summary"]
        assert summary["total"] == 0
        assert summary["passed"] == 0
        assert summary["failed"] == 0


class TestMainCli:
    """Tests for CLI entry point."""

    def test_main_outputs_json(self, tmp_path):
        log_file = tmp_path / "test.log"
        log_file.write_text(SINGLE_SUCCESS_LOG)

        with patch("sys.argv", ["parse_logs.py", str(log_file)]):
            buf = StringIO()
            with patch("sys.stdout", buf):
                main()

        output = json.loads(buf.getvalue())
        assert "files" in output
        assert "test.log" in output["files"]

    def test_main_no_args_exits(self):
        with patch("sys.argv", ["parse_logs.py"]):
            with pytest.raises(SystemExit):
                main()
