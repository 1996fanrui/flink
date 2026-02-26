"""Tests for split_failure_logs.py - failure log splitter."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from split_failure_logs import split_failure_logs, sanitize_filename, main


MIXED_LOG = """\
================================================================================
Test execute[p=1](org.apache.flink.TestA) is running.
--------------------------------------------------------------------------------
detailed log content here
--------------------------------------------------------------------------------
Test execute[p=1](org.apache.flink.TestA) failed with:
java.lang.AssertionError: expected:<1> but was:<2>
    at org.junit.Assert.fail(Assert.java:88)
================================================================================
================================================================================
Test execute[p=2](org.apache.flink.TestA) is running.
--------------------------------------------------------------------------------
success log content
--------------------------------------------------------------------------------
Test execute[p=2](org.apache.flink.TestA) successfully run.
================================================================================
"""

MULTIPLE_FAILURES_LOG = """\
================================================================================
Test m1[a](org.example.X) is running.
--------------------------------------------------------------------------------
output for m1[a]
--------------------------------------------------------------------------------
Test m1[a](org.example.X) failed with:
Error1
================================================================================
================================================================================
Test m2[b](org.example.Y) is running.
--------------------------------------------------------------------------------
output for m2[b]
--------------------------------------------------------------------------------
Test m2[b](org.example.Y) failed with:
Error2
    at com.example.Y.m2(Y.java:5)
================================================================================
"""

ALL_SUCCESS_LOG = """\
================================================================================
Test run(org.apache.flink.TestB) is running.
--------------------------------------------------------------------------------
output
--------------------------------------------------------------------------------
Test run(org.apache.flink.TestB) successfully run.
================================================================================
"""

NO_PARAMS_FAILURE_LOG = """\
================================================================================
Test testMethod(org.apache.flink.streaming.tests.SomeTest) is running.
--------------------------------------------------------------------------------
output for non-param test
--------------------------------------------------------------------------------
Test testMethod(org.apache.flink.streaming.tests.SomeTest) failed with:
java.lang.NullPointerException
================================================================================
"""


class TestSanitizeFilename:
    def test_replaces_special_chars(self):
        assert sanitize_filename("a/b:c*d?e") == "a_b_c_d_e"

    def test_preserves_safe_chars(self):
        assert sanitize_filename("Test_method[p=1]") == "Test_method[p=1]"

    def test_brackets_and_equals_preserved(self):
        name = "TestA_execute[p=1]_from_run1.log"
        assert sanitize_filename(name) == name


class TestSplitFailureLogs:
    def test_splits_single_failure(self, tmp_path):
        log_file = tmp_path / "20260226_111126.log"
        log_file.write_text(MIXED_LOG)
        output_dir = tmp_path / "split_failures"

        files = split_failure_logs(str(log_file), str(output_dir))

        assert len(files) == 1
        assert output_dir.exists()

        split_file = Path(files[0])
        assert split_file.exists()
        assert "TestA" in split_file.name
        assert "execute" in split_file.name
        assert "p=1" in split_file.name
        assert "20260226_111126" in split_file.name

        content = split_file.read_text()
        assert "is running" in content
        assert "failed with" in content
        assert "detailed log content here" in content
        # Must NOT contain the success test's content
        assert "success log content" not in content

    def test_splits_multiple_failures(self, tmp_path):
        log_file = tmp_path / "run.log"
        log_file.write_text(MULTIPLE_FAILURES_LOG)
        output_dir = tmp_path / "out"

        files = split_failure_logs(str(log_file), str(output_dir))
        assert len(files) == 2

        contents = [Path(f).read_text() for f in files]
        assert any("output for m1[a]" in c for c in contents)
        assert any("output for m2[b]" in c for c in contents)

    def test_no_failures_returns_empty(self, tmp_path):
        log_file = tmp_path / "success.log"
        log_file.write_text(ALL_SUCCESS_LOG)
        output_dir = tmp_path / "out"

        files = split_failure_logs(str(log_file), str(output_dir))
        assert files == []

    def test_empty_log_returns_empty(self, tmp_path):
        log_file = tmp_path / "empty.log"
        log_file.write_text("")
        output_dir = tmp_path / "out"

        files = split_failure_logs(str(log_file), str(output_dir))
        assert files == []

    def test_non_parameterized_failure(self, tmp_path):
        log_file = tmp_path / "noparams.log"
        log_file.write_text(NO_PARAMS_FAILURE_LOG)
        output_dir = tmp_path / "out"

        files = split_failure_logs(str(log_file), str(output_dir))
        assert len(files) == 1

        split_file = Path(files[0])
        assert "SomeTest" in split_file.name
        assert "testMethod" in split_file.name
        content = split_file.read_text()
        assert "NullPointerException" in content

    def test_creates_output_dir(self, tmp_path):
        log_file = tmp_path / "test.log"
        log_file.write_text(MIXED_LOG)
        output_dir = tmp_path / "nested" / "deep" / "dir"

        split_failure_logs(str(log_file), str(output_dir))
        assert output_dir.exists()

    def test_error_message_included_in_split(self, tmp_path):
        log_file = tmp_path / "test.log"
        log_file.write_text(MIXED_LOG)
        output_dir = tmp_path / "out"

        files = split_failure_logs(str(log_file), str(output_dir))
        content = Path(files[0]).read_text()
        assert "AssertionError" in content
        assert "expected:<1> but was:<2>" in content


class TestMainCli:
    def test_main_creates_files(self, tmp_path):
        log_file = tmp_path / "test.log"
        log_file.write_text(MIXED_LOG)
        output_dir = tmp_path / "split_out"

        with patch("sys.argv", [
            "split_failure_logs.py",
            "--output-dir", str(output_dir),
            str(log_file),
        ]):
            main()

        assert output_dir.exists()
        split_files = list(output_dir.iterdir())
        assert len(split_files) == 1

    def test_main_no_args_exits(self):
        with patch("sys.argv", ["split_failure_logs.py"]):
            with pytest.raises(SystemExit):
                main()

    def test_main_default_output_dir(self, tmp_path, monkeypatch):
        log_file = tmp_path / "test.log"
        log_file.write_text(MIXED_LOG)
        monkeypatch.chdir(tmp_path)

        with patch("sys.argv", ["split_failure_logs.py", str(log_file)]):
            main()

        default_dir = tmp_path / "split_failures"
        assert default_dir.exists()
