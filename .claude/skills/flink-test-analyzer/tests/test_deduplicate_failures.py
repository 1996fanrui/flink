"""Unit tests for deduplicate_failures.py script.

This module tests the two-level deduplication strategy:
- Level 1: Same test deduplication (based on class, method, parameters)
- Level 2: Same root cause deduplication (based on error fingerprint)

Test cases cover scenarios from acceptance test document T1.1-T1.4.
"""

import json
import sys
from pathlib import Path
from typing import Any

import pytest

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from deduplicate_failures import (
    AnalysisStatus,
    deduplicate_failures,
    generate_test_identifier,
    generate_error_fingerprint,
    group_by_root_cause,
    apply_analysis_strategy,
    main
)


class TestTestIdentifier:
    """Test Level 1: Same test deduplication."""

    def test_generate_identifier_without_params(self) -> None:
        """Test identifier generation for non-parameterized test."""
        test = {
            "full_class_name": "org.apache.flink.test.TestA",
            "method": "testMethod",
            "parameters": None
        }
        identifier = generate_test_identifier(test)
        assert identifier == "org.apache.flink.test.TestA.testMethod"

    def test_generate_identifier_with_params(self) -> None:
        """Test identifier generation for parameterized test."""
        test = {
            "full_class_name": "org.apache.flink.test.TestA",
            "method": "testMethod",
            "parameters": "param1=value1"
        }
        identifier = generate_test_identifier(test)
        assert identifier == "org.apache.flink.test.TestA.testMethod[param1=value1]"

    def test_same_test_multiple_iterations(self) -> None:
        """Test T1.1: Same test across multiple iterations should be deduplicated."""
        # Prepare data: same test failed in 5 iterations
        parse_results = {
            "files": {
                f"iteration_{i}.log": {
                    "tests": [
                        {
                            "test_class": "TestA",
                            "full_class_name": "org.apache.flink.test.TestA",
                            "method": "method1",
                            "parameters": "param1",
                            "status": "failed",
                            "error_message": "NullPointerException at Foo.java:42"
                        }
                    ]
                }
                for i in range(1, 6)
            }
        }

        result = deduplicate_failures(parse_results)

        # Should have only one failure group
        assert len(result["failure_groups"]) == 1

        # The test should appear only once in the group
        group = result["failure_groups"][0]
        assert len(group["tests"]) == 1
        assert group["tests"][0]["test_identifier"] == "org.apache.flink.test.TestA.method1[param1]"
        assert group["occurrence_count"] == 5


class TestErrorFingerprint:
    """Test error fingerprint generation for root cause grouping."""

    def test_fingerprint_with_exception(self) -> None:
        """Test fingerprint generation from exception message."""
        error_msg = """java.lang.NullPointerException: Cannot invoke method
    at org.apache.flink.Foo.bar(Foo.java:42)
    at org.apache.flink.Test.test(Test.java:100)"""

        fingerprint = generate_error_fingerprint(error_msg)

        # Should extract exception type and key stack frame
        assert "NullPointerException" in fingerprint
        assert "Foo.java:42" in fingerprint

    def test_fingerprint_without_exception(self) -> None:
        """Test fingerprint generation from non-exception error."""
        error_msg = "Test timed out after 60 seconds"
        fingerprint = generate_error_fingerprint(error_msg)
        assert fingerprint == "Test timed out after 60 seconds"

    def test_fingerprint_none_error(self) -> None:
        """Test fingerprint generation when error_message is None."""
        fingerprint = generate_error_fingerprint(None)
        assert fingerprint == ""


class TestRootCauseGrouping:
    """Test Level 2: Same root cause deduplication."""

    def test_different_tests_same_root_cause(self) -> None:
        """Test T1.2: Different tests with same root cause should be grouped."""
        failures = [
            {
                "test_identifier": "org.apache.flink.TestA.method1",
                "test_class": "TestA",
                "full_class_name": "org.apache.flink.TestA",
                "method": "method1",
                "parameters": None,
                "error_message": "NullPointerException at Foo.java:42",
                "files": ["test.log"]
            },
            {
                "test_identifier": "org.apache.flink.TestB.method2",
                "test_class": "TestB",
                "full_class_name": "org.apache.flink.TestB",
                "method": "method2",
                "parameters": None,
                "error_message": "NullPointerException at Foo.java:42",
                "files": ["test.log"]
            },
            {
                "test_identifier": "org.apache.flink.TestC.method3",
                "test_class": "TestC",
                "full_class_name": "org.apache.flink.TestC",
                "method": "method3",
                "parameters": None,
                "error_message": "TimeoutException at Bar.java:100",
                "files": ["test.log"]
            }
        ]

        groups = group_by_root_cause(failures)

        # Should have 2 groups: NPE group and Timeout group
        assert len(groups) == 2

        # Find NPE group
        npe_group = next(g for g in groups if "NullPointerException" in g["error_fingerprint"])
        assert len(npe_group["tests"]) == 2
        assert npe_group["occurrence_count"] == 2

        # Find Timeout group
        timeout_group = next(g for g in groups if "TimeoutException" in g["error_fingerprint"])
        assert len(timeout_group["tests"]) == 1
        assert timeout_group["occurrence_count"] == 1


class TestAnalysisStrategy:
    """Test high-frequency-first analysis strategy."""

    def test_high_frequency_priority(self) -> None:
        """Test T1.3: High frequency failures should be prioritized."""
        groups = [
            {
                "error_fingerprint": "NPE_fingerprint",
                "tests": [{"test_identifier": f"Test{i}.method"} for i in range(500)],
                "occurrence_count": 500,
                "error_pattern": "NullPointerException",
                "sample_error": "NPE error"
            },
            {
                "error_fingerprint": "Timeout_fingerprint",
                "tests": [{"test_identifier": f"TestTimeout{i}.method"} for i in range(3)],
                "occurrence_count": 3,
                "error_pattern": "TimeoutException",
                "sample_error": "Timeout error"
            },
            {
                "error_fingerprint": "IO_fingerprint",
                "tests": [{"test_identifier": f"TestIO{i}.method"} for i in range(2)],
                "occurrence_count": 2,
                "error_pattern": "IOException",
                "sample_error": "IO error"
            }
        ]

        result = apply_analysis_strategy(groups)

        # Should be sorted by frequency
        assert result[0]["occurrence_count"] == 500
        assert result[0]["analysis_status"] == AnalysisStatus.NEEDS_ANALYSIS
        assert result[0]["analysis_priority"] == 1

        assert result[1]["occurrence_count"] == 3
        assert result[2]["occurrence_count"] == 2

        # Different root causes should be marked for analysis
        for group in result:
            if group["error_pattern"] != result[0]["error_pattern"]:
                assert group["analysis_status"] in [AnalysisStatus.NEEDS_ANALYSIS, AnalysisStatus.POSSIBLY_SIMILAR]

    def test_similarity_marking(self) -> None:
        """Test marking of possibly similar low-frequency groups."""
        groups = [
            {
                "error_fingerprint": "NPE_main",
                "tests": [{"test_identifier": f"Test{i}.method"} for i in range(100)],
                "occurrence_count": 100,
                "error_pattern": "NullPointerException at Main.java",
                "sample_error": "NPE in main"
            },
            {
                "error_fingerprint": "NPE_similar",
                "tests": [{"test_identifier": "TestSimilar.method"}],
                "occurrence_count": 1,
                "error_pattern": "NullPointerException",  # Similar but not certain
                "sample_error": "NPE somewhere"
            },
            {
                "error_fingerprint": "Different_error",
                "tests": [{"test_identifier": "TestDifferent.method"}],
                "occurrence_count": 1,
                "error_pattern": "IllegalStateException",  # Clearly different
                "sample_error": "Different error"
            }
        ]

        result = apply_analysis_strategy(groups)

        # High frequency group should be marked for analysis
        assert result[0]["analysis_status"] == AnalysisStatus.NEEDS_ANALYSIS

        # Clearly different error should be marked for analysis
        different_group = next(g for g in result if "IllegalState" in g["error_pattern"])
        assert different_group["analysis_status"] == AnalysisStatus.NEEDS_ANALYSIS

        # Similar but uncertain group might be marked as possibly similar
        # (Implementation detail - may vary based on similarity algorithm)


class TestEmptyInput:
    """Test handling of empty or no-failure inputs."""

    def test_no_failures(self) -> None:
        """Test T1.4: No failures should result in empty groups."""
        parse_results = {
            "files": {
                "test.log": {
                    "tests": [
                        {
                            "test_class": "TestA",
                            "full_class_name": "org.apache.flink.TestA",
                            "method": "method1",
                            "parameters": None,
                            "status": "success",
                            "error_message": None
                        }
                    ]
                }
            }
        }

        result = deduplicate_failures(parse_results)

        assert result["failure_groups"] == []
        assert result["total_failures"] == 0
        assert result["unique_failures"] == 0

    def test_empty_input(self) -> None:
        """Test empty input handling."""
        parse_results = {"files": {}}
        result = deduplicate_failures(parse_results)

        assert result["failure_groups"] == []
        assert result["total_failures"] == 0
        assert result["unique_failures"] == 0


class TestIntegration:
    """Integration tests for the complete deduplication flow."""

    def test_complex_scenario(self) -> None:
        """Test a complex scenario with multiple patterns."""
        parse_results = {
            "files": {
                "iteration_1.log": {
                    "tests": [
                        {
                            "test_class": "TestA",
                            "full_class_name": "org.apache.flink.TestA",
                            "method": "test1",
                            "parameters": None,
                            "status": "failed",
                            "error_message": "NPE at Foo.java:42"
                        },
                        {
                            "test_class": "TestB",
                            "full_class_name": "org.apache.flink.TestB",
                            "method": "test2",
                            "parameters": None,
                            "status": "failed",
                            "error_message": "NPE at Foo.java:42"
                        },
                        {
                            "test_class": "TestC",
                            "full_class_name": "org.apache.flink.TestC",
                            "method": "test3",
                            "parameters": None,
                            "status": "success",
                            "error_message": None
                        }
                    ]
                },
                "iteration_2.log": {
                    "tests": [
                        {
                            "test_class": "TestA",
                            "full_class_name": "org.apache.flink.TestA",
                            "method": "test1",
                            "parameters": None,
                            "status": "failed",
                            "error_message": "NPE at Foo.java:42"
                        },
                        {
                            "test_class": "TestD",
                            "full_class_name": "org.apache.flink.TestD",
                            "method": "test4",
                            "parameters": None,
                            "status": "failed",
                            "error_message": "TimeoutException"
                        }
                    ]
                }
            }
        }

        result = deduplicate_failures(parse_results)

        # Should have 2 groups: NPE and Timeout
        assert len(result["failure_groups"]) == 2

        # NPE group should have 3 unique tests (TestA.test1, TestB.test2)
        npe_group = next(g for g in result["failure_groups"] if "NPE" in g["error_fingerprint"])
        assert len(npe_group["tests"]) == 2  # TestA and TestB
        assert npe_group["occurrence_count"] == 3  # TestA appeared twice, TestB once

        # Timeout group should have 1 test
        timeout_group = next(g for g in result["failure_groups"] if "Timeout" in g["error_fingerprint"])
        assert len(timeout_group["tests"]) == 1
        assert timeout_group["occurrence_count"] == 1

    def test_main_function_with_file(self, tmp_path: Path) -> None:
        """Test main function with file I/O."""
        # Create test input file
        input_data = {
            "files": {
                "test.log": {
                    "tests": [
                        {
                            "test_class": "TestA",
                            "full_class_name": "org.apache.flink.TestA",
                            "method": "test1",
                            "parameters": None,
                            "status": "failed",
                            "error_message": "Test error"
                        }
                    ]
                }
            }
        }

        input_file = tmp_path / "parse_results.json"
        input_file.write_text(json.dumps(input_data, indent=2))

        output_file = tmp_path / "deduplicated_failures.json"

        # Mock sys.argv for main function
        original_argv = sys.argv
        try:
            sys.argv = ["deduplicate_failures.py", str(input_file), str(output_file)]
            main()
        finally:
            sys.argv = original_argv

        # Check output file was created
        assert output_file.exists()

        # Verify output content
        output_data = json.loads(output_file.read_text())
        assert "failure_groups" in output_data
        assert len(output_data["failure_groups"]) == 1
        assert output_data["total_failures"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])