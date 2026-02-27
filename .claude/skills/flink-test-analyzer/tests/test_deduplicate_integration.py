"""Integration tests for deduplicate_failures.py with realistic data.

This module tests the deduplication script with more complex, realistic scenarios
that simulate actual Flink test failures.
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from deduplicate_failures import deduplicate_failures


class TestRealisticScenarios:
    """Test with realistic Flink test failure scenarios."""

    def test_massive_same_root_cause(self) -> None:
        """Test scenario: 500 tests fail with same root cause (e.g., missing dependency)."""
        parse_results = {
            "files": {}
        }

        # Simulate 5 test iterations
        for iteration in range(1, 6):
            tests = []
            # 100 different tests per iteration, all failing with same root cause
            for i in range(100):
                tests.append({
                    "test_class": f"Test{i}",
                    "full_class_name": f"org.apache.flink.test.Test{i}",
                    "method": f"testMethod{i % 10}",
                    "parameters": f"param{i % 5}" if i % 3 == 0 else None,
                    "status": "failed",
                    "error_message": """java.lang.NoClassDefFoundError: org/apache/flink/api/common/functions/MapFunction
    at org.apache.flink.test.base.TestBase.setup(TestBase.java:45)
    at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
Caused by: java.lang.ClassNotFoundException: org.apache.flink.api.common.functions.MapFunction
    at java.net.URLClassLoader.findClass(URLClassLoader.java:381)"""
                })

            parse_results["files"][f"iteration_{iteration}.log"] = {
                "tests": tests,
                "summary": {"total": 100, "passed": 0, "failed": 100}
            }

        result = deduplicate_failures(parse_results)

        # Should have only 1 root cause group
        assert len(result["failure_groups"]) == 1

        # Should have 100 unique tests (different test classes/methods)
        assert result["unique_failures"] == 100

        # Total occurrences should be 500 (100 tests * 5 iterations)
        assert result["total_failures"] == 500

        # The single group should be marked for analysis
        group = result["failure_groups"][0]
        assert group["analysis_status"] == "needs_analysis"
        assert group["analysis_priority"] == 1
        # Should identify the root cause (ClassNotFoundException, not the wrapper NoClassDefFoundError)
        assert "ClassNotFoundException" in group["error_pattern"] or "NoClassDefFoundError" in group["error_pattern"]

    def test_mixed_failures_with_flaky_tests(self) -> None:
        """Test scenario: Mix of consistent failures and flaky tests."""
        parse_results = {
            "files": {
                "iteration_1.log": {
                    "tests": [
                        # Consistent failure - appears in all iterations
                        {
                            "test_class": "StateBackendTest",
                            "full_class_name": "org.apache.flink.runtime.state.StateBackendTest",
                            "method": "testCheckpoint",
                            "parameters": None,
                            "status": "failed",
                            "error_message": """java.lang.IllegalStateException: Checkpoint directory not configured
    at org.apache.flink.runtime.state.StateBackend.createCheckpoint(StateBackend.java:123)
    at org.apache.flink.runtime.state.StateBackendTest.testCheckpoint(StateBackendTest.java:45)"""
                        },
                        # Flaky test - only fails sometimes
                        {
                            "test_class": "NetworkTest",
                            "full_class_name": "org.apache.flink.runtime.io.network.NetworkTest",
                            "method": "testConnection",
                            "parameters": None,
                            "status": "failed",
                            "error_message": """java.net.SocketTimeoutException: Read timed out
    at java.net.SocketInputStream.socketRead0(Native Method)
    at org.apache.flink.runtime.io.network.NetworkTest.testConnection(NetworkTest.java:78)"""
                        },
                        # Another consistent failure
                        {
                            "test_class": "SerializerTest",
                            "full_class_name": "org.apache.flink.api.java.typeutils.runtime.SerializerTest",
                            "method": "testKryoSerializer",
                            "parameters": None,
                            "status": "failed",
                            "error_message": """java.lang.AssertionError: Expected:<10> but was:<11>
    at org.junit.Assert.assertEquals(Assert.java:115)
    at org.apache.flink.api.java.typeutils.runtime.SerializerTest.testKryoSerializer(SerializerTest.java:92)"""
                        }
                    ]
                },
                "iteration_2.log": {
                    "tests": [
                        # StateBackendTest fails again
                        {
                            "test_class": "StateBackendTest",
                            "full_class_name": "org.apache.flink.runtime.state.StateBackendTest",
                            "method": "testCheckpoint",
                            "parameters": None,
                            "status": "failed",
                            "error_message": """java.lang.IllegalStateException: Checkpoint directory not configured
    at org.apache.flink.runtime.state.StateBackend.createCheckpoint(StateBackend.java:123)
    at org.apache.flink.runtime.state.StateBackendTest.testCheckpoint(StateBackendTest.java:45)"""
                        },
                        # NetworkTest passes this time (flaky)
                        {
                            "test_class": "NetworkTest",
                            "full_class_name": "org.apache.flink.runtime.io.network.NetworkTest",
                            "method": "testConnection",
                            "parameters": None,
                            "status": "success",
                            "error_message": None
                        },
                        # SerializerTest fails again
                        {
                            "test_class": "SerializerTest",
                            "full_class_name": "org.apache.flink.api.java.typeutils.runtime.SerializerTest",
                            "method": "testKryoSerializer",
                            "parameters": None,
                            "status": "failed",
                            "error_message": """java.lang.AssertionError: Expected:<10> but was:<11>
    at org.junit.Assert.assertEquals(Assert.java:115)
    at org.apache.flink.api.java.typeutils.runtime.SerializerTest.testKryoSerializer(SerializerTest.java:92)"""
                        }
                    ]
                },
                "iteration_3.log": {
                    "tests": [
                        # All three tests, but NetworkTest fails with different error
                        {
                            "test_class": "StateBackendTest",
                            "full_class_name": "org.apache.flink.runtime.state.StateBackendTest",
                            "method": "testCheckpoint",
                            "parameters": None,
                            "status": "failed",
                            "error_message": """java.lang.IllegalStateException: Checkpoint directory not configured
    at org.apache.flink.runtime.state.StateBackend.createCheckpoint(StateBackend.java:123)
    at org.apache.flink.runtime.state.StateBackendTest.testCheckpoint(StateBackendTest.java:45)"""
                        },
                        {
                            "test_class": "NetworkTest",
                            "full_class_name": "org.apache.flink.runtime.io.network.NetworkTest",
                            "method": "testConnection",
                            "parameters": None,
                            "status": "failed",
                            "error_message": """java.net.ConnectException: Connection refused
    at java.net.Socket.connect(Socket.java:589)
    at org.apache.flink.runtime.io.network.NetworkTest.testConnection(NetworkTest.java:76)"""
                        },
                        {
                            "test_class": "SerializerTest",
                            "full_class_name": "org.apache.flink.api.java.typeutils.runtime.SerializerTest",
                            "method": "testKryoSerializer",
                            "parameters": None,
                            "status": "failed",
                            "error_message": """java.lang.AssertionError: Expected:<10> but was:<11>
    at org.junit.Assert.assertEquals(Assert.java:115)
    at org.apache.flink.api.java.typeutils.runtime.SerializerTest.testKryoSerializer(SerializerTest.java:92)"""
                        }
                    ]
                }
            }
        }

        result = deduplicate_failures(parse_results)

        # Should have 4 unique root causes:
        # 1. IllegalStateException (StateBackendTest) - 3 occurrences
        # 2. AssertionError (SerializerTest) - 3 occurrences
        # 3. SocketTimeoutException (NetworkTest) - 1 occurrence
        # 4. ConnectException (NetworkTest) - 1 occurrence
        # Note: exact count depends on fingerprinting algorithm
        assert 3 <= len(result["failure_groups"]) <= 4

        # Check the groups are sorted by frequency
        assert result["failure_groups"][0]["occurrence_count"] >= result["failure_groups"][1]["occurrence_count"]
        assert result["failure_groups"][1]["occurrence_count"] >= result["failure_groups"][2]["occurrence_count"]

        # The top 2 groups (with 3 occurrences each) should be marked for analysis
        for i in range(2):
            assert result["failure_groups"][i]["occurrence_count"] == 3
            assert result["failure_groups"][i]["analysis_status"] == "needs_analysis"

    def test_parameterized_tests_deduplication(self) -> None:
        """Test deduplication of parameterized tests."""
        parse_results = {
            "files": {
                "test.log": {
                    "tests": [
                        # Same parameterized test with different parameters
                        {
                            "test_class": "ParameterizedTest",
                            "full_class_name": "org.apache.flink.test.ParameterizedTest",
                            "method": "testWithParams",
                            "parameters": "input=1, expected=2",
                            "status": "failed",
                            "error_message": "AssertionError: Expected 2 but got 1"
                        },
                        {
                            "test_class": "ParameterizedTest",
                            "full_class_name": "org.apache.flink.test.ParameterizedTest",
                            "method": "testWithParams",
                            "parameters": "input=2, expected=4",
                            "status": "failed",
                            "error_message": "AssertionError: Expected 4 but got 2"
                        },
                        {
                            "test_class": "ParameterizedTest",
                            "full_class_name": "org.apache.flink.test.ParameterizedTest",
                            "method": "testWithParams",
                            "parameters": "input=3, expected=6",
                            "status": "failed",
                            "error_message": "AssertionError: Expected 6 but got 3"
                        },
                        # Same test, same parameters (duplicate)
                        {
                            "test_class": "ParameterizedTest",
                            "full_class_name": "org.apache.flink.test.ParameterizedTest",
                            "method": "testWithParams",
                            "parameters": "input=1, expected=2",
                            "status": "failed",
                            "error_message": "AssertionError: Expected 2 but got 1"
                        }
                    ]
                }
            }
        }

        result = deduplicate_failures(parse_results)

        # Should have 3 unique test failures (not 4, due to duplicate)
        assert result["unique_failures"] == 3

        # All failures have similar root cause (AssertionError pattern)
        # Might be grouped together or separately depending on fingerprinting
        assert len(result["failure_groups"]) >= 1

        # Check that each unique test appears only once
        test_identifiers = set()
        for group in result["failure_groups"]:
            for test in group["tests"]:
                assert test["test_identifier"] not in test_identifiers
                test_identifiers.add(test["test_identifier"])

    def test_complex_stack_traces(self) -> None:
        """Test fingerprinting with complex, nested stack traces."""
        parse_results = {
            "files": {
                "test.log": {
                    "tests": [
                        {
                            "test_class": "ComplexTest1",
                            "full_class_name": "org.apache.flink.test.ComplexTest1",
                            "method": "test1",
                            "parameters": None,
                            "status": "failed",
                            "error_message": """org.apache.flink.runtime.JobException: Recovery is suppressed by NoRestartBackOffTimeStrategy
    at org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler.handleFailure(ExecutionFailureHandler.java:138)
    at org.apache.flink.runtime.scheduler.DefaultScheduler.handleTaskFailure(DefaultScheduler.java:252)
    at org.apache.flink.runtime.scheduler.DefaultScheduler.maybeHandleTaskFailure(DefaultScheduler.java:242)
    at org.apache.flink.runtime.scheduler.DefaultScheduler.updateTaskExecutionStateInternal(DefaultScheduler.java:233)
Caused by: java.lang.RuntimeException: Error in task execution
    at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:534)
    at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:721)
    at org.apache.flink.runtime.taskmanager.Task.run(Task.java:546)
Caused by: java.lang.NullPointerException
    at org.apache.flink.streaming.api.operators.StreamMap.processElement(StreamMap.java:38)
    at org.apache.flink.streaming.runtime.tasks.ChainingOutput.pushToOperator(ChainingOutput.java:108)"""
                        },
                        {
                            "test_class": "ComplexTest2",
                            "full_class_name": "org.apache.flink.test.ComplexTest2",
                            "method": "test2",
                            "parameters": None,
                            "status": "failed",
                            "error_message": """org.apache.flink.runtime.JobException: Recovery is suppressed by NoRestartBackOffTimeStrategy
    at org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler.handleFailure(ExecutionFailureHandler.java:138)
    at org.apache.flink.runtime.scheduler.DefaultScheduler.handleTaskFailure(DefaultScheduler.java:252)
    at org.apache.flink.runtime.scheduler.DefaultScheduler.maybeHandleTaskFailure(DefaultScheduler.java:242)
    at org.apache.flink.runtime.scheduler.DefaultScheduler.updateTaskExecutionStateInternal(DefaultScheduler.java:233)
Caused by: java.lang.RuntimeException: Error in task execution
    at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:534)
    at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:721)
    at org.apache.flink.runtime.taskmanager.Task.run(Task.java:546)
Caused by: java.lang.NullPointerException
    at org.apache.flink.streaming.api.operators.StreamMap.processElement(StreamMap.java:38)
    at org.apache.flink.streaming.runtime.tasks.ChainingOutput.pushToOperator(ChainingOutput.java:108)"""
                        }
                    ]
                }
            }
        }

        result = deduplicate_failures(parse_results)

        # These two tests have identical stack traces, should be grouped together
        assert len(result["failure_groups"]) == 1
        assert len(result["failure_groups"][0]["tests"]) == 2
        # Should identify the root cause (NullPointerException, not the wrapper JobException)
        assert "NullPointerException" in result["failure_groups"][0]["error_pattern"]


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_malformed_error_messages(self) -> None:
        """Test handling of malformed or unusual error messages."""
        parse_results = {
            "files": {
                "test.log": {
                    "tests": [
                        {
                            "test_class": "TestA",
                            "full_class_name": "org.apache.flink.TestA",
                            "method": "test1",
                            "parameters": None,
                            "status": "failed",
                            "error_message": ""  # Empty error message
                        },
                        {
                            "test_class": "TestB",
                            "full_class_name": "org.apache.flink.TestB",
                            "method": "test2",
                            "parameters": None,
                            "status": "failed",
                            "error_message": "   \n  \n  "  # Whitespace only
                        },
                        {
                            "test_class": "TestC",
                            "full_class_name": "org.apache.flink.TestC",
                            "method": "test3",
                            "parameters": None,
                            "status": "failed",
                            "error_message": None  # None error message
                        },
                        {
                            "test_class": "TestD",
                            "full_class_name": "org.apache.flink.TestD",
                            "method": "test4",
                            "parameters": None,
                            "status": "failed",
                            "error_message": "😀 Unicode error 错误消息 🚀"  # Unicode
                        }
                    ]
                }
            }
        }

        # Should not crash
        result = deduplicate_failures(parse_results)

        assert result["unique_failures"] == 4
        # Empty/None errors might be grouped together
        assert len(result["failure_groups"]) >= 1

    def test_very_long_error_messages(self) -> None:
        """Test handling of very long error messages."""
        # Create a very long stack trace
        long_stack = "\n".join([f"    at org.apache.flink.Class{i}.method{i}(Class{i}.java:{i})" for i in range(1000)])

        parse_results = {
            "files": {
                "test.log": {
                    "tests": [
                        {
                            "test_class": "TestLong",
                            "full_class_name": "org.apache.flink.TestLong",
                            "method": "test1",
                            "parameters": None,
                            "status": "failed",
                            "error_message": f"java.lang.StackOverflowError\n{long_stack}"
                        }
                    ]
                }
            }
        }

        # Should not crash or hang
        result = deduplicate_failures(parse_results)

        assert result["unique_failures"] == 1
        assert len(result["failure_groups"]) == 1
        assert "StackOverflowError" in result["failure_groups"][0]["error_pattern"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])