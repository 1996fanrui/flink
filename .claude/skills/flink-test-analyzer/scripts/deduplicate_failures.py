#!/usr/bin/env python3
"""Deduplicate test failures for root cause analysis.

This script implements a two-level deduplication strategy:
1. Level 1: Same test deduplication (based on class, method, parameters)
2. Level 2: Same root cause deduplication (based on error fingerprint)

After deduplication, it applies a high-frequency-first analysis strategy
to prioritize which failure groups should be analyzed.

Usage:
    python deduplicate_failures.py <input_file> <output_file> [--verbose]

Examples:
    # Basic deduplication
    python deduplicate_failures.py parse_results.json deduplicated_failures.json

    # With verbose summary printed to stderr
    python deduplicate_failures.py parse_results.json deduplicated_failures.json --verbose

Input format:
    JSON file produced by parse_logs.py with structure:
    {
        "files": {
            "<log_file_name>": {
                "tests": [
                    {
                        "test_class": "TestA",
                        "full_class_name": "org.apache.flink.test.TestA",
                        "method": "testMethod",
                        "parameters": "param1" | null,
                        "status": "failed" | "success",
                        "error_message": "..." | null
                    }
                ]
            }
        }
    }

Output:
    JSON file with deduplicated failure groups sorted by frequency:
    {
        "failure_groups": [...],
        "total_failures": <int>,
        "unique_failures": <int>,
        "unique_root_causes": <int>,
        "analysis_summary": {
            "groups_needing_analysis": <int>,
            "groups_possibly_similar": <int>
        }
    }
"""

import argparse
import json
import re
import sys
from enum import StrEnum
from pathlib import Path
from typing import Any, Dict, List, Optional


class AnalysisStatus(StrEnum):
    NEEDS_ANALYSIS = "needs_analysis"
    POSSIBLY_SIMILAR = "possibly_similar"


def generate_test_identifier(test: Dict[str, Any]) -> str:
    """Generate a unique identifier for a test.

    Args:
        test: Test dict with full_class_name, method, and optional parameters

    Returns:
        Unique test identifier string
    """
    identifier = f"{test['full_class_name']}.{test['method']}"
    if test.get("parameters"):
        identifier += f"[{test['parameters']}]"
    return identifier


def generate_error_fingerprint(error_message: Optional[str]) -> str:
    """Generate a fingerprint for an error to identify same root cause.

    Extracts key information from error messages:
    - Root cause exception type (from "Caused by:" if present)
    - Key stack frame location
    - Normalized error message (removing timestamps, IDs, etc.)

    Args:
        error_message: The error message text, or None

    Returns:
        Error fingerprint string for grouping
    """
    if not error_message:
        return ""

    fingerprint_parts = []

    # Look for root cause exceptions first (Caused by:)
    # These are more important than the wrapper exceptions
    caused_by_matches = re.findall(r'Caused by:\s*(\w+(?:\.\w+)*(?:Exception|Error))', error_message)
    if caused_by_matches:
        # Use the deepest (last) "Caused by" as the root cause
        root_cause = caused_by_matches[-1]
        fingerprint_parts.append(root_cause)

        # Try to find the stack frame after this root cause
        pattern = f'Caused by:\\s*{re.escape(root_cause)}.*?\\n\\s*at\\s+[\\w.$]+\\((\\w+\\.java:\\d+)\\)'
        stack_match = re.search(pattern, error_message, re.DOTALL)
        if stack_match:
            fingerprint_parts.append(stack_match.group(1))
    else:
        # No "Caused by" - use the top-level exception
        exception_match = re.search(r'(\w+(?:\.\w+)*(?:Exception|Error))', error_message)
        if exception_match:
            fingerprint_parts.append(exception_match.group(1))

        # Extract first meaningful stack frame location
        # Matches patterns like "at package.Class.method(File.java:123)"
        stack_match = re.search(r'at\s+[\w.$]+\((\w+\.java:\d+)\)', error_message)
        if stack_match:
            fingerprint_parts.append(stack_match.group(1))

    # If no exception pattern found, use the first line (normalized)
    if not fingerprint_parts:
        first_line = error_message.split('\n')[0] if error_message else ""
        # Remove timestamps (various formats)
        first_line = re.sub(r'\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}', '', first_line)
        # Remove UUIDs
        first_line = re.sub(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', '', first_line)
        # Remove numbers that might be IDs or ports
        first_line = re.sub(r'\b\d{4,}\b', '', first_line)
        fingerprint_parts.append(first_line.strip())

    return '|'.join(fingerprint_parts)


def deduplicate_same_tests(parse_results: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Level 1 deduplication: Remove duplicate occurrences of same test.

    Args:
        parse_results: Parsed test results from parse_logs.py

    Returns:
        List of unique failed tests with occurrence tracking
    """
    test_map: Dict[str, Dict[str, Any]] = {}

    for file_name, file_data in parse_results.get("files", {}).items():
        for test in file_data.get("tests", []):
            # Skip successful tests
            if test.get("status") != "failed":
                continue

            identifier = generate_test_identifier(test)

            if identifier not in test_map:
                test_map[identifier] = {
                    "test_identifier": identifier,
                    "test_class": test["test_class"],
                    "full_class_name": test["full_class_name"],
                    "method": test["method"],
                    "parameters": test.get("parameters"),
                    "error_message": test.get("error_message"),
                    "files": []
                }

            # Track which files this test failed in
            test_map[identifier]["files"].append(file_name)

    return list(test_map.values())


def group_by_root_cause(failures: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Level 2 deduplication: Group failures by same root cause.

    Args:
        failures: List of unique failed tests

    Returns:
        List of failure groups, each containing tests with same root cause
    """
    groups: Dict[str, Dict[str, Any]] = {}

    for failure in failures:
        fingerprint = generate_error_fingerprint(failure.get("error_message"))

        if fingerprint not in groups:
            groups[fingerprint] = {
                "error_fingerprint": fingerprint,
                "tests": [],
                "occurrence_count": 0,
                "error_pattern": extract_error_pattern(failure.get("error_message")),
                "sample_error": failure.get("error_message")
            }

        groups[fingerprint]["tests"].append({
            "test_identifier": failure["test_identifier"],
            "test_class": failure["test_class"],
            "method": failure["method"],
            "parameters": failure.get("parameters"),
            "occurrence_count": len(failure["files"])
        })
        groups[fingerprint]["occurrence_count"] += len(failure["files"])

    return list(groups.values())


def extract_error_pattern(error_message: Optional[str]) -> str:
    """Extract a human-readable error pattern from error message.

    Args:
        error_message: The error message text

    Returns:
        Short description of the error pattern
    """
    if not error_message:
        return "Unknown error"

    # Look for root cause exceptions first (Caused by:)
    caused_by_matches = re.findall(r'Caused by:\s*(\w+(?:\.\w+)*(?:Exception|Error))', error_message)
    if caused_by_matches:
        # Use the deepest (last) "Caused by" as the root cause
        root_cause = caused_by_matches[-1]
        # Extract just the class name without package
        simple_name = root_cause.split('.')[-1]

        # Try to find the location after this root cause
        pattern = f'Caused by:\\s*{re.escape(root_cause)}.*?\\n\\s*at\\s+[\\w.$]+\\((\\w+\\.java:\\d+)\\)'
        stack_match = re.search(pattern, error_message, re.DOTALL)
        if stack_match:
            return f"{simple_name} at {stack_match.group(1)}"
        return simple_name

    # Check for common exception types (if no Caused by found)
    exception_match = re.search(r'(\w+(?:\.\w+)*(?:Exception|Error))', error_message)
    if exception_match:
        exception_type = exception_match.group(1)
        simple_name = exception_type.split('.')[-1]

        # Try to get location
        match = re.search(r'at\s+([\w.$]+)\((\w+\.java:\d+)\)', error_message)
        if match:
            return f"{simple_name} at {match.group(2)}"
        return simple_name

    # For non-exception errors (assertions, etc.)
    if "AssertionError" in error_message:
        # Try to extract assertion message
        first_line = error_message.split('\n')[0]
        return f"AssertionError: {first_line[:100]}" if len(first_line) > 100 else f"AssertionError: {first_line}"

    # Default: use first line
    first_line = error_message.split('\n')[0]
    return first_line[:100] if len(first_line) > 100 else first_line


def calculate_similarity(group1: Dict[str, Any], group2: Dict[str, Any]) -> float:
    """Calculate similarity score between two failure groups.

    Args:
        group1: First failure group
        group2: Second failure group

    Returns:
        Similarity score between 0 and 1
    """
    # Simple heuristic: check if exception types are the same
    pattern1 = group1.get("error_pattern", "")
    pattern2 = group2.get("error_pattern", "")

    # Extract exception type from pattern
    exc_type1 = re.search(r'(\w+Exception|\w+Error)', pattern1)
    exc_type2 = re.search(r'(\w+Exception|\w+Error)', pattern2)

    if exc_type1 and exc_type2:
        if exc_type1.group(1) == exc_type2.group(1):
            # Same exception type - possibly similar
            return 0.7

    # Check for keyword overlap
    keywords1 = set(re.findall(r'\b\w+\b', pattern1.lower()))
    keywords2 = set(re.findall(r'\b\w+\b', pattern2.lower()))

    if keywords1 and keywords2:
        overlap = len(keywords1 & keywords2)
        total = len(keywords1 | keywords2)
        if total > 0:
            return overlap / total

    return 0.0


def apply_analysis_strategy(groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Apply high-frequency-first analysis strategy to failure groups.

    Strategy:
    1. Sort by frequency (descending)
    2. Mark high-frequency groups as "needs_analysis"
    3. Mark clearly different groups as "needs_analysis"
    4. Mark uncertain low-frequency groups as "possibly_similar"

    Args:
        groups: List of failure groups

    Returns:
        List of failure groups with analysis strategy applied
    """
    # Sort by occurrence count (high to low)
    sorted_groups = sorted(groups, key=lambda g: g["occurrence_count"], reverse=True)

    # Apply analysis strategy
    for i, group in enumerate(sorted_groups):
        group["analysis_priority"] = i + 1

        # High frequency groups always need analysis
        if i == 0 or group["occurrence_count"] >= 10:
            group["analysis_status"] = AnalysisStatus.NEEDS_ANALYSIS
            group["analysis_reason"] = "High frequency failure"
        else:
            # Check similarity with higher priority groups
            similar_to = None
            max_similarity = 0.0

            for j in range(i):
                similarity = calculate_similarity(group, sorted_groups[j])
                if similarity > max_similarity:
                    max_similarity = similarity
                    similar_to = sorted_groups[j]

            if max_similarity > 0.6:
                # Possibly similar to a higher priority group
                group["analysis_status"] = AnalysisStatus.POSSIBLY_SIMILAR
                group["similar_to"] = similar_to["error_pattern"]
                group["analysis_reason"] = f"Possibly similar to: {similar_to['error_pattern']}"
            else:
                # Clearly different - needs separate analysis
                group["analysis_status"] = AnalysisStatus.NEEDS_ANALYSIS
                group["analysis_reason"] = "Distinct error pattern"

    return sorted_groups


def deduplicate_failures(parse_results: Dict[str, Any]) -> Dict[str, Any]:
    """Main deduplication function combining both levels.

    Args:
        parse_results: Parsed test results from parse_logs.py

    Returns:
        Deduplicated and prioritized failure groups
    """
    # Level 1: Deduplicate same tests
    unique_failures = deduplicate_same_tests(parse_results)

    if not unique_failures:
        return {
            "failure_groups": [],
            "total_failures": 0,
            "unique_failures": 0,
            "analysis_summary": {
                "groups_needing_analysis": 0,
                "groups_possibly_similar": 0
            }
        }

    # Level 2: Group by root cause
    failure_groups = group_by_root_cause(unique_failures)

    # Apply analysis strategy
    prioritized_groups = apply_analysis_strategy(failure_groups)

    # Calculate summary statistics
    total_failures = sum(g["occurrence_count"] for g in prioritized_groups)
    groups_needing_analysis = sum(1 for g in prioritized_groups if g["analysis_status"] == AnalysisStatus.NEEDS_ANALYSIS)
    groups_possibly_similar = sum(1 for g in prioritized_groups if g["analysis_status"] == AnalysisStatus.POSSIBLY_SIMILAR)

    return {
        "failure_groups": prioritized_groups,
        "total_failures": total_failures,
        "unique_failures": len(unique_failures),
        "unique_root_causes": len(prioritized_groups),
        "analysis_summary": {
            "groups_needing_analysis": groups_needing_analysis,
            "groups_possibly_similar": groups_possibly_similar
        }
    }


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Deduplicate test failures for root cause analysis."
    )
    parser.add_argument(
        "input_file",
        help="Path to parse_results.json from parse_logs.py"
    )
    parser.add_argument(
        "output_file",
        help="Path to output deduplicated_failures.json"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print summary to stderr"
    )

    args = parser.parse_args()

    # Read input
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Error: Input file {input_path} does not exist", file=sys.stderr)
        sys.exit(1)

    with open(input_path, "r") as f:
        parse_results = json.load(f)

    # Perform deduplication
    result = deduplicate_failures(parse_results)

    # Write output
    output_path = Path(args.output_file)
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    # Print summary if verbose
    if args.verbose:
        print(f"\nDeduplication Summary:", file=sys.stderr)
        print(f"  Total failures: {result['total_failures']}", file=sys.stderr)
        print(f"  Unique test failures: {result['unique_failures']}", file=sys.stderr)
        print(f"  Unique root causes: {result['unique_root_causes']}", file=sys.stderr)
        print(f"  Groups needing analysis: {result['analysis_summary']['groups_needing_analysis']}", file=sys.stderr)
        print(f"  Groups possibly similar: {result['analysis_summary']['groups_possibly_similar']}", file=sys.stderr)
        print(f"\nOutput written to: {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()