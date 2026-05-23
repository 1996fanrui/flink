# Flink Test Analyzer Scripts

This directory contains Python scripts for analyzing Flink test results.

## Scripts Overview

### 1. parse_logs.py
Parses Flink test log files and extracts structured test results.

**Usage:**
```bash
uv run python parse_logs.py file1.log file2.log ... > parse_results.json
```

**Output:** JSON with test results grouped by file, including test class, method, parameters, status, and error messages.

### 2. deduplicate_failures.py
Deduplicates test failures for root cause analysis using a two-level strategy:
- Level 1: Same test deduplication (based on class, method, parameters)
- Level 2: Same root cause deduplication (based on error fingerprint)

**Usage:**
```bash
uv run python deduplicate_failures.py parse_results.json deduplicated_failures.json [--verbose]
```

**Input:** parse_results.json from parse_logs.py
**Output:** JSON with deduplicated failure groups, sorted by frequency

**Key Features:**
- Identifies root cause from nested "Caused by" exceptions
- Groups failures with identical error patterns
- Applies high-frequency-first analysis strategy
- Marks uncertain low-frequency groups as "possibly similar"

### 3. generate_report.py
Generates markdown reports from parsed test results.

**Usage:**
```bash
uv run python generate_report.py < parse_results.json [options]
```

**Options:**
- `--commit-hash`: Git commit hash
- `--branch-name`: Git branch name
- `--test-request`: Test request description
- `--output-dir`: Directory to write report files
- `--split-dir`: Directory containing split failure logs

**Output:**
- report.md: Summary report with statistics
- failure_details.md: Detailed failure information

### 4. split_failure_logs.py
Splits a single log file into individual failure files.

**Usage:**
```bash
uv run python split_failure_logs.py input.log [output_dir]
```

**Output:** Individual log files for each failed test in output_dir

### 5. extract_exception_summary.py
Builds a by-exception summary from the per-test split failure logs. For each
split file it identifies the top-level exception (what JUnit reported) and
the deepest `Caused by:` root-cause class, then aggregates counts.

**Usage:**
```bash
uv run python extract_exception_summary.py \
    --split-dir <split failures dir> \
    --output-dir <where to write summaries> \
    [--source-log <original full log, recorded in the header>]
```

**Output:**
- `exception_summary.json`: per-failure rows plus counts grouped by both
  top-level exception and root cause.
- `exception_summary.md`: human-readable Markdown summary.

This script is **mandatory** and must run before any root-cause analysis
(Phase 6.5 in `SKILL.md`). The summary is the entry point for deciding
which failure groups need deeper analysis.

### 6. find_failures.sh
Shell script to find test failures in a log file using awk.

**Usage:**
```bash
./find_failures.sh test.log
```

### 7. run_tests.sh
Shell script to run Flink tests with proper configuration.

**Usage:**
```bash
./run_tests.sh <test_pattern> [additional_mvn_args]
```

## Typical Workflow

1. Run tests and capture logs:
   ```bash
   ./run_tests.sh "TestClassName" > test.log 2>&1
   ```

2. Parse the log files:
   ```bash
   uv run python parse_logs.py test.log > parse_results.json
   ```

3. Split failures into per-test logs and build the by-exception summary
   (mandatory before any root-cause analysis):
   ```bash
   uv run python split_failure_logs.py --output-dir reports/split_failures test.log
   uv run python extract_exception_summary.py \
       --split-dir reports/split_failures \
       --output-dir reports \
       --source-log test.log
   ```

4. Deduplicate failures for root cause analysis:
   ```bash
   uv run python deduplicate_failures.py parse_results.json deduplicated_failures.json --verbose
   ```

5. Generate reports:
   ```bash
   uv run python generate_report.py < parse_results.json --output-dir reports/
   ```

## Testing

All scripts have comprehensive unit tests in the `tests/` directory. Run tests with:

```bash
uv run pytest tests/ -v
```

## Dependencies

The scripts use only Python standard library modules and have no external dependencies for runtime. Testing requires pytest.