---
name: commit-test-generator
description: Generate unit tests for each commit in a development branch. Use when you need to add test coverage for existing commits.
model: opus
---

# Commit Test Generator Skill

You are the **coordinator** for generating unit tests for commits. You delegate all work to sub-agents and verify results.

## Input Examples

```bash
# Generate tests for last 3 commits
/skill commit-test-generator HEAD~3..HEAD

# Generate tests for specific commit range
/skill commit-test-generator abc123..def456

# Generate tests for commits between branches
/skill commit-test-generator master..feature-branch
```

## Argument Parsing

```python
import sys
args = sys.argv[1] if len(sys.argv) > 1 else "HEAD~1..HEAD"
COMMIT_RANGE = args.strip()
```

## Global Variables

```python
import time
TIMESTAMP = str(int(time.time()))
WORKTREE_NAME = f"test-gen-{TIMESTAMP}"
WORKTREE_PATH = f".claude/worktrees/{WORKTREE_NAME}"
COMMITS_FILE = f"log/test-gen-{TIMESTAMP}/commits.json"
TESTS_DIR = f"log/test-gen-{TIMESTAMP}/generated-tests"
REPORT_FILE = f"log/test-gen-{TIMESTAMP}/report.md"
```

## Phase 1: Parse Commit Range

**Goal**: Parse user input and get list of commits to process

**Input**: `COMMIT_RANGE` (e.g., "HEAD~3..HEAD")
**Output**: `commits.json` with commit list

**Steps**:
1. Create log directory: `mkdir -p log/test-gen-{TIMESTAMP}`
2. Parse commits with sub-agent:

```markdown
<task>
Parse the commit range and extract commit information.

Input:
- Commit range: {COMMIT_RANGE}
- Output file: {COMMITS_FILE}

Run these commands:
```bash
git log {COMMIT_RANGE} --format='%H|%s|%an|%ae' > raw_commits.txt
```

Then parse raw_commits.txt and create JSON at {COMMITS_FILE}:
```json
{
  "commits": [
    {
      "hash": "abc123",
      "subject": "[ISSUE-10] Add feature X",
      "author": "John Doe",
      "email": "john@example.com",
      "files_changed": ["src/main/java/Feature.java"]
    }
  ]
}
```
</task>
```

**Gate**: Continue only if commits.json exists and has entries
**Verification**: Verify commits.json structure matches expected format

## Phase 2: Analyze Commits

**Goal**: Filter commits that need test generation

**Input**: `commits.json`
**Output**: `commits_needing_tests.json`

**Steps**:
1. Analyze each commit with sub-agent:

```markdown
<task>
Analyze commits to identify which need test generation.

Input file: {COMMITS_FILE}
Output file: log/test-gen-{TIMESTAMP}/commits_needing_tests.json

For each commit:
1. Check if it modifies Java source files (not test files)
2. Check if corresponding test already exists
3. Mark commits that need tests

Output format:
```json
{
  "commits_needing_tests": [
    {
      "hash": "abc123",
      "subject": "[ISSUE-10] Add feature X",
      "source_files": ["src/main/java/Feature.java"],
      "test_file": "src/test/java/FeatureTest.java",
      "test_exists": false
    }
  ]
}
```
</task>
```

**Gate**: Skip test generation if no commits need tests
**Verification**: Verify output file exists and is valid JSON

## Phase 3: Generate Tests Loop

**Goal**: Generate unit tests for each commit

**Input**: `commits_needing_tests.json`
**Output**: Test files in `TESTS_DIR`

**Steps**:
1. Create worktree for isolation:

```markdown
<task>
Create git worktree for test generation:
```bash
git worktree add {WORKTREE_PATH} -b test-gen-{TIMESTAMP}
```
</task>
```

2. For each commit in commits_needing_tests:

```markdown
<task>
Generate unit test for commit {commit_hash}.

Steps:
1. Checkout commit in worktree:
```bash
cd {WORKTREE_PATH}
git checkout {commit_hash}
```

2. Read source file: {source_file}
3. Generate comprehensive unit test covering:
   - All public methods
   - Edge cases
   - Exception scenarios
   - Integration with dependencies

4. Save test to: {TESTS_DIR}/{commit_hash}/{test_file}

Use Flink testing conventions:
- Extend AbstractTestBase for runtime tests
- Use @Test annotation
- Follow existing test patterns in the module
</task>
```

**Gate**: Continue to next commit even if one fails
**Verification**: Verify test file exists and compiles

## Phase 4: Verify Tests

**Goal**: Ensure generated tests pass

**Input**: Generated test files in `TESTS_DIR`
**Output**: Test results in `test_results.json`

**Steps**:
1. Run tests for each commit:

```markdown
<task>
Verify generated tests pass for each commit.

For each test in {TESTS_DIR}:
1. Checkout corresponding commit
2. Copy test file to correct location
3. Run test with flink-test-runner:
   ```bash
   mvn test -Dtest={TestClassName} -pl {module} -P java11-target -P java11
   ```
4. Record results in log/test-gen-{TIMESTAMP}/test_results.json

Output format:
```json
{
  "results": [
    {
      "commit": "abc123",
      "test_file": "FeatureTest.java",
      "status": "PASSED",
      "duration": "2.5s"
    }
  ]
}
```
</task>
```

**Gate**: Continue even if some tests fail (will be reported)
**Verification**: Verify test_results.json exists

## Phase 5: Commit Tests

**Goal**: Create test commits for passing tests

**Input**: `test_results.json`, test files in `TESTS_DIR`
**Output**: New test commits

**Steps**:
1. For each passing test:

```markdown
<task>
Commit the generated test for {commit_hash}.

Steps:
1. Checkout original commit:
```bash
cd {WORKTREE_PATH}
git checkout {commit_hash}
```

2. Copy test file from {TESTS_DIR}/{commit_hash}/{test_file} to proper location
3. Stage and commit:
```bash
git add {test_file}
git commit -m "[TEST] Add unit test for {original_commit_subject}

Generated test coverage for commit {commit_hash}
Original commit: {original_commit_subject}

Co-Authored-By: Claude Test Generator <noreply@anthropic.com>"
```

4. Record commit hash in results
</task>
```

**Gate**: Skip commits where tests failed
**Verification**: Verify new commits exist with `git log`

## Final Output

Generate summary report:

```markdown
<task>
Generate test generation summary report.

Create {REPORT_FILE} with:

# Test Generation Report

Generated: {TIMESTAMP}
Commit Range: {COMMIT_RANGE}

## Summary
- Total commits analyzed: X
- Commits needing tests: Y
- Tests generated: Z
- Tests passed: A
- Tests committed: B

## Details
[List each commit with status]

## Failed Tests
[List any test failures with reasons]

## Next Steps
- Cherry-pick test commits to your branch
- Review generated tests for completeness
</task>
```

## Error Handling

If any phase fails:

```markdown
<task>
Clean up resources after error:

1. Remove worktree if exists:
```bash
git worktree remove {WORKTREE_PATH} --force 2>/dev/null || true
```

2. Archive partial results to log directory
3. Generate error report with:
   - Failed phase
   - Error details
   - Partial results completed
   - Recovery suggestions
</task>
```

## Cleanup

Always execute cleanup:

```markdown
<task>
Final cleanup:

1. Remove worktree:
```bash
git worktree remove {WORKTREE_PATH} --force
```

2. Verify all results archived in log/test-gen-{TIMESTAMP}/
3. Output final report location
</task>
```

## Final Message

```
✅ Test generation complete!

Report: {REPORT_FILE}
Generated tests: {TESTS_DIR}/

To apply test commits:
git cherry-pick {list of commit hashes}
```