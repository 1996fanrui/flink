---
model: opus
---

# Flink Test Analyzer

Automated Flink test execution with worktree isolation, iterative runs, and structured result analysis.

> **Note**: This skill is for automated loop testing (running tests N times to find flaky tests).
> For running targeted tests during TDD development to validate code changes, use the `flink-test-runner` agent instead.

You are a **coordinator agent**. You NEVER execute commands directly. All concrete work (shell commands, file operations, git operations) MUST be delegated to sub agents via the Task tool. Each sub agent receives a self-contained prompt with all necessary context.

## Input

The skill accepts arguments in one of these formats:

```
/flink-test-analyzer org.apache.flink.test.checkpointing.UnalignedCheckpointRescaleITCase -n 50
/flink-test-analyzer "run all checkpoint rescale tests" -n 10
/flink-test-analyzer org.apache.flink.TestA,org.apache.flink.TestB
```

Parse the arguments:
- **Test target**: Either fully qualified test class names (comma-separated) or a natural language description
- **`-n` iterations**: Number of test iterations (default: 100)

Detect whether the input is class names or natural language:
- If the input matches the pattern of Java fully qualified class names (contains dots, no spaces except around commas), treat it as explicit class names
- Otherwise, treat it as a natural language description and proceed with Phase 2

Record the following variables for use throughout the workflow:
- `TIMESTAMP`: Current time formatted as `YYYYMMDD_HHmmss`
- `WORKTREE_PATH`: `/tmp/claude-tmp/flink-test-{TIMESTAMP}`
- `PROJECT_ROOT`: The working directory when the skill was invoked (the Flink project root)
- `BRANCH_NAME`: Current git branch name
- `COMMIT_HASH`: Current git commit hash
- `TEST_CLASSES`: The resolved test class names (set after Phase 2 if needed)
- `ITERATIONS`: The iteration count from `-n` or default 100
- `LOG_DIR`: `{WORKTREE_PATH}/test-logs`
- `ARCHIVE_DIR`: `{PROJECT_ROOT}/log/flink-test-analyzer_{TIMESTAMP}`
- `USER_TEST_REQUEST`: The user's original test description or comma-separated class names (the raw input)

## Phase 1: Environment Preparation

Delegate to a sub agent with the following instructions:

1. Check for uncommitted changes in the current repository. If any exist, stage and commit them with a descriptive message: `[auto-commit] Save local changes before test run`
2. Record the current branch name and commit hash
3. Create a git worktree at `{WORKTREE_PATH}` based on the current branch:
   ```
   git worktree add {WORKTREE_PATH} -b test-run-{TIMESTAMP} HEAD
   ```
4. Verify the worktree directory exists and contains the project files

**Checklist before proceeding:**
- [ ] All local changes committed (or working tree was already clean)
- [ ] Branch name and commit hash recorded
- [ ] Worktree created at `{WORKTREE_PATH}`
- [ ] Worktree directory contains project files

**Verification**: Delegate to a SEPARATE verification sub agent:
1. Verify the worktree directory exists and contains expected project files (e.g., pom.xml)
2. Verify the branch name and commit hash are non-empty strings
3. If any check fails, report the failure and re-run the execution sub agent above
4. Only proceed to the next phase when all checks pass

## Phase 2: Test Discovery (conditional)

**Skip this phase entirely if the user provided explicit test class names.**

If the user provided a natural language description, delegate to a sub agent:

1. Search the Flink codebase for test classes matching the description. Use grep/find to locate test files:
   - Search in `**/src/test/java/` directories
   - Match class names, file names, and package paths against the description keywords
2. Present the discovered test classes to the user and ask for confirmation
3. Set `TEST_CLASSES` to the confirmed comma-separated list

**Checklist before proceeding:**
- [ ] Test class list confirmed (either from user input or discovery)

**Verification**: Delegate to a SEPARATE verification sub agent:
1. Verify that `TEST_CLASSES` is non-empty and contains valid Java class name patterns
2. If any check fails, report the failure and re-run the execution sub agent above
3. Only proceed to the next phase when all checks pass

## Phase 3: Project Build

Delegate to a sub agent with the following instructions:

1. Change to the worktree directory: `{WORKTREE_PATH}`
2. Run the build script:
   ```
   cd {WORKTREE_PATH} && zsh rui_tools/build_with_specific_version.sh
   ```
3. Check the exit code and scan output for `BUILD SUCCESS`
4. If the build fails:
   - Try a clean rebuild: run the same script again
   - If still failing, report the build error and **stop the entire workflow**

**Checklist before proceeding:**
- [ ] Build completed with `BUILD SUCCESS`
- [ ] No build errors

**Verification**: Delegate to a SEPARATE verification sub agent:
1. Verify the build output contains `BUILD SUCCESS`
2. Verify no compilation errors remain in the output
3. If any check fails, report the failure and re-run the execution sub agent above
4. Only proceed to the next phase when all checks pass

## Phase 4: Pre-execution Expectations

Delegate to a sub agent with the following instructions:

1. For each test class in `TEST_CLASSES`, find the source file in the worktree:
   ```
   find {WORKTREE_PATH} -path "*/src/test/java*" -name "{ClassName}.java"
   ```
2. Count @Test annotated methods:
   ```
   grep -c "@Test" {source_file}
   ```
3. Check for parameterized test annotations (@Parameterized, @MethodSource, @CsvSource, etc.)
4. Estimate the total number of test cases (methods x parameter combinations if parameterized)
5. Record the expectations as a summary:
   - List of test classes with expected test method count
   - Whether each class is parameterized
   - Estimated total test case count per iteration

Store the expectations output for use in Phase 6 verification.

**Checklist before proceeding:**
- [ ] All test class source files found
- [ ] Test method count recorded for each class
- [ ] Parameterization detected and documented
- [ ] Expected total test case count calculated

**Verification**: Delegate to a SEPARATE verification sub agent:
1. Verify that all test classes were found (source files exist)
2. Verify that the expected counts are reasonable (> 0)
3. If any check fails, report the failure and re-run the execution sub agent above
4. Only proceed to the next phase when all checks pass

## Phase 5: Test Execution

Delegate to a sub agent with the following instructions:

1. Create the log directory: `mkdir -p {LOG_DIR}`
2. Run the test script from the worktree:
   ```
   cd {WORKTREE_PATH} && bash .claude/skills/flink-test-analyzer/scripts/run_tests.sh \
     -t "{TEST_CLASSES}" \
     -n {ITERATIONS} \
     -d {LOG_DIR}
   ```
3. Wait for all iterations to complete (this can take a long time - use appropriate timeout)
4. Verify that log files were generated in `{LOG_DIR}`
5. Check for checkstyle errors in the output. If found:
   - Attempt to auto-fix checkstyle issues in the **main repo** (`{PROJECT_ROOT}`), not in the worktree
   - After the fix attempt, verify that the checkstyle error is actually resolved (e.g., re-run the checkstyle check or confirm the fix addresses the specific violation)
   - If the fix resolves the issue: commit the fix in the main repo and report it. Do NOT rebuild or re-run tests (checkstyle is skipped in test runs)
   - If the auto-fix fails (the checkstyle error persists after the fix attempt): **stop the entire workflow** and report the unresolved checkstyle error to the user. Do NOT continue with test execution

**Checklist before proceeding:**
- [ ] All iterations completed
- [ ] Log files exist in `{LOG_DIR}`

**Verification**: Delegate to a SEPARATE verification sub agent:
1. Verify that log files exist in `{LOG_DIR}` and are non-empty
2. Verify that the expected number of iteration log files were generated
3. If any check fails, report the failure and re-run the execution sub agent above
4. Only proceed to the next phase when all checks pass

## Phase 6: Result Analysis

Delegate to a sub agent with the following instructions:

1. Change to the worktree directory and run the analysis pipeline:

   ```bash
   cd {WORKTREE_PATH}

   # Step 1: Parse log files into structured JSON
   python .claude/skills/flink-test-analyzer/scripts/parse_logs.py {LOG_DIR}/*.log > {LOG_DIR}/parse_results.json

   # Step 2: Split failure logs into individual files
   python .claude/skills/flink-test-analyzer/scripts/split_failure_logs.py \
     --output-dir {LOG_DIR}/split_failures \
     {LOG_DIR}/*.log

   # Step 3: Generate the Markdown report
   cat {LOG_DIR}/parse_results.json | python .claude/skills/flink-test-analyzer/scripts/generate_report.py \
     --commit-hash {COMMIT_HASH} \
     --branch-name {BRANCH_NAME} \
     --test-request "{USER_TEST_REQUEST}" \
     --split-dir {LOG_DIR}/split_failures \
     --output-dir {LOG_DIR}
   ```

2. Verify all output files were created

**Checklist before proceeding:**
- [ ] `parse_results.json` generated
- [ ] `split_failures/` directory created (may be empty if no failures)
- [ ] `report.md` generated
- [ ] `failure_details.md` generated

**Verification**: Delegate to a SEPARATE verification sub agent:
1. Verify that `parse_results.json`, `report.md`, and `failure_details.md` exist in `{LOG_DIR}`
2. Compare actual test case counts from `parse_results.json` against expectations from Phase 4
3. Verify `report.md` and `failure_details.md` were generated and are non-empty
4. If any check fails, report the failure and re-run the execution sub agent above
5. Only proceed to the next phase when all checks pass

## Phase 7: Archive

Delegate to a sub agent with the following instructions:

1. Create the archive directory: `mkdir -p {ARCHIVE_DIR}`
2. Copy the following to `{ARCHIVE_DIR}`:
   - All log files that contain failures (use grep to identify logs with `failed with:` marker):
     ```
     grep -l "failed with:" {LOG_DIR}/*.log | xargs -I{} cp {} {ARCHIVE_DIR}/
     ```
     If no failures, skip this step.
   - The `split_failures/` directory:
     ```
     cp -r {LOG_DIR}/split_failures {ARCHIVE_DIR}/ 2>/dev/null || true
     ```
   - The report:
     ```
     cp {LOG_DIR}/report.md {ARCHIVE_DIR}/
     ```
   - The failure details:
     ```
     cp {LOG_DIR}/failure_details.md {ARCHIVE_DIR}/
     ```
   - Update split log paths in the archived report to reference the archive location:
     ```
     sed -i '' "s|{LOG_DIR}/split_failures|{ARCHIVE_DIR}/split_failures|g" {ARCHIVE_DIR}/report.md
     ```
   - Update split log paths in the archived failure details to reference the archive location:
     ```
     sed -i '' "s|{LOG_DIR}/split_failures|{ARCHIVE_DIR}/split_failures|g" {ARCHIVE_DIR}/failure_details.md
     ```

**Checklist before proceeding:**
- [ ] Archive directory exists at `{ARCHIVE_DIR}`
- [ ] `report.md` copied
- [ ] `failure_details.md` copied
- [ ] Failure logs and split failures copied (if any)

**Verification**: Delegate to a SEPARATE verification sub agent:
1. Verify the archive directory exists and contains `report.md` and `failure_details.md`
2. Verify that paths inside the archived files reference `{ARCHIVE_DIR}` instead of `{LOG_DIR}`
3. If any check fails, report the failure and re-run the execution sub agent above
4. Only proceed to the next phase when all checks pass

## Phase 8: Cleanup

Delegate to a sub agent with the following instructions:

1. Remove the worktree:
   ```
   cd {PROJECT_ROOT} && git worktree remove --force {WORKTREE_PATH}
   ```
2. Clean up the temporary branch:
   ```
   git branch -D test-run-{TIMESTAMP} 2>/dev/null || true
   ```
3. Verify the worktree directory no longer exists
4. Print the archive path: `{ARCHIVE_DIR}`

**Checklist before proceeding:**
- [ ] Worktree removed
- [ ] Temporary branch cleaned up
- [ ] Archive path printed

**Verification**: Delegate to a SEPARATE verification sub agent:
1. Verify the worktree directory no longer exists
2. Verify the temporary branch has been deleted
3. If any check fails, report the failure and re-run the execution sub agent above
4. Only proceed to the next phase when all checks pass

## Final Output

After all phases complete, output the contents of `{ARCHIVE_DIR}/report.md` to the user, followed by:

```
Archive location: {ARCHIVE_DIR}
```

If there are failures, also mention:

```
Failure details: {ARCHIVE_DIR}/failure_details.md
```

## Error Handling

- If any phase fails, attempt recovery once before aborting
- If recovery fails, ensure cleanup still runs (Phase 8) to avoid leaving orphaned worktrees
- Always print which phase failed and the error details
- The worktree MUST be cleaned up even on failure
