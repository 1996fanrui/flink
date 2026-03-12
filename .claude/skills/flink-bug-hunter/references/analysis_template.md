# Root Cause Analysis Document Template

## Structure

```markdown
# <Issue Title> - Root Cause Analysis

Log files: `<LOG_DIR path>`
Investigation date: <date>
Iterations: <N>

## Issue Description

<What was observed, expected vs actual behavior>

## Root Cause

<Specific code location + condition causing the issue>

**Code location**: `ClassName.java` method `methodName()` line ~N:
\```java
<the problematic code snippet>
\```

**What happens**:
1. <step 1>
2. <step 2>
...

## Evidence Chain

Chronological log lines proving the cause:

| Time | Event | Log Line |
|------|-------|----------|
| HH:MM:SS.sss | <event description> | `<exact log line>` |
| ... | ... | ... |

## Contrast Analysis

Show the difference between success and failure cases:

**Success case** (normal behavior):
\```
<log lines showing normal path>
\```

**Failure case** (buggy behavior):
\```
<log lines showing buggy path>
\```

## Fix Direction

<How to fix, if clear from the analysis>
```

## Guidelines

- Every claim must reference a specific log line with timestamp
- Include the CONTRAST between working and broken cases — this is the most convincing evidence
- Timeline table should be chronological and show cause-effect relationships
- Keep the document concise — focus on the evidence chain, not general explanations
