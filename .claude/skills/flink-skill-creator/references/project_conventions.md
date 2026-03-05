# Project Skill Conventions

Extracted from existing skills and CLAUDE.md. Follow these when creating new skills.

## Frontmatter

```yaml
---
name: skill-name
description: What the skill does and WHEN to use it (max 1024 chars)
model: opus
---
```

## Coordinator Skill Template

```markdown
# Skill Title

Brief description. When to use vs alternatives.

You are a **coordinator agent**. You NEVER execute commands directly.
All concrete work MUST be delegated to sub agents via the Task tool.

## Input

Example invocations with argument definitions.

Global variables:
- `TIMESTAMP`, `PROJECT_ROOT`, etc.

## Phase N: Name

**Goal**: One sentence.

**I/O**:
- Input: what this phase receives
- Output: what this phase produces

**Steps** (delegate to sub agent):
1. Concrete steps with exact commands

**Gate**: Pass/fail condition.

**Verification** (separate sub agent): Independent check of gate conditions.

## Final Output

What the user sees when complete.

## Error Handling

- Recovery strategy per phase
- Cleanup on failure
```

## Key Rules from CLAUDE.md

1. **Coordinator pattern**: Main agent is pure coordinator, never runs commands directly
2. **Sub agent isolation**: Each sub agent gets self-contained prompt with all context
3. **Independent verification**: Verification must be by a separate sub agent, not the executor
4. **Deterministic tasks → scripts**: Log parsing, data processing, file operations
5. **LLM tasks → agent instructions**: Root cause analysis, code review, natural language
6. **Scripts output JSON**: For consumption by subsequent phases
7. **Phase gates**: Current phase must pass verification before next phase starts
8. **Resource cleanup**: Even on failure, clean up worktrees, temp branches, temp files
9. **Error recovery**: Retry once before aborting; never silently degrade
10. **Archive results**: Persist outputs to `log/` directory in project root

## Directory Structure

```
.claude/skills/{skill-name}/
├── SKILL.md                    # Required: main skill file
├── scripts/                    # Deterministic tasks
│   └── *.sh / *.py
├── references/                 # Detailed docs for phases
│   └── *.md
├── tests/                      # Script tests
│   └── test_*.py
└── assets/                     # Templates, icons (rare)
```

## Best Practices Checklist

- [ ] SKILL.md body < 500 lines
- [ ] Phase details > 30 lines → move to `references/`
- [ ] Shell scripts have `set -e` and usage comment
- [ ] Scripts tested before delivery
- [ ] No README.md, CHANGELOG.md, or auxiliary docs
- [ ] All references mentioned in SKILL.md actually exist
- [ ] Description includes both WHAT and WHEN to use
