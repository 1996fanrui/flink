---
name: flink-skill-creator
description: Create or update a Claude skill in the current project. Use when users want to create a new skill, update an existing skill, or need guidance on skill structure and conventions. Triggers on requests like "create a skill", "new skill for X", "update skill Y".
model: opus
---

# Skill Creator

Create well-structured Claude skills following project conventions and best practices.

You are a **coordinator agent**. Delegate concrete work (file creation, validation) to sub agents via the Task tool.

## Input

```
/flink-skill-creator <skill-name-or-description>
/flink-skill-creator checkpoint-analyzer --update
```

Arguments:
- **Skill name or description**: A hyphen-case name or natural language description of the desired skill
- **`--update`**: Update an existing skill instead of creating a new one

## Step 1: Requirements Gathering

Use `AskUserQuestion` to understand:
1. What does the skill do? (concrete use cases, example invocations)
2. Is it a coordinator-style skill (multi-phase, delegates to sub agents) or a simple workflow skill?
3. What scripts/references/assets are needed?

Skip if the user already provided detailed requirements.

Conclude with:
- `SKILL_NAME`: Hyphen-case name (lowercase, digits, hyphens, max 64 chars)
- `SKILL_TYPE`: `coordinator` (multi-phase with sub agents) or `simple` (direct instructions)
- `RESOURCES`: List of needed resource types (`scripts`, `references`, `assets`)
- `SKILL_DIR`: `.claude/skills/{SKILL_NAME}`

## Step 2: Analyze Existing Conventions

Delegate to a sub agent:
1. Read `.claude/skills/*/SKILL.md` to extract project conventions
2. Read the best practices guide: `/Users/ruifan/code/github/env_tools/coding/_guides/skills_best_practices.md`
3. Read the skill-creator guide: `/Users/ruifan/code/github/env_tools/coding/generated/codex/skills/.system/flink-skill-creator/SKILL.md`
4. Summarize conventions found (frontmatter fields, phase structure, verification pattern)

Key project conventions (for reference, still verify from actual files):
- Frontmatter: `model: opus` + standard `name`/`description`
- Coordinator skills: phases with I/O, Steps, Gate, Verification
- Deterministic tasks → scripts; non-deterministic → agent instructions
- Verification by separate sub agent per phase
- `references/*.md` for detailed phase docs (>30 lines threshold)

## Step 3: Design Skill Structure

Based on requirements and conventions, design:
1. Phase breakdown (for coordinator skills) or section structure (for simple skills)
2. Scripts to create (deterministic tasks)
3. References to create (detailed docs per phase)
4. Directory layout

Present the design to the user via `AskUserQuestion` for confirmation before proceeding.

## Step 4: Create Skill

Delegate to sub agents (parallelize independent work):

### 4a: Create SKILL.md

Write `{SKILL_DIR}/SKILL.md` following these rules:

**Frontmatter**:
```yaml
---
name: {SKILL_NAME}
description: <what + when to use, max 1024 chars>
model: opus
---
```

**Body** (for coordinator skills):
- Opening: role declaration ("You are a **coordinator agent**...")
- Input: example invocations, argument definitions, global variables
- Phases: each with Goal, I/O, Steps, Gate, Verification
- Final Output: what the user sees
- Error Handling: recovery and cleanup

**Body** (for simple skills):
- Purpose and scope
- Input format
- Step-by-step workflow
- Output format

Keep body under 500 lines. Move phase details >30 lines to `references/`.

### 4b: Create Scripts

For each identified script:
1. Write the script in `{SKILL_DIR}/scripts/`
2. Add `set -e` for shell scripts
3. Add usage comment at top
4. Write corresponding tests in `{SKILL_DIR}/tests/` if the script has non-trivial logic

### 4c: Create References

For each phase that needs detailed docs:
1. Write `{SKILL_DIR}/references/<topic>.md`
2. Ensure SKILL.md references it with explicit "see `references/<topic>.md`"

## Step 5: Validate

Delegate to a sub agent:
1. Verify SKILL.md exists with valid YAML frontmatter (`name` + `description` required)
2. Verify `name` is lowercase, digits, hyphens only, max 64 chars
3. Verify `description` is max 1024 chars
4. Verify body is under 500 lines
5. Verify all `references/` files mentioned in SKILL.md actually exist
6. Verify all `scripts/` are executable and have usage comments
7. If coordinator skill: verify each phase has I/O, Steps, Gate, Verification
8. Run script tests if any exist

Report validation results. Fix issues and re-validate until all checks pass.

## Step 6: Summary

Output:
```
Skill created: {SKILL_DIR}
Structure:
  {file listing}

To use: /{SKILL_NAME} <args>
```

## Conventions Reference

When creating skills for this project, follow these conventions (detailed in `references/project_conventions.md`):

- **Architecture**: See CLAUDE.md "Skill 开发原则" section for coordinator pattern, verification, and resource management rules
- **Best Practices**: See `/Users/ruifan/code/github/env_tools/coding/_guides/skills_best_practices.md` for multi-agent, quality, and result closure principles
- **Skill-Creator Guide**: See `/Users/ruifan/code/github/env_tools/coding/generated/codex/skills/.system/flink-skill-creator/SKILL.md` for detailed anatomy and progressive disclosure

## Error Handling

- If validation fails, fix and re-validate (max 3 attempts)
- If user rejects design, return to Step 3 with feedback
- Never leave partially created skills — either complete or clean up
