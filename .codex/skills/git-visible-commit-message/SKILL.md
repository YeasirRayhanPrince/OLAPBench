---
name: git-visible-commit-message
description: Generate a commit message for the currently visible uncommitted changes since HEAD. Use when the user asks for a commit message, git commit summary, or subject/body text based on the current diff, staged changes, and untracked files.
---

# Git Visible Commit Message

Generate a commit message from the repo's current uncommitted state relative to `HEAD`.

## When to use

Use this skill when the user wants:
- a commit message for current local changes
- a subject/body summary for staged or unstaged work
- a commit summary based on what is visible in `git status` and `git diff`

Do not use it for:
- summarizing historical commits that are already committed
- writing PR titles/descriptions for a remote branch unless the user explicitly asks for that

## Workflow

1. Inspect current local changes with:
   - `git status --short`
   - `git diff --stat HEAD`
   - `git diff --cached --stat HEAD`
2. If needed, inspect targeted diffs for the files carrying the main behavior change.
3. Infer the dominant change group from the visible diff, not from conversation wording alone.
4. Write:
   - one concise subject line
   - optional body bullets only if there are 2-4 distinct meaningful changes

## Message rules

- Prefer imperative mood.
- Keep the subject under about 72 characters when possible.
- Use conventional-commit style only if the repo/user already uses it or the existing history suggests it.
- Mention the main user-facing or behavior-changing outcome, not low-level edit noise.
- If the visible changes span multiple unrelated areas, say that explicitly and offer 2-3 commit message options grouped by likely intent.

## Heuristics

- If most files support one feature, name the feature directly.
- If tests are added for the same feature, include that only in the body, not the subject unless testing is the main change.
- If the diff mixes implementation and shell/tooling updates for the same feature, summarize them as one change.
- If untracked files define the main feature, include them in the summary.

## Output format

Default:

```text
<subject>
```

If a body is useful:

```text
<subject>

- bullet 1
- bullet 2
```

## Suggested commands

Use these first:

```bash
git status --short
git diff --stat HEAD
git diff --cached --stat HEAD
```

Use file-level diffs only when the summary is ambiguous.
