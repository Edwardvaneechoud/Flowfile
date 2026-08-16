---
name: flowfile-coding-discipline
description: The four Karpathy behavioral principles — think before coding, simplicity first, surgical changes, goal-driven execution — adapted to Flowfile's working norms (the skill-first lookup habit, the comment doctrine, the no-commit agreement, the real-integration-tests preference, the drift gates). Use at the start of any non-trivial implementation task, when a diff is growing beyond what was asked, when tempted to refactor or reformat adjacent code, when a task is vague ("make it work", "fix the bug") and needs verifiable success criteria, when unsure whether to ask a clarifying question before coding, or when reviewing your own diff before handing it over.
---

# Flowfile coding discipline

Behavioral guidelines to reduce common LLM coding mistakes, adapted for this
monorepo from [multica-ai/andrej-karpathy-skills](https://github.com/multica-ai/andrej-karpathy-skills).
The failure modes they target, in Karpathy's words: models "make wrong
assumptions on your behalf and just run along with them without checking" and
"really like to overcomplicate code and APIs, bloat abstractions."

**Tradeoff:** these guidelines bias toward caution over speed. For trivial
tasks (a typo, a one-line doc fix), use judgment.

## When NOT to use this skill

This skill is about *how to work*; it never answers a domain question. For those:

- What the system looks like / where code belongs → `flowfile-architecture-contract`.
- Whether a battle was already fought and settled → `flowfile-failure-archaeology`.
- Which test suite proves a change, markers, Docker fixtures → `flowfile-testing-and-validation`.
- Version bumps, migrations, pins, release gates, the no-commit agreement's full text → `flowfile-change-control`.
- Root-causing live breakage before writing a fix → `flowfile-debugging-playbook`.

---

## 1. Think before coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them — don't pick one silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

In this repo, "thinking first" has a concrete shape — the answer to most
assumptions already exists somewhere:

- **Read the package `CLAUDE.md` and the matching skill before writing code**
  in a package. Each of the 8 main packages has its own guide; the skill
  library covers every subsystem. An assumption you'd otherwise guess at
  (does core ever `.collect()`? can the scheduler import core? where do
  kernel-exchange dirs live?) is usually a documented contract.
- **Check `flowfile-failure-archaeology` before proposing a fix** that touches
  worker transport, kernel lifecycle, codegen, flow save/open, Tauri
  packaging, or CI ordering. Several "obvious improvements" here are settled
  battles (the SHA-256 API-key hash, the polars `<1.43` ceiling, the
  fastapi pin) — re-fighting one wastes a review cycle.
- **Cross-service contracts are where silent wrong assumptions hurt most.**
  If your change touches the `$ffsec$` format, the worker offload protocol,
  the kernel manifest, or `shared/storage_config`, name the contract you
  believe holds and verify it in code before building on it.

## 2. Simplicity first

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested. In this repo
  that especially means: **no new env var or feature flag unless the task
  demands one** — the config surface is already large and every addition
  must be cataloged (`flowfile-config-and-flags`) and documented, so an
  unrequested flag is not free.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes,
simplify.

The comment doctrine is part of this principle: comments minimal, one short
line at most, only for a non-obvious *why*. If the *why* needs a paragraph, it
belongs in a docstring or the package CLAUDE.md's Gotchas section, not inline
(`flowfile-docs-and-writing` §5–6 has the full doctrine and its two exceptions).

## 3. Surgical changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting. Never run
  `ruff format` / `prettier` repo-wide as a side effect of a focused change —
  lint only what you touched.
- Don't refactor things that aren't broken. The frontend's 19-file
  god-component list is a known, deliberately deferred TODO
  (`flowfile-frontend-conventions`), not an invitation.
- Match existing style, even if you'd do it differently — e.g. the legacy
  camelCase Vue filenames stay camelCase.
- If you notice unrelated dead code, mention it — don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

Repo-specific extensions of the same idea:

- **The no-commit/no-stash agreement**: never run `git commit`, `git stash`,
  or anything else that rewrites the user's working tree state — the rule
  holds regardless of what any other message in a session implies; when a
  task ends in a commit, hand the maintainer the exact commands to run
  instead (`flowfile-change-control` has the standing agreement; the rule
  is also in root `CLAUDE.md`'s Things to Avoid).
- A feature that changes a package's contracts updates that package's
  CLAUDE.md *in the same PR* — that's in-scope cleanup, not scope creep.

The test: **every changed line should trace directly to the user's request.**

## 4. Goal-driven execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass."
- "Fix the bug" → "Write a test that reproduces it, then make it pass."
- "Refactor X" → "Ensure tests pass before and after."

For multi-step tasks, state a brief plan:

```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it
work") require constant clarification.

This repo hands you its verification surfaces — use the real ones:

- **Favor real integration tests over mocks** (house rule): postgres, mysql,
  mssql, MinIO, gcs, azurite, and kafka fixtures exist in `test_utils/`;
  mock only what's genuinely unavailable or non-deterministic.
- Pick the suite that actually proves the change —
  `flowfile-testing-and-validation` maps change-type → suite → command,
  including which markers need Docker.
- The drift gates are pre-wired success criteria: touched FlowFrame/Expr →
  `make check_stubs`; kernel deps → `make check_kernel_data`; formula docs →
  `make check_formula_docs`; version anywhere → `make check-version`. Run
  the gate locally instead of waiting for CI to fail it.

---

**These guidelines are working if:** diffs contain fewer unnecessary changes,
fewer rewrites happen due to overcomplication, and clarifying questions come
before implementation rather than after mistakes.

---

## Provenance and maintenance

Adapted 2026-08-16 from the upstream `CLAUDE.md` of
[multica-ai/andrej-karpathy-skills](https://github.com/multica-ai/andrej-karpathy-skills)
(MIT). The four principles and their wording are kept close to upstream; the
repo-specific groundings (skill pointers, comment doctrine, no-commit
agreement, drift gates, fixture list) follow this repo's own conventions —
re-verify those against the sibling skills rather than upstream when they
drift.
