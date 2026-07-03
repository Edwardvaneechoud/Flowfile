# Investigation archive — repo discovery sweep (2026-07-03)

> **STATUS: FROZEN EVIDENCE. Deliberately not maintained. Expected to drift.**

These are the full-fidelity dossiers from the 12-agent discovery sweep that
seeded the skill library in `.claude/skills/`. Each was written by a read-only
investigator that verified commands/paths/claims against the repo at **v0.12.7,
commit `f6963c77`** (the parent of the commit that archived them). They are a
**point-in-time snapshot**, not a living document.

## The contract (this is the intended long-term behavior, not an accident)

This archive is kept **frozen on purpose**. It will not be refreshed as the code
moves on, and that is the design — so nobody has to carry a second maintenance
burden and no reader mistakes a snapshot for current truth.

- **The living layer is `.claude/skills/`.** Those skills are maintained, carry
  their own "Provenance and maintenance" re-verification commands, and are what
  you load to do work. This archive exists only as the *evidence trail* behind
  them: use it to audit *why* a skill claims something, or to recover context a
  skill compressed away — never as an instruction source.
- **Authority order, highest first:**
  1. **The live repository** — always wins. Read the actual file.
  2. **`.claude/skills/`** — the maintained interpretation of the repo.
  3. **These dossiers** — *leads only*. A statement here is a hypothesis to
     re-verify against (1), never a fact to cite. Assume every file:line,
     count, and version has drifted until you confirm it.

## Measure the drift before trusting anything here

Run this to see how far the repo has moved since the snapshot. The larger the
number, the more suspect these files are:

```bash
# From the repo root. f6963c77 is the state these dossiers describe.
git log --oneline f6963c77..HEAD | wc -l                 # commits since snapshot
git log --oneline f6963c77..HEAD                         # what changed, newest first
git diff --stat f6963c77..HEAD -- flowfile_core/         # churn in a specific area
```

If that count is large or a dossier's area shows heavy churn, treat the dossier
as historical background and re-derive from the live repo (the matching skill's
Provenance section has the exact re-verification commands).

## When to reach for this archive vs. delete it

- **Keep using it** to understand a past incident, recover a wider evidence base
  than a skill carries, or check whether a skill faithfully reflected its source.
- **Do not** update a dossier to "keep it current" — that reintroduces the
  double-maintenance the freeze exists to avoid. If a fact here is wrong, fix the
  **skill** (the living layer), not the dossier.
- If this archive ever stops earning its keep as an audit trail, delete the whole
  `.claude/investigation/` directory — the skills are self-contained without it.

## Dossier index

| File | Dimension |
|------|-----------|
| `build-env.md` | Build system, from-scratch env recipes, version pins, PyInstaller/sidecar/signing |
| `ci-release.md` | All CI workflows, release process, branch-protection reality, tag forensics |
| `config-flags.md` | Every env var / feature flag read in code, defaults, drift report |
| `core-architecture.md` | flow_graph engine, node lifecycle, execution model, worker offload seam |
| `docs-manifests.md` | Docs inventory, house style, CLAUDE.md quality audit, doc-vs-code drift |
| `frame-api.md` | flowfile_frame mental model, emission paths, expr system, stub pipeline |
| `frontend-tauri-wasm.md` | Renderer/store/node-UI conventions, Tauri shell, WASM constraints |
| `git-archaeology.md` | 25 reconstructed incidents, stalled branches, removed features, timeline |
| `run-operate-data.md` | CLI verbs, headless runs, filesystem map, logs, state-inspection runbook |
| `subsystems-contracts.md` | AI, secrets, sharing, catalog, scheduler, kernel, migrations contracts |
| `test-infra.md` | Test suites/markers/fixtures, isolation model, evidence bar |
| `todos-debt.md` | TODO/FIXME/xfail inventory, ranked live pain, by-design boundaries |
