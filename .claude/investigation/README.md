# Investigation archive — repo discovery sweep (2026-07-03)

Full-fidelity dossiers from the 12-agent discovery sweep that seeded the skill
library in `.claude/skills/`. Each was written by a read-only investigator that
verified commands/paths/claims against the repo at v0.12.7 (branch
`feature/claude-skills`, main at `f6963c77`). Facts here are point-in-time —
the skills carry the maintained versions; treat these as raw evidence and
re-verify before citing.

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
