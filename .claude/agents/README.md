# Flowfile development squad

A squad of Claude Code subagents for developing this repository. Each agent is
scoped to its area, grounded in the relevant `CLAUDE.md`, and knows the
conventions, test commands, and invariants it must not break. Delegate work to
the matching specialist (the main session stays the coordinator).

## Package specialists — one per package

| Agent | Package | Owns |
|-------|---------|------|
| `core-engineer` | `flowfile_core/` | FastAPI backend, DAG engine, auth, catalog, secrets, AI, kernels, migrations |
| `worker-engineer` | `flowfile_worker/` | Spawn-subprocess compute service, connectors, streaming |
| `frame-engineer` | `flowfile_frame/` | Polars-like Python API that builds in-process FlowGraphs (+ `.pyi` stub gate) |
| `frontend-engineer` | `flowfile_frontend/` | Tauri 2 shell + Vue 3 renderer (VueFlow designer, stores, desktop bridge) |
| `wasm-engineer` | `flowfile_wasm/` | Browser-only Pyodide editor (`flowfile-editor` npm package) |
| `scheduler-engineer` | `flowfile_scheduler/` | Embeddable cron/interval/table-trigger polling engine (core→scheduler only) |
| `shared-engineer` | `shared/` | Bottom-of-the-graph utils: storage paths, wire/DB models, cloud/Kafka/ML helpers |
| `kernel-engineer` | `kernel_runtime/` | Sandboxed Docker code-exec kernel, `flowfile_ctx` API, artifact persistence |

## Cross-cutting specialists — span packages

| Agent | Area | Owns |
|-------|------|------|
| `docs-writer` | Documentation | MkDocs site under `docs/`, `mkdocs.yml`, `CLAUDE.md` guides, frame API docstrings |
| `release-engineer` | Build / CI / release | Makefile, PyInstaller + Tauri bundling, `.github/workflows/*`, Docker images, version pins |

These are definitions only — they ship no runtime code into the packages and
have no effect on the built application. Add a new member here (and to the table
above) when the squad needs to grow.
