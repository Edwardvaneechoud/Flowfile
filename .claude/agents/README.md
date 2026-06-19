# Flowfile development squad

A squad of Claude Code subagents — one specialist per major package — for
developing this repository. Each agent is scoped to its package, grounded in
that package's `CLAUDE.md`, and knows the package's conventions, test commands,
and the invariants it must not break. Delegate package work to the matching
specialist (the main session stays the coordinator).

| Agent | Package | Owns |
|-------|---------|------|
| `core-engineer` | `flowfile_core/` | FastAPI backend, DAG engine, auth, catalog, secrets, AI, kernels, migrations |
| `worker-engineer` | `flowfile_worker/` | Spawn-subprocess compute service, connectors, streaming |
| `frame-engineer` | `flowfile_frame/` | Polars-like Python API that builds in-process FlowGraphs (+ `.pyi` stub gate) |
| `frontend-engineer` | `flowfile_frontend/` | Tauri 2 shell + Vue 3 renderer (VueFlow designer, stores, desktop bridge) |
| `wasm-engineer` | `flowfile_wasm/` | Browser-only Pyodide editor (`flowfile-editor` npm package) |

These are definitions only — they ship no runtime code into the packages and
have no effect on the built application. The other packages (`flowfile_scheduler`,
`shared`, `kernel_runtime`) are covered by their own `CLAUDE.md`; add a
specialist here if the squad needs to grow.
