# CLAUDE.md - flowfile_wasm

Browser-only, Pyodide-powered build of the Flowfile visual designer: a Vue 3 + VueFlow editor where Polars runs entirely in WebAssembly, with no backend. Package-specific notes; see the root `/CLAUDE.md` for monorepo-wide setup, build, lint, ports, and cross-package contracts.

## Role
A fully self-contained, in-browser ETL editor. Unlike the desktop/web app, it does **not** talk to `flowfile_core`, `flowfile_worker`, or `kernel_runtime` — there are no axios/fetch backend calls. All execution happens client-side: a Pyodide runtime (CDN `v0.27.7`) loads `polars` + `pydantic` plus a Python execution engine that lives as a real package under `src/pyodide/engine/` (written into Pyodide's virtual filesystem at startup and imported with `from engine import *`).

Two consumption modes:
- **App** (`npm run dev` → :5174, `npm run build`): SPA mounted via `src/main.ts` + `src/router/index.ts` (routes `/` and `/embed-example`).
- **Library** (`npm run build:lib` → npm package `flowfile-editor`): entry `src/lib/index.ts`, exports the `FlowfileEditor` component + `FlowfileEditorPlugin`. Host apps embed it, pass `inputData`, drive it via a template-ref API (`executeFlow`, `executeNode`, `exportFlow`, `importFlow`, `setInputData`, `getNodeResult`, `clearFlow`, `initializePyodide`) and listen to `ready` / `output` / `execution-complete` / `error` events. Type/schema shapes in `src/types/index.ts` are *structurally* aligned with `flowfile_core` schemas for save-file compatibility only — no runtime coupling.

Execution contract: nodes are stored as Polars **LazyFrames** keyed by `node_id`; schema is read via `collect_schema()` without collecting; previews are materialized on demand and LRU-cached (`_preview_cache`). The orchestrator (`src/stores/flow-store.ts`) topologically sorts the DAG (`getExecutionOrder()`) and calls `execute_<type>(...)` Python functions through `pyodideStore.runPythonWithResult(...)`.

## Layout
- `src/stores/pyodide-store.ts` — Pyodide bootstrap; `import.meta.glob`s the `src/pyodide/engine/` package, writes it into Pyodide's FS, and runs `from engine import *`.
- `src/pyodide/engine/` — the Python execution engine as a real package: `state`/`dtypes`/`errors`/`preview`/`validation`, `nodes_*` executors, `schema_propagation`, with `__init__.py` re-exporting the public API. Ruff-linted and pytest-tested (`tests/python/`); the same files load in the browser and under pytest.
- `src/stores/flow-store.ts` — DAG state, `getExecutionOrder()` topological sort, `executeNode`/`executeFlow`, the JS↔Python `toPythonJson()` helper, IndexedDB persistence orchestration.
- `src/stores/file-storage.ts` — hybrid file persistence: sessionStorage (<5MB) + IndexedDB (≥5MB) for CSV inputs and output downloads.
- `src/stores/schema-inference.ts` — pure-TS output-schema inference (no Python) for live previews; returns `null` for `polars_code`/`formula`/`pivot`/joins-without-right-schema (can't infer → triggers lazy execution).
- `src/components/Canvas.vue` — VueFlow canvas + node palette (`nodeCategories` ref, 5 groups) and the `getSettingsComponent` type→panel map.
- `src/config/nodeDescriptions.ts` — node titles/intros for settings panels (16 keys).
- `src/config/polarsCompletions.ts` — CodeMirror Polars autocompletion for the Polars Code node.
- `src/components/nodes/` — per-node settings panels (`*Settings.vue`) + `FlowNode.vue`, including `exploreData/` (Graphic Walker).
- `src/lib/` — embeddable library surface: `index.ts` (exports), `FlowfileEditor.vue`, `plugin.ts`, `types.ts`.
- `src/composables/useCodeGeneration.ts` — generates standalone Python/Polars scripts from a flow (CodeGenerator modal).
- `src/utils/iconUrls.ts` — explicit icon imports (inlined as base64 in lib build).

## Key patterns & conventions
- **No `.collect()` unless required.** Nodes chain LazyFrames; only `output`, `pivot`, `explore_data`, and `polars_code` materialize. Mirror this when adding nodes.
- **Adding a node type** touches: `nodeCategories` + `getSettingsComponent` in `Canvas.vue`, `nodeDescriptions.ts`, a `*Settings.vue` panel, `NODE_TYPES`/types in `src/types/index.ts`, an `execute_<type>` Python fn in the matching `src/pyodide/engine/nodes_*.py` module (re-exported from `engine/__init__.py`'s `__all__`), a `case` in `executeNode` (`flow-store.ts`), and usually `useCodeGeneration.ts` + `schema-inference.ts`.
- **Pyodide is loaded once, lazily** from a CDN `<script>` tag injected in `initialize()`; `usePyodideStore.isReady` gates all execution. `numpy` is deliberately avoided — previews use native Polars `.rows()` to keep memory low.
- **Memory discipline:** `del` materialized DataFrames and call `gc.collect()` after heavy ops; honor `GW_MAX_ROWS` (100k) and pivot's `max_unique` (200) caps.
- **JS↔Python bridge:** large CSV/data content crosses via `setGlobal('_temp_content', …)` (read/manual_input/external_data); node settings always cross inline as `json.loads(${toPythonJson(node.settings)})`. Results return through `runPythonWithResult`, which deep-converts Maps → plain objects.
- **Library build:** `assetsInlineLimit: 100000` inlines icon PNGs as data URIs (avoids `import.meta.url` breakage in lib mode); `vue`/`pinia` are externalized (rollup `external`) and declared as peer deps (`pinia` optional).

## Running / entry points
```bash
cd flowfile_wasm
npm install
npm run dev        # app dev server → http://localhost:5174
npm run build      # app build (vue-tsc --noEmit && vite build)
npm run build:lib  # publishable flowfile-editor lib (BUILD_MODE=lib)
npm run build:all  # both
npm run preview    # preview built app
```

## Testing
Vitest, `happy-dom` env, config in `vitest.config.ts` (alias `@` → `src`, setup `tests/setup.ts`). IndexedDB is mocked via `fake-indexeddb`; sessionStorage/Blob mocked in setup. Tests live in `tests/` (`unit/`, `integration/`, `components/`).
```bash
npm run test          # watch
npm run test:run      # CI / one-shot
npm run test:coverage # v8 coverage
```
Note: the "integration" suite (`tests/integration/pyodide-execution.test.ts`) validates the *structure* of the embedded Python logic — it does **not** boot a real Pyodide runtime.

## Gotchas
- Pyodide needs `SharedArrayBuffer`, so the page must send COOP/COEP headers (`Cross-Origin-Opener-Policy: same-origin`, `Cross-Origin-Embedder-Policy: require-corp`). The dev server sets these in `vite.config.ts`; embedders must set them on their host page or Pyodide fails to load.
- Pyodide is pinned to **v0.27.7** in `pyodide-store.ts` — the last release with Polars support; bumping it likely breaks `loadPackage(['polars', 'pydantic'])`.
- Parquet output is unsupported in-browser — `execute_output` errors and asks for CSV (the standalone code generator *can* emit `sink_parquet`, which is separate).
- The Python engine is a real package under `src/pyodide/engine/`, written into Pyodide's virtual FS by `setupExecutionEngine()` and imported via `from engine import *` (so `flow-store.ts`'s bare-name `execute_*` calls resolve). It's ruff-linted and unit-tested by pytest (`tests/python/`, pinned to Pyodide's Polars 1.18.0). Any new public name must be re-exported in `engine/__init__.py`'s `__all__`, or the browser bridge / tests won't see it. The same files are the source of truth for both the browser and the tests.
- `optimizeDeps.exclude: ['pyodide']` and `include: ['react', 'react-dom/client', '@kanaries/graphic-walker']` are load-bearing (Graphic Walker is React, embedded inside Vue). `pyodide` is not an npm dependency at all — it's CDN-loaded.
- Node palette has **16 types across 5 categories**: Input Sources (Read CSV, Manual Input, External Data) · Transformations (Filter, Select, Sort, Polars Code, Unique, Take Sample) · Combine Operations (Join) · Aggregations (Group By, Pivot, Unpivot) · Output Operations (Explore Data, Write Data, External Output).
- `preview` is a **legacy** node type, migrated to `explore_data` on load (`flow-store.ts`). `PreviewSettings.vue` and an `execute_preview` Python fn still exist but are not in `NODE_TYPES`, the palette, the `getSettingsComponent` map, or the `executeNode` switch. The live sample/preview node is `head` (Take Sample).

## Key files
- `src/stores/pyodide-store.ts` — Pyodide init + the engine-package FS loader (`import.meta.glob` + `from engine import *`).
- `src/pyodide/engine/` — the Python execution engine package (executors, preview, schema propagation); `tests/python/` holds its pytest suite.
- `src/stores/flow-store.ts` — DAG store, topological execution, `executeNode`/`executeFlow`, `toPythonJson`.
- `src/components/Canvas.vue` — VueFlow canvas + node palette (`nodeCategories`) + settings-panel map.
- `src/lib/index.ts` — npm library public exports.
- `src/lib/FlowfileEditor.vue` — embeddable editor component + ref API/events.
- `src/lib/types.ts` — public props/API/event types for embedders.
- `src/types/index.ts` — node/flow data types + `NODE_TYPES` (structurally aligned with flowfile_core).
- `src/config/nodeDescriptions.ts` — settings-panel titles/intros per node.
- `src/composables/useCodeGeneration.ts` — flow → standalone Python/Polars script.
- `src/stores/file-storage.ts` — sessionStorage/IndexedDB hybrid file persistence.
- `vite.config.ts` — dual app/lib build config, dev COOP/COEP headers, :5174.
- `README.md` — published `flowfile-editor` consumer docs (props, events, API).
