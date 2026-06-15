# CLAUDE.md - flowfile_wasm

Browser-only, Pyodide-powered build of the Flowfile visual designer: a Vue 3 + VueFlow editor where Polars runs entirely in WebAssembly, with no backend. Package-specific notes; see the root `/CLAUDE.md` for monorepo-wide setup, build, lint, ports, and cross-package contracts.

## Role
A fully self-contained, in-browser ETL editor. Unlike the desktop/web app, it does **not** talk to `flowfile_core`, `flowfile_worker`, or `kernel_runtime` — there are no axios/fetch backend calls. All execution happens client-side: a Pyodide runtime (CDN `v0.27.7`) loads `polars` + `pydantic` plus a Python execution engine that lives as a real package under `src/pyodide/engine/` (written into Pyodide's virtual filesystem at startup, then re-exposed as a flat global namespace so the `flow-store.ts` bridge's bare-name calls resolve).

Two consumption modes:
- **App** (`npm run dev` → :5174, `npm run build`): SPA mounted via `src/main.ts` + `src/router/index.ts` (routes `/` and `/embed-example`).
- **Library** (`npm run build:lib` → npm package `flowfile-editor`): entry `src/lib/index.ts`, exports the `FlowfileEditor` component + `FlowfileEditorPlugin`. Host apps embed it, pass `inputData`, drive it via a template-ref API (`executeFlow`, `executeNode`, `exportFlow`, `importFlow`, `setInputData`, `getNodeResult`, `clearFlow`, `initializePyodide`) and listen to `ready` / `output` / `execution-complete` / `error` events. Type/schema shapes in `src/types/index.ts` are *structurally* aligned with `flowfile_core` schemas for save-file compatibility only — no runtime coupling.

Execution contract: nodes are stored as Polars **LazyFrames** keyed by `node_id`; schema is read via `collect_schema()` without collecting; previews are materialized on demand and LRU-cached (`_preview_cache`). The orchestrator (`src/stores/flow-store.ts`) topologically sorts the DAG (`getExecutionOrder()`) and calls `execute_<type>(...)` Python functions through `pyodideStore.runPythonWithResult(...)`.

## Layout
- `src/stores/pyodide-store.ts` — Pyodide bootstrap; `import.meta.glob`s the `src/pyodide/engine/` package, writes it into Pyodide's FS, imports it, and dumps its full namespace into Pyodide globals.
- `src/pyodide/engine/` — the Python execution engine as a real package: `state`/`dtypes`/`errors`/`preview`/`validation`, `nodes_*` executors, `schema_propagation`, with `__init__.py` re-exporting the public API. Ruff-linted and pytest-tested (`tests/python/`); the same files load in the browser and under pytest.
- `src/stores/flow-store.ts` — DAG state, `getExecutionOrder()` topological sort, `executeNode`/`executeFlow`, the JS↔Python `toPythonJson()` helper, IndexedDB persistence orchestration.
- `src/stores/file-storage.ts` — hybrid file persistence: sessionStorage (<5MB) + IndexedDB (≥5MB) for CSV inputs and output downloads.
- `src/stores/schema-inference.ts` — pure-TS output-schema inference (no Python) for live previews; returns `null` for `polars_code`/`formula`/`pivot`/joins-without-right-schema (can't infer → triggers lazy execution).
- `src/components/Canvas.vue` — VueFlow canvas + node palette (`nodeCategories` ref, 5 groups) and the `getSettingsComponent` type→panel map.
- `src/config/nodeDescriptions.ts` — node titles/intros for settings panels (23 keys).
- `src/config/polarsCompletions.ts` — CodeMirror Polars autocompletion for the Polars Code node.
- `src/components/nodes/` — per-node settings panels (`*Settings.vue`) + `FlowNode.vue`, including `exploreData/` (Graphic Walker).
- `src/lib/` — embeddable library surface: `index.ts` (exports), `FlowfileEditor.vue`, `plugin.ts`, `types.ts`.
- `src/composables/useCodeGeneration.ts` — generates standalone Python/Polars scripts from a flow (CodeGenerator modal).
- `src/utils/iconUrls.ts` — explicit icon imports (inlined as base64 in lib build).

## Key patterns & conventions
- **Execution is explicit-only — data runs ONLY when the user clicks Run.** Node execution (`executeNode` / `executeNodeWithUpstream` / `executeFlow` in `flow-store.ts`) may be triggered **only** by deliberate Run actions: **Run flow** (toolbar / Ctrl+E / lib `run` API), **Run Now** (node context menu), **Apply** (settings drawer), and **Fetch data** (the Table-preview button). Selecting a node, opening the Settings or Table panel, single-/double-click, drop, and paste must **never** call an execution path — opening the Table shows existing rows or a placeholder, not a fresh run. `fetchNodePreview` is **preview-only**: it materializes already-computed data, stays gated on `result.success`, and must never reach an `execute_*` bridge. Regression guard: `tests/unit/no-auto-run.test.ts`.
- **No `.collect()` unless required.** Nodes chain LazyFrames; only `output`, `pivot`, `explore_data`, and `polars_code` materialize. Mirror this when adding nodes.
- **Adding a node type** touches: `nodeCategories` + `getSettingsComponent` in `Canvas.vue`, `nodeDescriptions.ts`, a `*Settings.vue` panel, `NODE_TYPES`/types in `src/types/index.ts`, an `execute_<type>` Python fn in the matching `src/pyodide/engine/nodes_*.py` module (re-exported from `engine/__init__.py`'s `__all__`), a `case` in `executeNode` (`flow-store.ts`), and usually `useCodeGeneration.ts` + `schema-inference.ts`.
- **Pyodide is loaded once, lazily** from a CDN `<script>` tag injected in `initialize()`; `usePyodideStore.isReady` gates all execution. `numpy` is deliberately avoided — previews use native Polars `.rows()` to keep memory low.
- **Memory discipline:** `del` materialized DataFrames and call `gc.collect()` after heavy ops; honor `GW_MAX_ROWS` (100k) and pivot's `max_unique` (200) caps.
- **JS↔Python bridge:** large CSV/text content crosses via `setGlobal('_temp_content', …)`; binary (xlsx/parquet-IPC/Arrow) crosses via `setGlobal('_temp_bytes', Uint8Array)` with `.to_py()` **in the bridge string** (engine signatures take plain bytes-like, so pytest passes `bytes`). Node settings always cross inline as `json.loads(${toPythonJson(node.settings)})`. Results return through `runPythonWithResult` (deep-converts Maps → plain objects) — **except bytes**, which don't survive `toJs()`: the engine stages them in `state._output_binaries` and JS pulls once via `runPythonGetBytes('take_output_binary(id)')` (`PyProxy.getBuffer('u8')`).
- **File content is a tagged union** (`src/types/file-content.ts`): `{kind:'text',data:string} | {kind:'binary',data:Uint8Array,format:'excel'|'parquet'|'arrow-ipc'}`. Binary wrappers are `markRaw`'d, always persist to IndexedDB (structured clone, no base64), never enter sessionStorage JSON / saved-flows entries / the persisted clipboard, and restore via `largeFileNodeIds` + re-pick.
- **Heavy deps load lazily on first use, never at boot:** Excel = micropip (`ensurePyPackages(['openpyxl==3.1.5'])` / `XlsxWriter==3.2.0` — pins must match `tests/python/requirements.txt`); Parquet = `parquet-wasm@0.7.1` esm from jsdelivr via a **bundler-opaque** dynamic import (`src/utils/parquet-bridge.ts`; a literal https: `import()` in the published lib breaks webpack5/esbuild embedders — guarded by `tests/unit/parquet-bridge.test.ts`).
- **Parquet never touches Python as parquet:** the wasm polars wheel has parquet compiled out but IPC kept. JS (parquet-wasm) converts Parquet ⇄ Arrow IPC stream bytes; the engine only runs `pl.read_ipc_stream` / `write_ipc_stream(compat_level=oldest)`. `_clean_strings_for_export` (nodes_io.py) rebuilds String/Binary columns before IPC export — the wasm build panics ("capacity overflow") converting view-type columns whose buffers came through IPC import or the excel reader.
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
- The Read File node handles CSV, Excel (.xlsx via openpyxl, explicit `engine=` — calamine/fastexcel has no wasm wheel; polars 1.18 `read_excel` has **no row-offset kwarg**, `start_row` is a `.slice()` in the engine) and Parquet (JS-decoded to IPC), plus remote URLs (plain CORS; COEP does not block `fetch`). Output writes CSV, Excel (xlsxwriter → `take_output_binary`) and Parquet (IPC staging → `ipcStreamToParquet`). `file_type:'json'` from core-saved flows fails with a clear message. The catalog stays CSV-only.
- The Python engine is a real package under `src/pyodide/engine/`, written into Pyodide's virtual FS by `setupExecutionEngine()`. **Flat-namespace contract:** `flow-store.ts`'s bridge runs bare Python that references not just the public `execute_*` functions but also engine **internals** (e.g. `_lazyframes`) and the modules the engine imports (e.g. `gc`). So the bootstrap doesn't just `from engine import *` — it **dumps the package's entire namespace** (every submodule's non-dunder names) into Pyodide globals, reproducing the original single-module `exec`-into-globals behavior. Consequence: CPython unit tests (`tests/python/`, pinned to Pyodide's Polars 1.18.0) verify engine *logic* but **cannot** catch a broken browser namespace — the real-Pyodide smoke test (`tests/pyodide-smoke/smoke.cjs`, CI job `pyodide-smoke`) replays the bridge against actual Pyodide and is the guard for that. The same files are the source of truth for the browser, pytest, and the smoke test.
- `optimizeDeps.exclude: ['pyodide']` and `include: ['react', 'react-dom/client', '@kanaries/graphic-walker']` are load-bearing (Graphic Walker is React, embedded inside Vue). `pyodide` is not an npm dependency at all — it's CDN-loaded.
- Node palette has **23 types across 5 categories** (21 + the 2 catalog nodes): Input Sources (Read File, Manual Input, External Data, Read from Catalog) · Transformations (Filter, Select, Formula, Sort, Polars Code, Unique, Rename/`dynamic_rename`, Record ID, Take Sample) · Combine Operations (Join, Cross Join, Union) · Aggregations (Group By, Pivot, Unpivot) · Output Operations (Explore Data, Write Data, Write to Catalog, External Output). Hosts can filter these via the `nodeCategories` prop, so marketing copy says "20+". The library exposes an Arrow host contract: `inputData` accepts `Uint8Array` (Arrow IPC / Parquet, explicit `format` preferred over the PAR1 sniff) and `getNodeResultArrow(nodeId)` pulls a node's frame as IPC bytes via `get_node_arrow`.
- `preview` is a **legacy** node type, migrated to `explore_data` on load (`flow-store.ts`). `PreviewSettings.vue` and an `execute_preview` Python fn still exist but are not in `NODE_TYPES`, the palette, the `getSettingsComponent` map, or the `executeNode` switch. The live sample/preview node is `head` (Take Sample).

## Key files
- `src/stores/pyodide-store.ts` — Pyodide init + the engine-package FS loader (`import.meta.glob` + namespace dump into globals).
- `src/pyodide/engine/` — the Python execution engine package (executors, preview, schema propagation); `tests/python/` holds its pytest suite and `tests/pyodide-smoke/` the real-Pyodide bridge guard.
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
