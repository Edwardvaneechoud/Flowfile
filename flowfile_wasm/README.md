<h1 align="center">
  <img src="https://raw.githubusercontent.com/edwardvaneechoud/Flowfile/main/.github/images/logo.png" alt="Flowfile logo" width="100">
  <br>
  flowfile-editor
</h1>

<p align="center">
  <b>The Flowfile canvas, running entirely in the browser.</b>
  <br>
  <sub>A visual Polars pipeline editor as a single Vue component.<br>No backend, no server round-trips. Data never leaves the page.</sub>
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/flowfile-editor"><img src="https://img.shields.io/npm/v/flowfile-editor?style=flat-square&logo=npm&logoColor=white" alt="npm version"></a>
  <a href="https://github.com/edwardvaneechoud/Flowfile/actions/workflows/flowfile-wasm-build.yml"><img src="https://img.shields.io/github/actions/workflow/status/edwardvaneechoud/Flowfile/flowfile-wasm-build.yml?branch=main&style=flat-square&logo=github&label=build" alt="Build status"></a>
  <a href="https://github.com/edwardvaneechoud/Flowfile/blob/main/LICENSE"><img src="https://img.shields.io/github/license/edwardvaneechoud/Flowfile?style=flat-square" alt="License"></a>
  <a href="https://github.com/edwardvaneechoud/Flowfile/stargazers"><img src="https://img.shields.io/github/stars/edwardvaneechoud/Flowfile?style=flat-square&logo=github" alt="GitHub stars"></a>
</p>

<p align="center">
  <a href="https://demo.flowfile.org"><b>▶&nbsp;&nbsp;Try it in your browser&nbsp;&nbsp;→</b></a>
  <br>
  <sub>No install. No signup. Polars in the browser via Pyodide.</sub>
</p>

<p align="center">
  <a href="https://github.com/edwardvaneechoud/Flowfile">Flowfile</a> ·
  <a href="https://edwardvaneechoud.github.io/Flowfile/">Docs</a> ·
  <a href="https://www.npmjs.com/package/flowfile-editor">npm</a> ·
  <a href="https://github.com/edwardvaneechoud/Flowfile/discussions">Discussions</a>
</p>

---

`flowfile-editor` is the browser-native core of [Flowfile](https://github.com/edwardvaneechoud/Flowfile), a visual ETL tool that compiles to Polars. This package takes the same canvas and runs it on [Pyodide](https://pyodide.org/): [Polars](https://pola.rs/) executes in WebAssembly, so every filter, join and group-by runs client-side. Drop it into a Vue app as `<FlowfileEditor />`, feed it data, and read results back as Arrow.

- Want to see it first? [demo.flowfile.org](https://demo.flowfile.org) is this package, deployed as is.
- Embedding it? `npm install flowfile-editor`, then the [Quick Start](#quick-start) below.
- Need databases, cloud storage, a catalog, a scheduler, or an AI assistant? That is the full platform: `pip install Flowfile`. Flows built here open there.

<div align="center">
  <img src="https://raw.githubusercontent.com/edwardvaneechoud/Flowfile/main/.github/images/flowfile_wasm_editor.png" alt="The flowfile-editor canvas: two file readers joined, a formula node converting order_date, with the formula settings panel open" width="800"/>
  <br>
  <sub>Two readers, a join and a formula, with the formula editor open. Everything on screen ran in the browser tab.</sub>
</div>

&nbsp;

---

## What you get

**Real Polars, not a toy.** A lazy DAG with 23 node types: file readers for CSV, Excel and Parquet, filters, selects, formulas, sorts, joins, cross joins, unions, group-bys, pivots, unpivots, dynamic renames, record ids, sampling, and a raw Polars code node for anything the palette does not cover. The formula editor is the same one Flowfile ships, with field and function autocompletion.

**One component to embed.** `<FlowfileEditor />` with a template-ref API (`executeFlow`, `setInputData`, `getNodeResult`, and friends) and events for `ready`, `output`, `execution-complete` and `error`. Hide toolbar buttons and node categories to fit your product.

**Zero backend.** Nothing to host and nothing to secure. The Pyodide runtime loads from a CDN, and the user's data stays on their machine.

**Arrow-native I/O.** Push frames in as Arrow IPC or Parquet bytes and pull results back the same way. Pairs cleanly with duckdb-wasm and arrow-js.

**Code, not just clicks.** The Code panel shows the flow as a standalone Polars script. With `teachingMode` on, users can also opt into a plain-Python walkthrough and a per-node "How would I write this myself?" explainer.

**Part of Flowfile.** Flows download as `.flowfile` files that open in the desktop app and the Docker deployment, and the full platform can mint share links that open here. Nodes that only exist in the full platform show up in the palette as greyed-out teasers linking to the docs.

---

## Install

```bash
npm install flowfile-editor
```

**Peer dependencies:** Vue 3.3+ is required. Pinia 2.0+ is optional; the editor creates its own instance when none is provided.

## Quick Start

```vue
<script setup>
import { ref } from 'vue'
import { FlowfileEditor } from 'flowfile-editor'
import 'flowfile-editor/style.css'

const editorRef = ref()
</script>

<template>
  <FlowfileEditor
    ref="editorRef"
    height="600px"
    @ready="console.log('Pyodide loaded')"
    @output="data => console.log('Output:', data)"
  />
</template>
```

Pyodide needs `SharedArrayBuffer`, so the page hosting the editor must be cross-origin isolated. See [CORS headers](#cors-headers) below before deploying.

### Plugin registration

If you prefer global registration:

```ts
import { createApp } from 'vue'
import { FlowfileEditorPlugin } from 'flowfile-editor'
import 'flowfile-editor/style.css'

const app = createApp(App)
app.use(FlowfileEditorPlugin)
// <FlowfileEditor /> is now available in every template
```

---

## Props

| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `height` | `string` | `'100%'` | CSS height of the editor |
| `width` | `string` | `'100%'` | CSS width of the editor |
| `readonly` | `boolean` | `false` | Disable editing |
| `initialFlow` | `FlowfileData` | — | Pre-load a saved flow |
| `inputData` | `InputDataMap` | — | Provide named datasets for External Data nodes |
| `theme` | `ThemeConfig` | — | `{ mode: 'light' \| 'dark' \| 'system' }` |
| `toolbar` | `ToolbarConfig` | — | Show/hide toolbar buttons |
| `nodeCategories` | `NodeCategoryConfig[]` | — | Control which node types are available |
| `teachingMode` | `boolean` | `true` | Offer the learning surfaces: the Python walkthrough tab in the Code panel and the "How would I write this myself?" panel in node settings. They stay hidden until the user opts in via the graduation-cap button in the Code panel header, so `true` only makes the opt-in available. Set `false` to remove the capability entirely |
| `pyodide` | `PyodideConfig` | — | `{ autoInit: boolean }` |

## Events

| Event | Payload | Description |
|-------|---------|-------------|
| `ready` | — | Pyodide is initialized and ready |
| `execution-complete` | `Map<number, NodeResult>` | Flow execution finished |
| `output` | `OutputData` | An External Output node produced data |
| `error` | `EditorError` | An error occurred |
| `loading-status` | `string` | Loading status message changed |

## Programmatic API

Access the API via a template ref:

```vue
<script setup>
import { ref } from 'vue'
import { FlowfileEditor } from 'flowfile-editor'
import 'flowfile-editor/style.css'

const editor = ref()

async function run() {
  if (editor.value?.isReady) {
    await editor.value.executeFlow()
  }
}
</script>

<template>
  <FlowfileEditor ref="editor" />
  <button @click="run">Run</button>
</template>
```

| Method | Returns | Description |
|--------|---------|-------------|
| `isReady` | `boolean` | Whether Pyodide is initialized |
| `isExecuting` | `boolean` | Whether a flow is running |
| `executeFlow()` | `Promise<void>` | Run the entire flow |
| `executeNode(nodeId)` | `Promise<NodeResult>` | Run a single node |
| `exportFlow()` | `FlowfileData` | Export the current flow as JSON |
| `importFlow(data)` | `boolean` | Load a flow from JSON |
| `setInputData(name, content, format?)` | `void` | Push a named dataset (CSV string, or `Uint8Array` of Arrow IPC / Parquet bytes) |
| `getNodeResult(nodeId)` | `NodeResult \| undefined` | Get a node's result |
| `getNodeResultArrow(nodeId)` | `Promise<Uint8Array \| null>` | A node's full result frame as Arrow IPC stream bytes, ready for arrow-js or duckdb-wasm |
| `clearFlow()` | `void` | Clear all nodes and edges |
| `initializePyodide()` | `Promise<void>` | Manually init Pyodide (when `autoInit: false`) |

---

## Providing input data

Pass data to External Data nodes via the `inputData` prop or the API:

```vue
<template>
  <FlowfileEditor :input-data="datasets" />
</template>

<script setup>
const datasets = {
  // Simple string (CSV)
  customers: 'name,age,city\nAlice,30,Amsterdam\nBob,25,Berlin',

  // Or with metadata
  orders: {
    content: 'id,amount\n1,100\n2,250',
    format: 'csv',
    delimiter: ','
  },

  // Binary: Arrow IPC stream or Parquet bytes (e.g. from duckdb-wasm or a fetch)
  events: {
    content: parquetBytes,   // Uint8Array
    format: 'parquet'        // or 'arrow-ipc'; omit to sniff (PAR1 magic)
  }
}
</script>
```

## Capturing output

Listen for External Output node results:

```vue
<template>
  <FlowfileEditor @output="handleOutput" />
</template>

<script setup>
function handleOutput(data) {
  console.log(data.nodeId)    // Which node produced it
  console.log(data.content)   // CSV string
  console.log(data.fileName)  // e.g. "result.csv"
  console.log(data.mimeType)  // e.g. "text/csv"
}
</script>
```

---

## CORS headers

Pyodide requires `SharedArrayBuffer`, which needs these HTTP headers on your page:

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

Most dev servers can be configured to send these. For Vite:

```ts
// vite.config.ts
export default defineConfig({
  server: {
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp'
    }
  }
})
```

### Content-Security-Policy allowlist

If your page sets a CSP, the editor needs network access to:

- `cdn.jsdelivr.net` for the Pyodide runtime (always) and parquet-wasm (only when a flow reads or writes Parquet)
- `pypi.org` and `files.pythonhosted.org` only when a flow uses Excel files (openpyxl and xlsxwriter are installed with micropip on first use; CSV-only flows never touch PyPI)

---

## Node types

| Category | Nodes |
|---|---|
| **Input** | Read File (CSV / Excel / Parquet), Manual Input, External Data, Read from Catalog |
| **Transform** | Filter, Select, Formula, Sort, Polars Code, Unique, Rename (dynamic), Record ID, Take Sample |
| **Combine** | Join, Cross Join, Union |
| **Aggregate / reshape** | Group By, Pivot, Unpivot |
| **Output** | Explore Data, Output (download), Write to Catalog, External Output (emits to host) |

Hosts can show or hide categories and individual node types via the `nodeCategories` prop. The full-platform nodes (databases, cloud storage, Kafka, fuzzy matching, machine learning, Python kernels and more) appear as unavailable entries that link to the Flowfile docs, so users know what is one `pip install` away.

## TypeScript

All types are exported:

```ts
import type {
  FlowfileEditorProps,
  FlowfileEditorAPI,
  FlowfileData,
  InputDataMap,
  OutputData,
  NodeResult
} from 'flowfile-editor'
```

---

## Development

This package lives in the `flowfile_wasm/` directory of the [Flowfile monorepo](https://github.com/edwardvaneechoud/Flowfile).

```bash
cd flowfile_wasm
npm install
npm run dev        # Dev server at http://localhost:5174
npm run build:lib  # Build the library to dist/
npm run test:run   # Run the test suite
```

## The full platform

Flowfile is the same canvas with a Python backend behind it: 46 node types, connectors for five databases, S3 / ADLS / GCS, Kafka, Google Analytics and REST APIs, a Delta-backed data catalog with a SQL editor, a scheduler, sandboxed Python kernels, an AI assistant, and Python code export. It runs as a desktop app, a `pip install`, or a Docker stack for a team.

- [Flowfile on GitHub](https://github.com/edwardvaneechoud/Flowfile)
- [Documentation](https://edwardvaneechoud.github.io/Flowfile/)
- [Releases](https://github.com/edwardvaneechoud/Flowfile/releases)
- [Discussions](https://github.com/edwardvaneechoud/Flowfile/discussions)

## License

MIT. See the [Flowfile repository](https://github.com/edwardvaneechoud/Flowfile/blob/main/LICENSE) for the full text.
