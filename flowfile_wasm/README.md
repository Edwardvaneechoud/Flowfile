# flowfile-editor

> A visual ETL editor you can drop into any web app — Polars pipelines that run
> **entirely in the browser**. No backend, no server round-trips, data never leaves
> the page.

<p>
  <a href="https://demo.flowfile.org"><b>▶ Live demo</b></a> ·
  <a href="https://www.npmjs.com/package/flowfile-editor">npm</a> ·
  <a href="https://github.com/Edwardvaneechoud/Flowfile">Full platform</a>
</p>

Build CSV / Excel / Parquet transformation pipelines on a drag-and-drop canvas —
joins, filters, group-bys, pivots, formulas, and a raw Polars code node.
[Polars](https://pola.rs/) runs in WebAssembly via [Pyodide](https://pyodide.org/),
so every operation executes client-side. Ship it as a single `<FlowfileEditor />`
Vue component, drive it from your own UI, and read results back as Arrow.

**Why flowfile-editor**

- **🦀 Real Polars, in the browser** — a full lazy DAG with 20+ node types
  (joins, formula, dynamic rename, pivots, union, record id, a raw Polars node), not a toy.
- **🔌 One component to embed** — `<FlowfileEditor />` with a ref API
  (`executeFlow`, `setInputData`, `getNodeResult`) and events (`ready`, `output`, …).
- **🔒 Zero backend** — nothing to host, nothing to secure; the user's data stays
  on their machine.
- **🏹 Arrow-native I/O** — push/pull frames as Arrow IPC bytes; pairs cleanly with
  duckdb-wasm or arrow-js.

> Want the full platform — databases, cloud storage, Kafka, a Delta catalog, a
> scheduler, and an AI assistant? That's [Flowfile](https://github.com/Edwardvaneechoud/Flowfile)
> (`pip install flowfile`). This package is its browser-native core, embeddable on its own.

## Install

```bash
npm install flowfile-editor
```

**Peer dependencies:** Vue 3.3+ is required. Pinia 2.0+ is optional (the editor creates its own instance if not provided).

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

## Plugin Registration (Optional)

If you prefer global registration:

```ts
import { createApp } from 'vue'
import { FlowfileEditorPlugin } from 'flowfile-editor'
import 'flowfile-editor/style.css'

const app = createApp(App)
app.use(FlowfileEditorPlugin)
// Now <FlowfileEditor /> is available in all templates
```

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

### API Methods

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
| `getNodeResultArrow(nodeId)` | `Promise<Uint8Array \| null>` | A node's full result frame as Arrow IPC stream bytes — feed straight to arrow-js / duckdb-wasm, no CSV stringification |
| `clearFlow()` | `void` | Clear all nodes and edges |
| `initializePyodide()` | `Promise<void>` | Manually init Pyodide (when `autoInit: false`) |

## Providing Input Data

Pass data to External Data nodes via the `inputData` prop or API:

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

## Capturing Output

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

## CORS Headers

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

- `cdn.jsdelivr.net` — Pyodide runtime (always) and parquet-wasm (only when a
  flow reads/writes Parquet)
- `pypi.org` + `files.pythonhosted.org` — only when a flow uses Excel files
  (openpyxl/xlsxwriter are micropip-installed on first use; CSV-only flows
  never touch PyPI)

## Available Node Types

**Input:** Read File (CSV/Excel/Parquet), Manual Input, External Data, Read from Catalog
**Transform:** Filter, Select, Formula, Sort, Polars Code, Unique, Rename (dynamic), Record ID, Take Sample
**Combine:** Join, Cross Join, Union
**Aggregate / reshape:** Group By, Pivot, Unpivot
**Output:** Explore Data, Output (download), Write to Catalog, External Output (emits to host)

> The default palette ships 20+ node types. Hosts can show/hide categories and
> node types via the `nodeCategories` prop.

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

## Development

```bash
cd flowfile_wasm
npm install
npm run dev        # Dev server at http://localhost:5174
npm run build:lib  # Build the library to dist/
npm run test:run   # Run tests
```

## License

See the [Flowfile repository](https://github.com/Edwardvaneechoud/Flowfile) for license details.
