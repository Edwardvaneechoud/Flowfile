# flowfile-editor

An embeddable, browser-based data flow editor powered by [Pyodide](https://pyodide.org/) and [Polars](https://pola.rs/). Design data transformation pipelines visually — all computation runs in the browser via WebAssembly.

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
| `setInputData(name, csv)` | `void` | Push a named dataset |
| `getNodeResult(nodeId)` | `NodeResult \| undefined` | Get a node's result |
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

## Available Node Types

**Input:** Read CSV, Manual Input, External Data
**Transform:** Filter, Select, Group By, Join, Sort, Unique, Take Sample, Pivot, Unpivot, Polars Code
**Output:** Preview, Output (download), External Output (emits to host)

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
