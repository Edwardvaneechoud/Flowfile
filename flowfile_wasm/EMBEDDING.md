# Embedding Flowfile WASM in Vue Applications

This guide explains how to embed the Flowfile WASM data flow designer into your Vue 3 application.

## Installation

```bash
# Install as a dependency
npm install flowfile-wasm

# Or from a local build
npm run build:lib  # In the flowfile_wasm directory
```

## Quick Start

### 1. Basic Setup

```typescript
// main.ts
import { createApp } from 'vue'
import { createPinia } from 'pinia'
import App from './App.vue'

// Import the Flowfile plugin and styles
import { FlowfileWasmPlugin } from 'flowfile-wasm'
import 'flowfile-wasm/style.css'

const app = createApp(App)
app.use(createPinia())  // Required - Flowfile uses Pinia for state management
app.use(FlowfileWasmPlugin)  // Registers FlowfileEditor globally
app.mount('#app')
```

### 2. Use the Component

```vue
<template>
  <div class="my-app">
    <FlowfileEditor
      height="600px"
      @pyodide-ready="onReady"
      @execution-complete="onExecutionComplete"
    />
  </div>
</template>

<script setup lang="ts">
import type { NodeResult } from 'flowfile-wasm'

function onReady() {
  console.log('Flowfile is ready!')
}

function onExecutionComplete(results: Map<number, NodeResult>) {
  console.log('Execution complete:', results)
}
</script>
```

## Loading External Data

### Option 1: Pass Initial Data via Props

```vue
<template>
  <FlowfileEditor
    :initial-data="csvData"
    :auto-execute="true"
    @data-loaded="onDataLoaded"
  />
</template>

<script setup lang="ts">
import { ref } from 'vue'

// Raw CSV string
const csvData = ref(`name,age,city
Alice,30,NYC
Bob,25,LA
Charlie,35,Chicago`)
</script>
```

### Option 2: Load Data Programmatically

```vue
<template>
  <FlowfileEditor ref="editorRef" />
  <button @click="loadMyData">Load Data</button>
</template>

<script setup lang="ts">
import { ref } from 'vue'

const editorRef = ref()

async function loadMyData() {
  // Wait for Pyodide to be ready
  if (!editorRef.value?.isPyodideReady()) {
    console.log('Waiting for Pyodide...')
    return
  }

  // Add a read node and load data
  const nodeId = editorRef.value.addNode('read', 100, 100)
  editorRef.value.loadData(nodeId, myCSVContent, 'mydata.csv')

  // Execute the flow
  await editorRef.value.executeFlow()
}
</script>
```

### Option 3: Load Initial Flow with Data

```vue
<template>
  <FlowfileEditor :initial-flow="savedFlow" />
</template>

<script setup lang="ts">
import type { FlowfileData } from 'flowfile-wasm'

// Load a saved flow (e.g., from your backend)
const savedFlow: FlowfileData = {
  flowfile_version: '1.0.0',
  flowfile_id: 1,
  flowfile_name: 'My Analysis',
  flowfile_settings: {
    description: '',
    execution_mode: 'Development',
    execution_location: 'local',
    auto_save: true,
    show_detailed_progress: false
  },
  nodes: [
    {
      id: 1,
      type: 'read',
      is_start_node: true,
      description: 'Load data',
      x_position: 100,
      y_position: 100,
      input_ids: [],
      outputs: [2],
      setting_input: {
        received_file: {
          name: 'data.csv',
          path: 'data.csv',
          file_type: 'csv',
          table_settings: {
            file_type: 'csv',
            delimiter: ',',
            has_headers: true,
            encoding: 'utf-8'
          }
        }
      }
    },
    // ... more nodes
  ]
}
</script>
```

## Component Props

| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `initialFlow` | `FlowfileData` | - | Pre-load a saved flow |
| `initialData` | `string \| { name: string; content: string }` | - | CSV data to load into a new read node |
| `autoExecute` | `boolean` | `false` | Auto-execute after loading initial data |
| `showHeader` | `boolean` | `false` | Show the header bar with branding |
| `showThemeToggle` | `boolean` | `true` | Show the theme toggle button |
| `theme` | `'light' \| 'dark'` | `'light'` | Initial theme |
| `containerClass` | `string` | `''` | Custom CSS class for the container |
| `height` | `string` | `'100%'` | Editor height |
| `width` | `string` | `'100%'` | Editor width |

## Events

| Event | Payload | Description |
|-------|---------|-------------|
| `pyodide-ready` | - | Pyodide is loaded and ready |
| `flow-change` | `FlowfileData` | Flow structure changed |
| `execution-complete` | `Map<number, NodeResult>` | Flow execution completed |
| `execution-error` | `string` | Execution failed |
| `node-selected` | `number \| null` | Node selection changed |
| `data-loaded` | `nodeId: number, fileName: string` | Data loaded into a node |

## Exposed Methods (via ref)

```typescript
interface FlowfileEditorAPI {
  // Execution
  executeFlow(): Promise<void>
  executeNode(nodeId: number): Promise<NodeResult>

  // Flow management
  exportFlow(name?: string): FlowfileData
  importFlow(data: FlowfileData): boolean
  clearFlow(): void

  // Node operations
  addNode(type: string, x: number, y: number): number
  removeNode(nodeId: number): void
  selectNode(nodeId: number | null): void
  loadData(nodeId: number, content: string, fileName?: string): void

  // Results
  getNodeResult(nodeId: number): NodeResult | undefined
  getAllResults(): Map<number, NodeResult>

  // Status
  isPyodideReady(): boolean

  // Advanced: Direct store access
  getFlowStore(): FlowStore
  getPyodideStore(): PyodideStore
}
```

## Using Individual Stores

For advanced integration, you can use the stores directly:

```typescript
import { useFlowStore, usePyodideStore } from 'flowfile-wasm'

// In a Vue component
const flowStore = useFlowStore()
const pyodideStore = usePyodideStore()

// Access flow state
const nodes = flowStore.nodeList
const results = flowStore.nodeResults

// Execute custom Python
if (pyodideStore.isReady) {
  const result = await pyodideStore.runPythonWithResult(`
    import polars as pl
    # Your custom code
    result = {"success": True, "data": "Hello"}
    result
  `)
}
```

## Using Individual Components

You can also import and use individual components:

```typescript
import {
  Canvas,
  FlowNode,
  DraggablePanel,
  FilterSettings,
  SelectSettings,
  // ... other components
} from 'flowfile-wasm'
```

## Type Definitions

All types are exported for TypeScript users:

```typescript
import type {
  FlowfileData,
  FlowfileNode,
  FlowNode,
  FlowEdge,
  NodeResult,
  ColumnSchema,
  DataPreview,
  NodeSettings,
  // ... many more
} from 'flowfile-wasm'
```

## CORS Requirements

Flowfile WASM uses Pyodide which requires specific CORS headers. Your server must send:

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

For development with Vite, these are automatically configured.

## Browser Requirements

- Modern browser with WebAssembly support
- SharedArrayBuffer support (requires CORS headers above)
- Recommended: Chrome 89+, Firefox 79+, Safari 15.2+

## Example: Complete Integration

```vue
<template>
  <div class="data-analysis-app">
    <aside class="sidebar">
      <h2>My Datasets</h2>
      <ul>
        <li v-for="dataset in datasets" :key="dataset.name">
          <button @click="loadDataset(dataset)">
            {{ dataset.name }}
          </button>
        </li>
      </ul>
    </aside>

    <main class="editor-container">
      <FlowfileEditor
        ref="editorRef"
        height="100%"
        :show-header="true"
        @pyodide-ready="isReady = true"
        @execution-complete="handleResults"
      >
        <template #header-left>
          <span>My Data Analyzer</span>
        </template>
        <template #header-right>
          <button @click="saveFlow">Save</button>
          <button @click="shareFlow">Share</button>
        </template>
      </FlowfileEditor>
    </main>

    <aside class="results-panel" v-if="lastResults">
      <h2>Results Summary</h2>
      <pre>{{ JSON.stringify(lastResults, null, 2) }}</pre>
    </aside>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { FlowfileEditor } from 'flowfile-wasm'
import type { NodeResult } from 'flowfile-wasm'

const editorRef = ref()
const isReady = ref(false)
const lastResults = ref<any>(null)

const datasets = [
  { name: 'Sales Data', content: '...' },
  { name: 'Customer Data', content: '...' },
]

async function loadDataset(dataset: { name: string; content: string }) {
  if (!editorRef.value || !isReady.value) return

  editorRef.value.clearFlow()
  const nodeId = editorRef.value.addNode('read', 100, 100)
  editorRef.value.loadData(nodeId, dataset.content, `${dataset.name}.csv`)
}

function handleResults(results: Map<number, NodeResult>) {
  lastResults.value = Object.fromEntries(results)
}

function saveFlow() {
  const flow = editorRef.value.exportFlow('My Analysis')
  // Save to your backend
  console.log('Saving:', flow)
}

function shareFlow() {
  const flow = editorRef.value.exportFlow('Shared Analysis')
  // Generate share link
  console.log('Sharing:', flow)
}
</script>

<style>
.data-analysis-app {
  display: grid;
  grid-template-columns: 200px 1fr 300px;
  height: 100vh;
}

.editor-container {
  overflow: hidden;
}
</style>
```

## Troubleshooting

### Pyodide fails to load
- Check CORS headers are correctly set
- Verify network access to jsdelivr.net CDN
- Check browser console for specific errors

### Styles not applied
- Ensure you import `flowfile-wasm/style.css`
- Check for CSS conflicts with your application

### Store not working
- Ensure Pinia is installed and used before mounting the app
- Verify you're using Vue 3.3+

## Support

For issues and feature requests, visit:
https://github.com/Edwardvaneechoud/Flowfile
