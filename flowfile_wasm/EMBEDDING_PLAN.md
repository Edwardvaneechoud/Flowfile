# Embeddable Flowfile WASM - Implementation Plan

## Goal

Make `flowfile_wasm` fully embeddable as a drop-in plugin for any web application, similar to how VueFlow is used as a library component. A host application should be able to:

```html
<script src="https://cdn.example.com/flowfile-widget.js"></script>
<flowfile-editor
  initial-data='[{"col":"a","val":1},{"col":"a","val":2}]'
  available-nodes="filter,select,sort,group_by,join"
  theme="dark"
  on-output="handleResult"
/>
```

Or from a Vue/React/Svelte app:

```js
import { FlowfileEditor } from 'flowfile-wasm'

// Programmatic API
const editor = FlowfileEditor.create('#container', {
  input: myDataFrame,
  nodes: ['filter', 'select', 'sort', 'group_by'],
  onOutput: (result) => console.log(result),
})
```

---

## Current State Analysis

The `flowfile_wasm` module already has most of the pieces:

- **VueFlow-based canvas** (`Canvas.vue`) with drag-drop node editor
- **Pyodide + Polars** runtime for in-browser data processing (`pyodide-store.ts`)
- **Full flow execution engine** (`flow-store.ts`) with lazy evaluation, schema inference
- **14 node types** covering input, transform, aggregate, combine, and output
- **Import/export** in FlowfileData YAML/JSON format
- **Session persistence** via sessionStorage + IndexedDB

What's missing for embeddability:

1. **No library build** - only builds as a standalone SPA
2. **Tightly coupled to full-page layout** - header, router, AppPage.vue assume they own the viewport
3. **No external API surface** - no way to programmatically feed data in or get results out
4. **No Web Component wrapper** - can't be used outside Vue
5. **No configurable node palette** - all 14 nodes are always shown
6. **Hardcoded COOP/COEP headers** - embedder must configure these on their server
7. **No event system** - no way for host app to react to flow changes or execution results

---

## Architecture

```
flowfile_wasm/
  src/
    main.ts                    # Existing SPA entry (unchanged)
    embed.ts                   # NEW - Library entry point
    widget.ts                  # NEW - Web Component registration
    api/
      FlowfileEditor.ts       # NEW - Public programmatic API class
      types.ts                 # NEW - Public TypeScript types for consumers
      events.ts                # NEW - Custom event definitions
    components/
      EmbeddableCanvas.vue     # NEW - Headless canvas (no header/toolbar chrome)
      ... existing components
    stores/
      ... existing stores (used internally)
```

### Build Outputs

Vite lib mode will produce:

| Output | Use Case |
|--------|----------|
| `flowfile-editor.es.js` | ES module import for bundler-based apps |
| `flowfile-editor.umd.js` | UMD for `<script>` tag inclusion |
| `flowfile-editor.css` | Extracted styles (optional, auto-injected in Web Component mode) |
| `flowfile-editor.d.ts` | TypeScript declarations |

---

## Implementation Phases

### Phase 1: Decouple the Core Canvas from SPA Shell

**Files to modify:**
- `src/components/Canvas.vue`
- `src/views/AppPage.vue`
- `src/stores/flow-store.ts`
- `src/stores/pyodide-store.ts`

**What to do:**

1. **Extract `EmbeddableCanvas.vue`** from `Canvas.vue`:
   - Strip the full-page header, toolbar, theme toggle, about/docs buttons
   - Accept props for configuration:
     ```ts
     interface EmbeddableCanvasProps {
       availableNodes?: string[]      // Subset of node types to show
       showToolbar?: boolean          // Show run/save/load/clear toolbar
       showMinimap?: boolean          // Show VueFlow minimap
       showNodeList?: boolean         // Show drag-drop sidebar
       showDataPreview?: boolean      // Show bottom data grid
       readOnly?: boolean            // Disable editing (view-only)
       theme?: 'light' | 'dark' | 'auto'
       initialFlow?: FlowfileData    // Pre-loaded flow definition
       height?: string               // CSS height (default: '100%')
     }
     ```
   - Keep `Canvas.vue` as-is for the standalone SPA (it imports `EmbeddableCanvas` internally)

2. **Make stores injectable** (not globally coupled):
   - The Pinia stores (`flow-store`, `pyodide-store`, `panel-store`, etc.) already work within a Pinia instance
   - Ensure each embedded instance gets its own Pinia instance (isolated state)
   - Add a `pyodideCdnUrl` option so embedders can self-host the Pyodide runtime

3. **Remove router dependency for embedded mode**:
   - Currently `main.ts` uses `vue-router` with `AppPage.vue`
   - The embedded component should not require a router
   - `EmbeddableCanvas.vue` is a self-contained component with no route dependencies

### Phase 2: Define the Public API Surface

**New files:**
- `src/api/FlowfileEditor.ts`
- `src/api/types.ts`
- `src/api/events.ts`

**Public API (`FlowfileEditor` class):**

```ts
class FlowfileEditor {
  // Factory
  static create(container: string | HTMLElement, options?: EditorOptions): FlowfileEditor

  // Data Input - feed data into the flow
  setInputData(nodeId: number, data: Record<string, any>[]): void
  setInputDataFromCsv(nodeId: number, csvString: string): void
  addInputNode(name: string, data: Record<string, any>[]): number  // returns nodeId

  // Flow Control
  run(): Promise<ExecutionResult>
  runNode(nodeId: number): Promise<NodeResult>
  stop(): void

  // Flow Definition
  loadFlow(flow: FlowfileData | string): void        // JSON/YAML string or object
  exportFlow(): FlowfileData
  clearFlow(): void

  // Output - get results from the flow
  getOutputData(nodeId: number): Promise<Record<string, any>[]>
  getOutputSchema(nodeId: number): ColumnSchema[]

  // Node Management
  addNode(type: string, x?: number, y?: number): number
  removeNode(nodeId: number): void
  connectNodes(sourceId: number, targetId: number): void

  // Configuration
  setAvailableNodes(types: string[]): void
  setTheme(theme: 'light' | 'dark' | 'auto'): void
  setReadOnly(readOnly: boolean): void

  // Events
  on(event: EditorEvent, callback: Function): void
  off(event: EditorEvent, callback: Function): void

  // Lifecycle
  destroy(): void

  // State
  readonly isReady: boolean          // Pyodide loaded and ready
  readonly isExecuting: boolean      // Flow currently running
  readonly nodes: FlowNode[]         // Current nodes
  readonly edges: FlowEdge[]         // Current edges
}
```

**Events:**

```ts
type EditorEvent =
  | 'ready'                    // Pyodide initialized
  | 'flow:executed'            // Full flow finished
  | 'flow:error'               // Flow execution error
  | 'node:executed'            // Single node finished
  | 'node:error'               // Single node error
  | 'node:added'               // Node added to canvas
  | 'node:removed'             // Node removed
  | 'node:selected'            // Node clicked/selected
  | 'edge:added'               // Connection made
  | 'edge:removed'             // Connection removed
  | 'flow:changed'             // Any flow state change
  | 'output:ready'             // Output node has downloadable data
```

**Types for consumers (`api/types.ts`):**

```ts
interface EditorOptions {
  // Node configuration
  availableNodes?: string[]           // Default: all nodes
  customNodeTypes?: CustomNodeDef[]   // User-defined node types

  // UI configuration
  showToolbar?: boolean               // Default: true
  showNodeList?: boolean              // Default: true
  showDataPreview?: boolean           // Default: true
  showMinimap?: boolean               // Default: true
  readOnly?: boolean                  // Default: false
  theme?: 'light' | 'dark' | 'auto'  // Default: 'auto'
  height?: string                     // Default: '600px'
  width?: string                      // Default: '100%'

  // Data
  initialFlow?: FlowfileData         // Pre-loaded flow
  inputData?: Record<string, any>[]  // Initial data for first input node

  // Runtime
  pyodideCdnUrl?: string             // Custom Pyodide CDN URL
  pyodidePackages?: string[]          // Additional Pyodide packages

  // Callbacks (alternative to .on())
  onReady?: () => void
  onOutput?: (nodeId: number, data: any) => void
  onFlowChanged?: (flow: FlowfileData) => void
  onError?: (error: Error) => void
}

interface ExecutionResult {
  success: boolean
  nodeResults: Map<number, NodeResult>
  errors: Array<{ nodeId: number; error: string }>
  executionTime: number
}
```

### Phase 3: Vite Library Build Configuration

**Modify `vite.config.ts`:**

```ts
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'path'

export default defineConfig(({ mode }) => {
  const isLib = mode === 'lib'

  return {
    plugins: [vue()],
    resolve: {
      alias: { '@': resolve(__dirname, 'src') }
    },

    // Library build settings
    ...(isLib && {
      build: {
        lib: {
          entry: resolve(__dirname, 'src/embed.ts'),
          name: 'FlowfileEditor',
          formats: ['es', 'umd'],
          fileName: (format) => `flowfile-editor.${format}.js`
        },
        rollupOptions: {
          // Do NOT externalize vue - bundle it (consumer may not have Vue)
          // Pyodide is loaded dynamically from CDN, not bundled
          output: {
            assetFileNames: 'flowfile-editor.[ext]',
          }
        },
        target: 'esnext',
        cssCodeSplit: false,  // Single CSS file
      }
    }),

    // SPA build settings (existing)
    ...(!isLib && {
      build: { target: 'esnext' }
    }),

    server: {
      port: 5174,
      headers: {
        'Cross-Origin-Opener-Policy': 'same-origin',
        'Cross-Origin-Embedder-Policy': 'require-corp'
      }
    },
    optimizeDeps: { exclude: ['pyodide'] }
  }
})
```

**New package.json scripts:**

```json
{
  "scripts": {
    "dev": "vite",
    "build": "vue-tsc --noEmit && vite build",
    "build:lib": "vue-tsc --noEmit && vite build --mode lib",
    "build:all": "npm run build && npm run build:lib"
  },
  "main": "dist/flowfile-editor.umd.js",
  "module": "dist/flowfile-editor.es.js",
  "types": "dist/flowfile-editor.d.ts",
  "exports": {
    ".": {
      "import": "./dist/flowfile-editor.es.js",
      "require": "./dist/flowfile-editor.umd.js",
      "types": "./dist/flowfile-editor.d.ts"
    },
    "./style.css": "./dist/flowfile-editor.css"
  }
}
```

### Phase 4: Web Component Wrapper

**New file `src/widget.ts`:**

Register a `<flowfile-editor>` custom element that can be used in any HTML page without Vue:

```ts
import { createApp } from 'vue'
import { createPinia } from 'pinia'
import EmbeddableCanvas from './components/EmbeddableCanvas.vue'
import { FlowfileEditor } from './api/FlowfileEditor'

class FlowfileEditorElement extends HTMLElement {
  private editor: FlowfileEditor | null = null

  static get observedAttributes() {
    return ['theme', 'available-nodes', 'read-only', 'show-toolbar',
            'show-node-list', 'show-minimap', 'initial-data']
  }

  connectedCallback() {
    // Create shadow DOM for style encapsulation
    const shadow = this.attachShadow({ mode: 'open' })

    // Inject styles into shadow DOM
    const style = document.createElement('style')
    style.textContent = EMBEDDED_STYLES  // Bundled CSS
    shadow.appendChild(style)

    // Create mount point
    const mountPoint = document.createElement('div')
    mountPoint.style.width = this.getAttribute('width') || '100%'
    mountPoint.style.height = this.getAttribute('height') || '600px'
    shadow.appendChild(mountPoint)

    // Create Vue app inside shadow DOM
    const app = createApp(EmbeddableCanvas, this.getPropsFromAttributes())
    app.use(createPinia())
    app.mount(mountPoint)

    // Expose programmatic API
    this.editor = new FlowfileEditor(/* ... */)
  }

  disconnectedCallback() {
    this.editor?.destroy()
  }

  attributeChangedCallback(name: string, oldVal: string, newVal: string) {
    // React to attribute changes
  }

  private getPropsFromAttributes() {
    return {
      availableNodes: this.getAttribute('available-nodes')?.split(','),
      theme: this.getAttribute('theme') || 'auto',
      readOnly: this.hasAttribute('read-only'),
      showToolbar: !this.hasAttribute('hide-toolbar'),
      showNodeList: !this.hasAttribute('hide-node-list'),
      showMinimap: !this.hasAttribute('hide-minimap'),
    }
  }
}

// Register the custom element
if (!customElements.get('flowfile-editor')) {
  customElements.define('flowfile-editor', FlowfileEditorElement)
}
```

### Phase 5: Input/Output Data Bridge

This is the core of making Flowfile useful as a plugin - the ability to pipe data in and get transformed data out.

**Input Mechanisms:**

1. **Programmatic data injection** (replaces file upload for embedded use):
   ```ts
   // Pass a JS array of objects - creates an input node automatically
   editor.setInputData(nodeId, [
     { name: 'Alice', age: 30, city: 'NYC' },
     { name: 'Bob', age: 25, city: 'LA' },
   ])
   ```
   Internally: converts to CSV string, calls `flow-store.setFileContent()`, triggers schema inference.

2. **Virtual input node type** (`data_source`):
   - A new node type that doesn't show a file picker
   - Instead shows "Connected Input" with schema display
   - Data comes from the host application via the API
   - Multiple virtual inputs for multi-table flows

3. **URL-based input** (fetch CSV/JSON from URL):
   ```ts
   editor.setInputFromUrl(nodeId, 'https://example.com/data.csv')
   ```

**Output Mechanisms:**

1. **Callback on output node execution:**
   ```ts
   editor.on('output:ready', (event) => {
     console.log(event.nodeId, event.data, event.schema)
   })
   ```

2. **Programmatic output retrieval:**
   ```ts
   const data = await editor.getOutputData(nodeId)
   // Returns: { columns: [...], data: [...], schema: [...] }
   ```

3. **Auto-output mode** (no explicit output node needed):
   ```ts
   // Get the result of the last node in the chain
   const result = await editor.getFlowOutput()
   ```

**Implementation in `flow-store.ts`:**

Add new methods:
```ts
// In useFlowStore:

// Inject data from host application (no file needed)
function injectData(nodeId: number, data: Record<string, any>[]): void {
  // Convert objects to CSV
  const columns = Object.keys(data[0])
  const csv = [
    columns.join(','),
    ...data.map(row => columns.map(col => JSON.stringify(row[col])).join(','))
  ].join('\n')
  setFileContent(nodeId, csv)
}

// Get output as JSON objects
async function getOutputAsObjects(nodeId: number): Promise<Record<string, any>[]> {
  const preview = await fetchNodePreview(nodeId, { maxRows: Infinity })
  if (!preview.data) return []
  return preview.data.data.map((row: any[]) => {
    const obj: Record<string, any> = {}
    preview.data.columns.forEach((col: string, i: number) => {
      obj[col] = row[i]
    })
    return obj
  })
}
```

### Phase 6: Embeddable Toolbar & Chrome Configuration

The current toolbar is part of `Canvas.vue` and assumes a standalone app context. For embedded mode:

**Configurable toolbar (`EmbeddableCanvas.vue`):**

```vue
<template>
  <div class="flowfile-embed" :class="[`theme-${theme}`]" :style="{ height, width }">
    <!-- Optional: Compact toolbar -->
    <div v-if="showToolbar" class="embed-toolbar">
      <button @click="run" :disabled="isExecuting">
        {{ isExecuting ? 'Running...' : 'Run' }}
      </button>
      <slot name="toolbar-extra" />  <!-- Host can inject custom buttons -->
    </div>

    <!-- Optional: Node palette sidebar -->
    <div v-if="showNodeList" class="embed-sidebar">
      <div v-for="node in filteredNodeDefs" :key="node.type" ...>
        <!-- Only show nodes in availableNodes list -->
      </div>
    </div>

    <!-- VueFlow canvas (always shown) -->
    <div class="embed-canvas">
      <VueFlow ... />
    </div>

    <!-- Optional: Data preview -->
    <div v-if="showDataPreview && selectedNode" class="embed-preview">
      <!-- AG Grid preview -->
    </div>
  </div>
</template>
```

### Phase 7: COOP/COEP Header Handling

Pyodide requires `Cross-Origin-Opener-Policy: same-origin` and `Cross-Origin-Embedder-Policy: require-corp` headers. This is a deployment constraint that affects embedders.

**Mitigation strategies:**

1. **Document the requirement clearly** - embedders must set these headers on their server
2. **Provide a service worker fallback** (`coi-serviceworker`):
   ```ts
   // Auto-register COI service worker if headers aren't set
   if (!crossOriginIsolated) {
     const sw = await navigator.serviceWorker.register('/coi-serviceworker.js')
     // Service worker adds headers via fetch interception
   }
   ```
3. **Graceful degradation** - if SharedArrayBuffer isn't available, show a warning but still try to load Pyodide (it works without threads, just slower)
4. **Bundle the service worker** as part of the library distribution

### Phase 8: Packaging & Distribution

**npm package structure:**

```
flowfile-editor/
  dist/
    flowfile-editor.es.js      # ES module (tree-shakeable)
    flowfile-editor.umd.js     # UMD (script tag)
    flowfile-editor.css         # Styles
    flowfile-editor.d.ts        # TypeScript types
    coi-serviceworker.js        # COOP/COEP service worker
  package.json
  README.md
```

**package.json for publishing:**

```json
{
  "name": "flowfile-editor",
  "version": "0.1.0",
  "description": "Embeddable browser-based data flow editor with Polars WASM",
  "main": "dist/flowfile-editor.umd.js",
  "module": "dist/flowfile-editor.es.js",
  "types": "dist/flowfile-editor.d.ts",
  "files": ["dist"],
  "peerDependencies": {},
  "keywords": ["data", "flow", "etl", "polars", "wasm", "editor", "embeddable"]
}
```

---

## File-by-File Change Summary

### New Files

| File | Purpose |
|------|---------|
| `src/embed.ts` | Library entry point - exports `FlowfileEditor` class and `<flowfile-editor>` custom element |
| `src/widget.ts` | Web Component registration logic |
| `src/api/FlowfileEditor.ts` | Public programmatic API class |
| `src/api/types.ts` | Public TypeScript types for consumers |
| `src/api/events.ts` | Event emitter and event type definitions |
| `src/components/EmbeddableCanvas.vue` | Headless, configurable canvas component (no chrome) |

### Modified Files

| File | Changes |
|------|---------|
| `vite.config.ts` | Add library build mode (`--mode lib`) |
| `package.json` | Add `build:lib` script, `main`/`module`/`types`/`exports` fields |
| `src/stores/flow-store.ts` | Add `injectData()`, `getOutputAsObjects()`, event emission hooks |
| `src/stores/pyodide-store.ts` | Make CDN URL configurable, add initialization options |
| `src/components/Canvas.vue` | Refactor to delegate to `EmbeddableCanvas.vue` internally |

### Unchanged Files

All existing node components (`FilterSettings.vue`, `SelectSettings.vue`, etc.), types, composables, and the SPA entry point (`main.ts`) remain unchanged. The standalone SPA continues to work exactly as before.

---

## Implementation Order

| # | Task | Dependencies | Effort |
|---|------|-------------|--------|
| 1 | Create `EmbeddableCanvas.vue` - extract configurable canvas from `Canvas.vue` | None | Medium |
| 2 | Create `api/types.ts` and `api/events.ts` - define the public contract | None | Small |
| 3 | Create `api/FlowfileEditor.ts` - programmatic API wrapping stores | 1, 2 | Large |
| 4 | Add `injectData()` / `getOutputAsObjects()` to `flow-store.ts` | None | Small |
| 5 | Make `pyodide-store.ts` configurable (CDN URL, packages) | None | Small |
| 6 | Create `embed.ts` - library entry point | 1, 3 | Small |
| 7 | Create `widget.ts` - Web Component wrapper | 1, 3, 6 | Medium |
| 8 | Update `vite.config.ts` for lib build mode | 6 | Small |
| 9 | Update `package.json` with exports and build scripts | 8 | Small |
| 10 | Add COOP/COEP service worker fallback | None | Small |
| 11 | Refactor `Canvas.vue` to use `EmbeddableCanvas.vue` internally | 1 | Small |
| 12 | Write integration tests for embedded mode | All above | Medium |
| 13 | Create example pages (vanilla HTML, React, Vue host apps) | All above | Medium |

---

## Usage Examples

### Vanilla HTML

```html
<!DOCTYPE html>
<html>
<head>
  <script src="https://cdn.example.com/flowfile-editor.umd.js"></script>
</head>
<body>
  <flowfile-editor
    height="600px"
    available-nodes="filter,select,sort,group_by"
    theme="light"
  ></flowfile-editor>

  <script>
    const editor = document.querySelector('flowfile-editor')

    editor.addEventListener('ready', () => {
      // Inject data from your app
      editor.setInputData([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
      ])
    })

    editor.addEventListener('output:ready', (e) => {
      console.log('Transformed data:', e.detail.data)
    })
  </script>
</body>
</html>
```

### Vue 3 Host App

```vue
<template>
  <div>
    <h1>My Analytics Dashboard</h1>
    <FlowfileEditor
      :input-data="rawData"
      :available-nodes="['filter', 'group_by', 'sort']"
      height="500px"
      @output:ready="handleOutput"
      @ready="onEditorReady"
    />
    <div v-if="transformedData">
      <h2>Results</h2>
      <pre>{{ transformedData }}</pre>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { FlowfileEditor } from 'flowfile-editor'
import 'flowfile-editor/style.css'

const rawData = ref([...])
const transformedData = ref(null)

function handleOutput(result) {
  transformedData.value = result.data
}
</script>
```

### React Host App

```jsx
import { useEffect, useRef } from 'react'
import { FlowfileEditor } from 'flowfile-editor'
import 'flowfile-editor/style.css'

function DataPipeline({ data, onResult }) {
  const containerRef = useRef(null)
  const editorRef = useRef(null)

  useEffect(() => {
    editorRef.current = FlowfileEditor.create(containerRef.current, {
      inputData: data,
      availableNodes: ['filter', 'select', 'sort', 'group_by'],
      showToolbar: true,
      height: '500px',
      onOutput: (nodeId, result) => onResult(result),
    })

    return () => editorRef.current?.destroy()
  }, [])

  useEffect(() => {
    editorRef.current?.setInputData(1, data)
  }, [data])

  return <div ref={containerRef} />
}
```
