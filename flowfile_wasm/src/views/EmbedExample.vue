<template>
  <div class="example-page">
    <header class="example-header">
      <h1>Flowfile Embedded Editor — Example</h1>
      <p>Edit the input datasets below, then use <strong>External Data</strong> nodes in the editor to load them.
         Use an <strong>External Output</strong> node to send results back here.</p>
    </header>

    <div class="example-layout">
      <!-- Left panel: Input datasets + Output results -->
      <aside class="data-panel">
        <section class="dataset-section">
          <div class="section-header">
            <h2>Input Datasets</h2>
            <button class="btn btn-sm" @click="addDataset">+ Add Dataset</button>
          </div>

          <div v-for="(ds, idx) in datasets" :key="ds.name" class="dataset-card">
            <div class="dataset-title-row">
              <input
                v-model="ds.name"
                class="dataset-name-input"
                placeholder="dataset_name"
                @change="refreshInputData"
              />
              <button class="btn-icon-danger" @click="removeDataset(idx)" title="Remove dataset">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 6L6 18M6 6l12 12"/></svg>
              </button>
            </div>
            <textarea
              v-model="ds.content"
              class="dataset-textarea"
              :placeholder="'name,age,city\nAlice,30,Amsterdam\nBob,25,Berlin'"
              rows="6"
              @input="refreshInputData"
            ></textarea>
          </div>
        </section>

        <section class="output-section">
          <h2>Output Results</h2>
          <div v-if="outputResults.length === 0" class="empty-state">
            No output yet. Add an <em>External Output</em> node and run the flow.
          </div>
          <div v-for="(out, idx) in outputResults" :key="idx" class="output-card">
            <div class="output-title">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#48BB78" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>
              <span>Node {{ out.nodeId }} — {{ out.fileName }}</span>
            </div>
            <textarea class="output-textarea" readonly :value="out.content" rows="6"></textarea>
          </div>
        </section>
      </aside>

      <!-- Right panel: The embedded editor -->
      <main class="editor-panel">
        <FlowfileEditor
          ref="editorRef"
          :input-data="inputData"
          height="100%"
          width="100%"
          :toolbar="{ showDemo: false }"
          @output="handleOutput"
          @ready="onReady"
        />
      </main>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, computed } from 'vue'
import FlowfileEditor from '../lib/FlowfileEditor.vue'
import type { InputDataMap, OutputData } from '../lib/types'

// ----- Input datasets (editable by the user) -----

const datasets = reactive([
  {
    name: 'orders',
    content: `order_id,customer,product,amount,date
1,Alice,Widget A,29.99,2025-01-15
2,Bob,Widget B,49.99,2025-01-16
3,Alice,Widget C,19.99,2025-01-17
4,Charlie,Widget A,29.99,2025-01-18
5,Bob,Widget A,29.99,2025-01-19
6,Alice,Widget B,49.99,2025-01-20
7,Diana,Widget C,19.99,2025-01-21
8,Charlie,Widget B,49.99,2025-01-22`
  },
  {
    name: 'customers',
    content: `customer,email,city
Alice,alice@example.com,Amsterdam
Bob,bob@example.com,Berlin
Charlie,charlie@example.com,Copenhagen
Diana,diana@example.com,Dublin`
  }
])

// Build the inputData map reactively from the editable datasets
const inputData = computed<InputDataMap>(() => {
  const map: InputDataMap = {}
  for (const ds of datasets) {
    if (ds.name.trim()) {
      map[ds.name.trim()] = ds.content
    }
  }
  return map
})

function refreshInputData() {
  // The computed inputData already reacts to changes
  // This is a no-op placeholder for @input handlers
}

function addDataset() {
  datasets.push({ name: `dataset_${datasets.length + 1}`, content: '' })
}

function removeDataset(idx: number) {
  datasets.splice(idx, 1)
}

// ----- Output results -----

interface OutputResult {
  nodeId: number
  content: string
  fileName: string
}

const outputResults = ref<OutputResult[]>([])

function handleOutput(data: OutputData) {
  // Replace if same nodeId, else append
  const existing = outputResults.value.findIndex(o => o.nodeId === data.nodeId)
  const entry: OutputResult = {
    nodeId: data.nodeId,
    content: data.content,
    fileName: data.fileName
  }
  if (existing >= 0) {
    outputResults.value[existing] = entry
  } else {
    outputResults.value.push(entry)
  }
}

// ----- Editor ref -----
const editorRef = ref<InstanceType<typeof FlowfileEditor> | null>(null)

function onReady() {
  console.log('Flowfile editor is ready')
}
</script>

<style scoped>
.example-page {
  display: flex;
  flex-direction: column;
  height: 100vh;
  background: #f5f5f7;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  color: #1a202c;
}

.example-header {
  padding: 16px 24px;
  background: #fff;
  border-bottom: 1px solid #e2e8f0;
}

.example-header h1 {
  margin: 0 0 4px;
  font-size: 18px;
  font-weight: 600;
}

.example-header p {
  margin: 0;
  font-size: 13px;
  color: #4a5568;
}

.example-layout {
  display: flex;
  flex: 1;
  overflow: hidden;
}

/* --- Left data panel --- */
.data-panel {
  width: 340px;
  min-width: 280px;
  display: flex;
  flex-direction: column;
  border-right: 1px solid #e2e8f0;
  background: #fff;
  overflow-y: auto;
}

.dataset-section,
.output-section {
  padding: 16px;
  border-bottom: 1px solid #e2e8f0;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.section-header h2,
.output-section h2 {
  margin: 0 0 8px;
  font-size: 14px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: #2d3748;
}

.section-header h2 {
  margin-bottom: 0;
}

.btn {
  padding: 6px 14px;
  border: 1px solid #cbd5e0;
  border-radius: 6px;
  background: #fff;
  font-size: 12px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.15s;
}

.btn:hover {
  background: #edf2f7;
  border-color: #a0aec0;
}

.btn-sm {
  padding: 4px 10px;
  font-size: 11px;
}

.btn-icon-danger {
  background: none;
  border: none;
  cursor: pointer;
  color: #a0aec0;
  padding: 4px;
  border-radius: 4px;
  display: flex;
  align-items: center;
}

.btn-icon-danger:hover {
  color: #e53e3e;
  background: #fed7d7;
}

/* Dataset cards */
.dataset-card {
  margin-bottom: 12px;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  overflow: hidden;
}

.dataset-title-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 10px;
  background: #f7fafc;
  border-bottom: 1px solid #e2e8f0;
}

.dataset-name-input {
  border: none;
  background: transparent;
  font-size: 13px;
  font-weight: 600;
  color: #2b6cb0;
  outline: none;
  flex: 1;
  font-family: 'SF Mono', 'Fira Code', monospace;
}

.dataset-name-input:focus {
  background: #fff;
  border-radius: 3px;
  box-shadow: 0 0 0 2px #bee3f8;
}

.dataset-textarea {
  width: 100%;
  border: none;
  padding: 10px;
  font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace;
  font-size: 11.5px;
  line-height: 1.5;
  resize: vertical;
  background: #fff;
  color: #2d3748;
  outline: none;
  box-sizing: border-box;
}

.dataset-textarea:focus {
  background: #fffff0;
}

/* Output cards */
.empty-state {
  font-size: 12px;
  color: #a0aec0;
  padding: 16px;
  text-align: center;
  background: #f7fafc;
  border-radius: 6px;
}

.output-card {
  margin-bottom: 10px;
  border: 1px solid #c6f6d5;
  border-radius: 8px;
  overflow: hidden;
}

.output-title {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 10px;
  background: #f0fff4;
  font-size: 12px;
  font-weight: 600;
  color: #276749;
  border-bottom: 1px solid #c6f6d5;
}

.output-textarea {
  width: 100%;
  border: none;
  padding: 10px;
  font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace;
  font-size: 11.5px;
  line-height: 1.5;
  resize: vertical;
  background: #fff;
  color: #2d3748;
  box-sizing: border-box;
}

/* --- Right editor panel --- */
.editor-panel {
  flex: 1;
  overflow: hidden;
}
</style>
