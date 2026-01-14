<!--
  Minimal example: Embedding flowfile_wasm in your Vue 3 app

  This shows the complete pattern:
  1. Load a pre-designed flow
  2. Inject your app's data into source nodes
  3. Execute and get results
-->

<template>
  <div class="app">
    <h1>My Data App</h1>

    <!-- Your app's data source -->
    <div class="data-source">
      <h2>Input Data</h2>
      <textarea v-model="myData" rows="5"></textarea>
      <button @click="runAnalysis" :disabled="!ready">
        Run Analysis
      </button>
    </div>

    <!-- Embedded Flowfile Editor -->
    <div class="editor-wrapper">
      <FlowfileEditor
        ref="editor"
        :initial-flow="analysisFlow"
        height="500px"
        @pyodide-ready="ready = true"
        @execution-complete="onResults"
      />
    </div>

    <!-- Results from the flow -->
    <div class="results" v-if="results">
      <h2>Results</h2>
      <table>
        <thead>
          <tr>
            <th v-for="col in results.columns" :key="col">{{ col }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, i) in results.data" :key="i">
            <td v-for="(cell, j) in row" :key="j">{{ cell }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import type { FlowfileData, NodeResult } from 'flowfile-wasm'

const editor = ref()
const ready = ref(false)
const results = ref<{ columns: string[]; data: any[][] } | null>(null)

// Sample data from your app
const myData = ref(`name,sales,region
Alice,1200,North
Bob,800,South
Charlie,1500,North
Diana,900,South
Eve,2000,North`)

// Pre-designed flow (you'd typically load this from a file/API)
// This flow: reads CSV → groups by region → sums sales
const analysisFlow: FlowfileData = {
  flowfile_version: '1.0.0',
  flowfile_id: 1,
  flowfile_name: 'Sales Analysis',
  flowfile_settings: {
    description: 'Group sales by region',
    execution_mode: 'Development',
    execution_location: 'local',
    auto_save: false,
    show_detailed_progress: false
  },
  nodes: [
    {
      id: 1,
      type: 'read',
      is_start_node: true,
      description: 'Input Data',
      x_position: 100,
      y_position: 150,
      input_ids: [],
      outputs: [2],
      setting_input: {
        node_id: 1,
        cache_results: false,
        pos_x: 100,
        pos_y: 150,
        is_setup: true,
        description: 'Input Data',
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
    {
      id: 2,
      type: 'group_by',
      is_start_node: false,
      description: 'Sum by Region',
      x_position: 350,
      y_position: 150,
      input_ids: [1],
      outputs: [],
      setting_input: {
        node_id: 2,
        cache_results: false,
        pos_x: 350,
        pos_y: 150,
        is_setup: true,
        description: 'Sum by Region',
        depending_on_id: 1,
        groupby_input: {
          agg_cols: [
            { old_name: 'region', agg: 'groupby', new_name: 'region' },
            { old_name: 'sales', agg: 'sum', new_name: 'total_sales' }
          ]
        }
      }
    }
  ],
  connections: [
    { from_node: 1, to_node: 2, from_handle: 'source', to_handle: 'target' }
  ]
}

// Inject data and execute
async function runAnalysis() {
  if (!editor.value || !ready.value) return

  // Inject your app's data into node 1 (the read node)
  await editor.value.injectData({
    1: { name: 'sales.csv', content: myData.value }
  }, true) // true = auto-execute
}

// Handle results
function onResults(nodeResults: Map<number, NodeResult>) {
  // Get result from the last node (group_by node, id=2)
  const groupByResult = nodeResults.get(2)

  if (groupByResult?.success && groupByResult.data) {
    results.value = {
      columns: groupByResult.data.columns,
      data: groupByResult.data.data
    }
  }
}
</script>

<style>
.app {
  max-width: 1200px;
  margin: 0 auto;
  padding: 20px;
  font-family: system-ui, sans-serif;
}

.data-source {
  margin-bottom: 20px;
}

.data-source textarea {
  width: 100%;
  font-family: monospace;
  padding: 10px;
}

.data-source button {
  margin-top: 10px;
  padding: 10px 20px;
  background: #3b82f6;
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
}

.data-source button:disabled {
  background: #ccc;
}

.editor-wrapper {
  border: 1px solid #e0e0e0;
  border-radius: 8px;
  overflow: hidden;
  margin-bottom: 20px;
}

.results table {
  width: 100%;
  border-collapse: collapse;
}

.results th, .results td {
  border: 1px solid #e0e0e0;
  padding: 8px 12px;
  text-align: left;
}

.results th {
  background: #f5f5f5;
}
</style>
