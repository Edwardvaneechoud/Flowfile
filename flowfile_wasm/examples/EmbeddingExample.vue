<!--
  Minimal example: Embedding flowfile_wasm in your Vue 3 app

  This shows the complete pattern:
  1. Load a pre-designed flow
  2. Bind your app's data using named inputs (matches node descriptions)
  3. Get results via v-model:outputs (also by name)
-->

<template>
  <div class="app">
    <h1>My Data App</h1>

    <!-- Your app's data source -->
    <div class="data-source">
      <h2>Input Data</h2>
      <textarea v-model="inputs.sales_data" rows="5"></textarea>
      <p class="hint">Edit the data above - flow auto-executes on change!</p>
    </div>

    <!-- Embedded Flowfile Editor with named bindings -->
    <div class="editor-wrapper">
      <FlowfileEditor
        :initial-flow="analysisFlow"
        :inputs="inputs"
        v-model:outputs="outputs"
        :auto-execute="true"
        height="500px"
        @pyodide-ready="ready = true"
      />
    </div>

    <!-- Results bound by name! -->
    <div class="results" v-if="outputs.summary">
      <h2>Results (from "summary" node)</h2>
      <table>
        <thead>
          <tr>
            <th v-for="col in outputs.summary.columns" :key="col">{{ col }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, i) in outputs.summary.data" :key="i">
            <td v-for="(cell, j) in row" :key="j">{{ cell }}</td>
          </tr>
        </tbody>
      </table>
      <p>Total rows: {{ outputs.summary.total_rows }}</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive } from 'vue'
import type { FlowfileData, DataPreview } from 'flowfile-wasm'

const ready = ref(false)

// Named inputs - keys match node descriptions in the flow
// When this changes, the flow auto-re-executes!
const inputs = reactive({
  sales_data: `name,sales,region
Alice,1200,North
Bob,800,South
Charlie,1500,North
Diana,900,South
Eve,2000,North`
})

// Named outputs - populated after execution
// Keys match node descriptions in the flow
const outputs = ref<Record<string, DataPreview | null>>({})

// Pre-designed flow with named nodes:
// - "sales_data" (read node) - receives input
// - "summary" (group_by node) - produces output
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
      description: 'sales_data',  // <-- This name is used for input binding!
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
        description: 'sales_data',
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
      description: 'summary',  // <-- This name is used for output binding!
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
        description: 'summary',
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
