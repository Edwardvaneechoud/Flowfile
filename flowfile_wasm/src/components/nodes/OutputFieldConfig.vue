<template>
  <div class="output-field-config">
    <div class="config-header">
      <div class="toggle-section">
        <label class="toggle-label">
          <input
            type="checkbox"
            :checked="localConfig.enabled"
            @change="toggleEnabled"
          />
          <span>Enable Output Field Configuration</span>
        </label>
      </div>

      <div v-if="localConfig.enabled" class="behavior-section">
        <label>VM Behavior:</label>
        <select v-model="localConfig.vm_behavior" @change="emitUpdate" class="vm-behavior-select">
          <option value="select_only">Select Only (keep only specified fields)</option>
          <option value="add_missing">Add Missing (add fields with defaults)</option>
          <option value="raise_on_missing">Raise on Missing (error if fields missing)</option>
        </select>
      </div>
    </div>

    <div v-if="localConfig.enabled" class="fields-section">
      <div class="section-header">
        <h3>Output Fields</h3>
        <div class="action-buttons">
          <button class="btn btn-small btn-secondary" @click="loadFromSchema" title="Load fields from current node schema">
            Load from Schema
          </button>
          <button class="btn btn-small btn-primary" @click="addField">
            + Add Field
          </button>
        </div>
      </div>

      <div v-if="localConfig.fields.length === 0" class="no-fields">
        No output fields configured. Click "Add Field" or "Load from Schema" to get started.
      </div>

      <div v-else class="fields-table-wrapper">
        <table class="styled-table">
          <thead>
            <tr>
              <th style="width: 30px;"></th>
              <th>Field Name</th>
              <th>Data Type</th>
              <th>Default Value</th>
              <th style="width: 40px;"></th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="(field, index) in localConfig.fields"
              :key="index"
              draggable="true"
              @dragstart="onDragStart(index)"
              @dragover.prevent="onDragOver(index)"
              @drop="onDrop(index)"
            >
              <td style="width: 30px; text-align: center; cursor: move;">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <line x1="3" y1="12" x2="21" y2="12"></line>
                  <line x1="3" y1="6" x2="21" y2="6"></line>
                  <line x1="3" y1="18" x2="21" y2="18"></line>
                </svg>
              </td>
              <td>
                <input
                  type="text"
                  v-model="field.name"
                  @input="emitUpdate"
                  class="input-sm"
                  placeholder="field_name"
                />
              </td>
              <td>
                <select v-model="field.data_type" @change="emitUpdate" class="input-sm">
                  <option value="String">String</option>
                  <option value="Int64">Int64</option>
                  <option value="Int32">Int32</option>
                  <option value="Float64">Float64</option>
                  <option value="Float32">Float32</option>
                  <option value="Boolean">Boolean</option>
                  <option value="Date">Date</option>
                  <option value="Datetime">Datetime</option>
                  <option value="Time">Time</option>
                  <option value="List">List</option>
                  <option value="Decimal">Decimal</option>
                </select>
              </td>
              <td>
                <input
                  type="text"
                  v-model="field.default_value"
                  @input="emitUpdate"
                  class="input-sm"
                  placeholder="null or expression"
                />
              </td>
              <td style="width: 40px; text-align: center;">
                <button class="btn-icon-danger" @click="removeField(index)" title="Remove field">
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <line x1="18" y1="6" x2="6" y2="18"></line>
                    <line x1="6" y1="6" x2="18" y2="18"></line>
                  </svg>
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <div v-if="localConfig.fields.length > 0" class="help-text">
        <strong>Tip:</strong> Drag rows to reorder fields. Default values can be literals (e.g., "0", "Unknown") or Polars expressions (e.g., "pl.lit(0)").
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { OutputFieldConfig, OutputFieldInfo, DataTypeStr } from '../../types'

const props = defineProps<{
  nodeId: number
  config: OutputFieldConfig | null | undefined
}>()

const emit = defineEmits<{
  (e: 'update:config', config: OutputFieldConfig): void
}>()

const flowStore = useFlowStore()

// Initialize local config
const localConfig = ref<OutputFieldConfig>(
  props.config || {
    enabled: false,
    vm_behavior: 'select_only',
    fields: []
  }
)

// Watch for external changes
watch(() => props.config, (newConfig) => {
  if (newConfig) {
    localConfig.value = JSON.parse(JSON.stringify(newConfig))
  }
}, { deep: true })

function toggleEnabled() {
  localConfig.value.enabled = !localConfig.value.enabled
  emitUpdate()
}

function emitUpdate() {
  emit('update:config', JSON.parse(JSON.stringify(localConfig.value)))
}

function addField() {
  localConfig.value.fields.push({
    name: '',
    data_type: 'String',
    default_value: null
  })
  emitUpdate()
}

function removeField(index: number) {
  localConfig.value.fields.splice(index, 1)
  emitUpdate()
}

async function loadFromSchema() {
  try {
    // Get the current node's schema
    const node = flowStore.nodes.find(n => n.id === props.nodeId)
    if (!node) return

    // Get node result to access schema
    const result = await flowStore.getNodeResult(props.nodeId)
    if (result?.data?.columns) {
      // Load fields from schema
      localConfig.value.fields = result.data.columns.map((col: any) => ({
        name: col.name,
        data_type: col.dtype as DataTypeStr,
        default_value: null
      }))
      emitUpdate()
    }
  } catch (error) {
    console.error('Error loading schema:', error)
  }
}

// Drag and drop functionality
let draggedIndex: number | null = null

function onDragStart(index: number) {
  draggedIndex = index
}

function onDragOver(index: number) {
  // Visual feedback could be added here
}

function onDrop(dropIndex: number) {
  if (draggedIndex === null || draggedIndex === dropIndex) return

  const fields = [...localConfig.value.fields]
  const [draggedField] = fields.splice(draggedIndex, 1)
  fields.splice(dropIndex, 0, draggedField)

  localConfig.value.fields = fields
  draggedIndex = null
  emitUpdate()
}
</script>

<style scoped>
.output-field-config {
  padding: 12px;
}

.config-header {
  margin-bottom: 16px;
}

.toggle-section {
  margin-bottom: 12px;
}

.toggle-label {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
  font-weight: 500;
}

.toggle-label input[type="checkbox"] {
  width: 18px;
  height: 18px;
  cursor: pointer;
}

.behavior-section {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.behavior-section label {
  font-size: 0.9em;
  font-weight: 500;
  color: #555;
}

.vm-behavior-select {
  padding: 6px;
  border: 1px solid #ccc;
  border-radius: 4px;
  font-size: 0.9em;
}

.fields-section {
  margin-top: 16px;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.section-header h3 {
  margin: 0;
  font-size: 1em;
  font-weight: 600;
}

.action-buttons {
  display: flex;
  gap: 8px;
}

.no-fields {
  padding: 20px;
  text-align: center;
  color: #666;
  font-style: italic;
  background: #f5f5f5;
  border-radius: 4px;
}

.fields-table-wrapper {
  overflow-x: auto;
}

.styled-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.9em;
}

.styled-table thead tr {
  background-color: #f0f0f0;
  text-align: left;
}

.styled-table th,
.styled-table td {
  padding: 8px;
  border: 1px solid #ddd;
}

.styled-table tbody tr:hover {
  background-color: #f9f9f9;
}

.input-sm {
  width: 100%;
  padding: 4px 6px;
  border: 1px solid #ccc;
  border-radius: 3px;
  font-size: 0.9em;
}

.input-sm:focus {
  outline: none;
  border-color: #4CAF50;
}

.btn {
  padding: 6px 12px;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 0.9em;
  transition: background-color 0.2s;
}

.btn-small {
  padding: 4px 8px;
  font-size: 0.85em;
}

.btn-primary {
  background-color: #4CAF50;
  color: white;
}

.btn-primary:hover {
  background-color: #45a049;
}

.btn-secondary {
  background-color: #f0f0f0;
  color: #333;
}

.btn-secondary:hover {
  background-color: #e0e0e0;
}

.btn-icon-danger {
  background: none;
  border: none;
  color: #f44336;
  cursor: pointer;
  padding: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 3px;
}

.btn-icon-danger:hover {
  background-color: #ffebee;
}

.help-text {
  margin-top: 12px;
  padding: 8px;
  background: #e3f2fd;
  border-left: 3px solid #2196F3;
  font-size: 0.85em;
  color: #555;
}
</style>
