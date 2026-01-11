<template>
  <div class="settings-form">
    <div class="form-group">
      <label>Add/Modify Columns</label>
      <div class="help-text">Define new columns using Polars expressions</div>
    </div>

    <div v-if="columns.length === 0" class="no-columns">
      No input columns available. Connect an input node first.
    </div>

    <template v-else>
      <div class="available-columns">
        <label>Available Columns</label>
        <div class="column-chips">
          <span
            v-for="col in columns"
            :key="col.name"
            class="column-chip"
            @click="insertColumn(col.name)"
            :title="'Click to insert pl.col(&quot;' + col.name + '&quot;)'"
          >
            {{ col.name }}
          </span>
        </div>
      </div>

      <div class="column-definitions">
        <div v-for="(colDef, idx) in columnDefs" :key="idx" class="column-def">
          <div class="def-header">
            <input
              type="text"
              v-model="colDef.name"
              @input="emitUpdate"
              class="input name-input"
              placeholder="Column name"
            />
            <button class="remove-btn" @click="removeColumn(idx)">×</button>
          </div>
          <textarea
            v-model="colDef.expression"
            @input="emitUpdate"
            class="textarea"
            rows="2"
            placeholder="e.g., pl.col('a') + pl.col('b')"
          ></textarea>
        </div>
      </div>

      <button class="btn btn-secondary" @click="addColumn">+ Add Column</button>
    </template>

    <div class="expression-help">
      <label>Expression Examples</label>
      <div class="examples">
        <div class="example">
          <code>pl.col("a") + pl.col("b")</code>
          <span>Add columns</span>
        </div>
        <div class="example">
          <code>pl.col("a") * 2</code>
          <span>Multiply by constant</span>
        </div>
        <div class="example">
          <code>pl.col("name").str.to_uppercase()</code>
          <span>String uppercase</span>
        </div>
        <div class="example">
          <code>pl.lit("constant")</code>
          <span>Literal value</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { WithColumnsSettings, WithColumnDef, ColumnSchema } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: WithColumnsSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: WithColumnsSettings): void
}>()

const flowStore = useFlowStore()

const columnDefs = ref<WithColumnDef[]>(props.settings.with_columns_input?.columns || [])

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

watch(() => props.settings.with_columns_input?.columns, (newCols) => {
  if (newCols) {
    columnDefs.value = [...newCols]
  }
}, { deep: true })

function addColumn() {
  columnDefs.value.push({
    name: '',
    expression: ''
  })
  emitUpdate()
}

function removeColumn(index: number) {
  columnDefs.value.splice(index, 1)
  emitUpdate()
}

function insertColumn(name: string) {
  // This is a simple implementation - could be enhanced with cursor position
  if (columnDefs.value.length === 0) {
    addColumn()
  }
  const lastDef = columnDefs.value[columnDefs.value.length - 1]
  lastDef.expression += `pl.col("${name}")`
  emitUpdate()
}

function emitUpdate() {
  const settings: WithColumnsSettings = {
    ...props.settings,
    is_setup: columnDefs.value.some(c => c.name && c.expression),
    with_columns_input: {
      columns: [...columnDefs.value]
    }
  }
  emit('update:settings', settings)
}
</script>

<style scoped>
.settings-form {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.form-group label {
  font-size: 13px;
  font-weight: 500;
  color: var(--text-primary);
}

.help-text {
  font-size: 12px;
  color: var(--text-secondary);
}

.no-columns {
  padding: 16px;
  text-align: center;
  color: var(--text-secondary);
  background: var(--bg-tertiary);
  border-radius: var(--radius-md);
}

.available-columns label {
  display: block;
  font-size: 12px;
  font-weight: 500;
  margin-bottom: 8px;
}

.column-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

.column-chip {
  padding: 4px 8px;
  font-size: 11px;
  background: var(--bg-tertiary);
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: all 0.15s;
}

.column-chip:hover {
  background: var(--accent-color);
  color: white;
}

.column-definitions {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.column-def {
  background: var(--bg-tertiary);
  border-radius: var(--radius-sm);
  padding: 12px;
}

.def-header {
  display: flex;
  gap: 8px;
  margin-bottom: 8px;
}

.name-input {
  flex: 1;
}

.input, .textarea {
  width: 100%;
  padding: 8px 12px;
  font-size: 13px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
  font-family: inherit;
}

.textarea {
  resize: vertical;
  font-family: monospace;
  font-size: 12px;
}

.remove-btn {
  background: none;
  border: none;
  color: var(--error-color);
  font-size: 18px;
  cursor: pointer;
  padding: 0 8px;
}

.btn {
  padding: 8px 16px;
  font-size: 13px;
  border: none;
  border-radius: var(--radius-sm);
  cursor: pointer;
}

.btn-secondary {
  background: var(--bg-tertiary);
  color: var(--text-primary);
}

.btn-secondary:hover {
  background: var(--border-color);
}

.expression-help {
  background: var(--bg-tertiary);
  border-radius: var(--radius-sm);
  padding: 12px;
}

.expression-help label {
  display: block;
  font-size: 12px;
  font-weight: 500;
  margin-bottom: 8px;
}

.examples {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.example {
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 12px;
}

.example code {
  background: var(--bg-secondary);
  padding: 2px 6px;
  border-radius: 3px;
  font-family: monospace;
  font-size: 11px;
}

.example span {
  color: var(--text-secondary);
}
</style>
