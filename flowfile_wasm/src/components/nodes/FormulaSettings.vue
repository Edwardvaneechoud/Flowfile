<template>
  <div class="formula-settings">
    <!-- Output field + data type (mirrors the desktop selector-container) -->
    <div class="selector-container">
      <div class="selector-field">
        <label>Output field</label>
        <input
          v-model="outputName"
          type="text"
          class="ff-input"
          list="formula-columns"
          placeholder="Select or create field"
          @input="emitUpdate"
        />
        <datalist id="formula-columns">
          <option v-for="col in columns" :key="col.name" :value="col.name" />
        </datalist>
      </div>
      <div class="selector-field selector-type">
        <label>Data type</label>
        <select v-model="outputType" class="ff-input" @change="emitUpdate">
          <option v-for="t in DATA_TYPES" :key="t" :value="t">{{ t }}</option>
        </select>
      </div>
    </div>

    <ExpressionEditor
      :node-id="props.nodeId"
      :model-value="formula"
      @update:model-value="handleFormulaChange"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { NodeFormulaSettings, ColumnSchema } from '../../types'
import ExpressionEditor from '../common/ExpressionEditor.vue'

const DATA_TYPES = ['Auto', 'String', 'Int64', 'Float64', 'Boolean', 'Date', 'Datetime']

const props = defineProps<{
  nodeId: number
  settings: NodeFormulaSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeFormulaSettings): void
}>()

const flowStore = useFlowStore()

const outputName = ref(props.settings.function?.field?.name ?? '')
const outputType = ref(props.settings.function?.field?.data_type || 'Auto')
const formula = ref(props.settings.function?.function || '')

const columns = computed<ColumnSchema[]>(() => flowStore.getNodeInputSchema(props.nodeId))

function handleFormulaChange(value: string) {
  formula.value = value
  emitUpdate()
}

function emitUpdate() {
  const settings: NodeFormulaSettings = {
    ...props.settings,
    is_setup: outputName.value.trim().length > 0 && formula.value.trim().length > 0,
    function: {
      field: { name: outputName.value.trim(), data_type: outputType.value },
      function: formula.value,
    },
  }
  emit('update:settings', settings)
}
</script>

<style scoped>
.formula-settings {
  display: flex;
  flex-direction: column;
  gap: 12px;
  padding: 12px;
  color: var(--color-text-primary);
}

.selector-container {
  display: flex;
  align-items: flex-end;
  gap: 10px;
}

.selector-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
  flex: 1;
}

.selector-field.selector-type {
  flex: 0 0 140px;
}

.selector-field label {
  font-size: 11px;
  font-weight: 600;
  color: var(--color-text-secondary);
}

.ff-input {
  padding: 6px 8px;
  font-size: 13px;
  color: var(--color-text-primary);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  outline: none;
}

.ff-input:focus {
  border-color: var(--color-border-focus);
  box-shadow: 0 0 0 2px var(--color-focus-ring-accent);
}
</style>
