<template>
  <div class="settings-form">
    <div class="form-group">
      <label class="checkbox-label">
        <input type="checkbox" v-model="isAdvanced" @change="handleModeChange" />
        <span>Advanced Filter</span>
      </label>
    </div>

    <template v-if="!isAdvanced">
      <div class="form-group">
        <label>Column</label>
        <select v-model="basicFilter.field" @change="emitUpdate" class="select">
          <option value="">Select column...</option>
          <option v-for="col in columns" :key="col.name" :value="col.name">
            {{ col.name }} ({{ col.data_type }})
          </option>
        </select>
      </div>

      <div class="form-group">
        <label>Operator</label>
        <select v-model="basicFilter.operator" @change="emitUpdate" class="select">
          <option v-for="op in operators" :key="op.value" :value="op.value">
            {{ op.label }}
          </option>
        </select>
      </div>

      <div v-if="showValueInput" class="form-group">
        <label>Value</label>
        <input
          type="text"
          v-model="basicFilter.value"
          @input="emitUpdate"
          class="input"
          :placeholder="valuePlaceholder"
        />
      </div>

      <div v-if="showValue2Input" class="form-group">
        <label>And</label>
        <input
          type="text"
          v-model="basicFilter.value2"
          @input="emitUpdate"
          class="input"
          placeholder="End value"
        />
      </div>

      <div v-if="helpText" class="help-text">{{ helpText }}</div>
    </template>

    <template v-else>
      <div class="form-group">
        <label>Polars Expression</label>
        <textarea
          v-model="advancedFilter"
          @input="emitUpdate"
          class="textarea"
          rows="5"
          placeholder="e.g., pl.col('age') > 30"
        ></textarea>
        <div class="help-text">
          Use Polars expressions like pl.col('column_name') > value
        </div>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { FilterSettings, FilterOperator, ColumnSchema } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: FilterSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: FilterSettings): void
}>()

const flowStore = useFlowStore()

const isAdvanced = ref(props.settings.filter_input?.mode === 'advanced')
const basicFilter = ref({
  field: props.settings.filter_input?.basic_filter?.field || '',
  operator: (props.settings.filter_input?.basic_filter?.operator || 'equals') as FilterOperator,
  value: props.settings.filter_input?.basic_filter?.value || '',
  value2: props.settings.filter_input?.basic_filter?.value2 || ''
})
const advancedFilter = ref(props.settings.filter_input?.advanced_filter || '')

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

const operators = [
  { value: 'equals', label: 'Equals' },
  { value: 'not_equals', label: 'Not Equals' },
  { value: 'greater_than', label: 'Greater Than' },
  { value: 'greater_than_or_equals', label: 'Greater Than or Equals' },
  { value: 'less_than', label: 'Less Than' },
  { value: 'less_than_or_equals', label: 'Less Than or Equals' },
  { value: 'contains', label: 'Contains' },
  { value: 'not_contains', label: 'Not Contains' },
  { value: 'starts_with', label: 'Starts With' },
  { value: 'ends_with', label: 'Ends With' },
  { value: 'is_null', label: 'Is Null' },
  { value: 'is_not_null', label: 'Is Not Null' },
  { value: 'in', label: 'In' },
  { value: 'not_in', label: 'Not In' },
  { value: 'between', label: 'Between' }
]

const noValueOperators = ['is_null', 'is_not_null']
const value2Operators = ['between']

const showValueInput = computed(() => !noValueOperators.includes(basicFilter.value.operator))
const showValue2Input = computed(() => value2Operators.includes(basicFilter.value.operator))

const valuePlaceholder = computed(() => {
  if (['in', 'not_in'].includes(basicFilter.value.operator)) {
    return 'value1, value2, value3'
  }
  if (basicFilter.value.operator === 'between') {
    return 'Start value'
  }
  return 'Enter value'
})

const helpText = computed(() => {
  switch (basicFilter.value.operator) {
    case 'in':
    case 'not_in':
      return 'Enter comma-separated values'
    case 'between':
      return 'Enter the range boundaries (inclusive)'
    case 'is_null':
      return 'Filters rows where the column value is null'
    case 'is_not_null':
      return 'Filters rows where the column value is not null'
    default:
      return ''
  }
})

watch(() => props.settings, (newSettings) => {
  isAdvanced.value = newSettings.filter_input?.mode === 'advanced'
  basicFilter.value = {
    field: newSettings.filter_input?.basic_filter?.field || '',
    operator: (newSettings.filter_input?.basic_filter?.operator || 'equals') as FilterOperator,
    value: newSettings.filter_input?.basic_filter?.value || '',
    value2: newSettings.filter_input?.basic_filter?.value2 || ''
  }
  advancedFilter.value = newSettings.filter_input?.advanced_filter || ''
}, { deep: true })

function handleModeChange() {
  emitUpdate()
}

function emitUpdate() {
  const settings: FilterSettings = {
    ...props.settings,
    is_setup: true,
    filter_input: {
      mode: isAdvanced.value ? 'advanced' : 'basic',
      basic_filter: { ...basicFilter.value },
      advanced_filter: advancedFilter.value
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

.form-group {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.form-group label {
  font-size: 13px;
  font-weight: 500;
  color: var(--text-primary);
}

.checkbox-label {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
}

.checkbox-label input {
  width: 16px;
  height: 16px;
}

.input, .select, .textarea {
  width: 100%;
  padding: 8px 12px;
  font-size: 13px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
  font-family: inherit;
}

.input:focus, .select:focus, .textarea:focus {
  outline: none;
  border-color: var(--accent-color);
}

.textarea {
  resize: vertical;
  min-height: 80px;
}

.help-text {
  font-size: 12px;
  color: var(--text-secondary);
  font-style: italic;
}
</style>
