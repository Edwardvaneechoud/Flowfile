<template>
  <div class="listbox-wrapper">
    <div class="switch-wrapper" style="margin-bottom: 12px;">
      <label class="checkbox-label">
        <input type="checkbox" v-model="isAdvanced" @change="handleModeChange" />
        <span>Advanced filter options</span>
      </label>
    </div>

    <template v-if="!isAdvanced">
      <div class="filter-section">
        <div class="filter-row">
          <!-- Column Selector -->
          <div class="filter-field">
            <label class="filter-label">Column</label>
            <select v-model="basicFilter.field" @change="emitUpdate" class="select">
              <option value="">Select column...</option>
              <option v-for="col in columns" :key="col.name" :value="col.name">
                {{ col.name }} ({{ col.data_type }})
              </option>
            </select>
          </div>

          <!-- Operator Selector -->
          <div class="filter-field">
            <label class="filter-label">Operator</label>
            <select v-model="basicFilter.operator" @change="emitUpdate" class="select">
              <option v-for="op in operators" :key="op.value" :value="op.value">
                {{ op.label }}
              </option>
            </select>
          </div>

          <!-- Value Input (shown for most operators) -->
          <div v-if="showValueInput" class="filter-field">
            <label class="filter-label">Value</label>
            <input
              type="text"
              v-model="basicFilter.value"
              @input="emitUpdate"
              class="input"
              :placeholder="valuePlaceholder"
            />
          </div>

          <!-- Second Value Input (for BETWEEN) -->
          <div v-if="showValue2Input" class="filter-field">
            <label class="filter-label">And</label>
            <input
              type="text"
              v-model="basicFilter.value2"
              @input="emitUpdate"
              class="input"
              placeholder="End value"
            />
          </div>
        </div>

        <!-- Help text for special operators -->
        <div v-if="helpText" class="help-text" style="margin-top: 8px;">{{ helpText }}</div>
      </div>
    </template>

    <template v-else>
      <div class="listbox-subtitle">Advanced filter</div>
      <div class="form-group">
        <textarea
          v-model="advancedFilter"
          @input="emitUpdate"
          class="input"
          rows="5"
          placeholder="e.g., pl.col('age') > 30"
          style="min-height: 120px; resize: vertical;"
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
/* Component uses global styles from main.css */
</style>
