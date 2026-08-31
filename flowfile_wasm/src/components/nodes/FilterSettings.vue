<template>
  <div class="listbox-wrapper">
    <div class="filter-section">
      <div class="filter-field mode-field">
        <label class="filter-label">Condition</label>
        <div class="segmented">
          <button
            v-for="m in modes"
            :key="m.value"
            type="button"
            class="seg-btn"
            :class="{ active: mode === m.value }"
            @click="setMode(m.value)"
          >
            {{ m.label }}
          </button>
        </div>
      </div>

      <!-- Advanced: one flowfile formula, the same dialect the Formula node speaks -->
      <div v-if="mode === 'advanced'" class="filter-field expression-field">
        <label class="filter-label">Expression</label>
        <ExpressionEditor
          :node-id="props.nodeId"
          :model-value="advancedFilter"
          :placeholder="expressionPlaceholder"
          @update:model-value="updateAdvanced"
        />
        <div class="help-text" style="margin-top: 8px;">Keep only the rows this is true for.</div>
      </div>

      <div v-else class="filter-row">
        <!-- Column Selector -->
        <div class="filter-field">
          <label class="filter-label">Column</label>
          <select :value="basicFilter.field" @change="updateField(($event.target as HTMLSelectElement).value)" class="select">
            <option value="">Select column...</option>
            <option v-for="col in columns" :key="col.name" :value="col.name">
              {{ col.name }} ({{ col.data_type }})
            </option>
          </select>
        </div>

        <!-- Operator Selector -->
        <div class="filter-field">
          <label class="filter-label">Operator</label>
          <select :value="basicFilter.operator" @change="updateOperator(($event.target as HTMLSelectElement).value as FilterOperator)" class="select">
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
            :value="basicFilter.value"
            @input="updateValue(($event.target as HTMLInputElement).value)"
            class="input"
            :placeholder="valuePlaceholder"
          />
        </div>

        <!-- Second Value Input (for BETWEEN) -->
        <div v-if="showValue2Input" class="filter-field">
          <label class="filter-label">And</label>
          <input
            type="text"
            :value="basicFilter.value2"
            @input="updateValue2(($event.target as HTMLInputElement).value)"
            class="input"
            placeholder="End value"
          />
        </div>
      </div>

      <!-- Help text for special operators -->
      <div v-if="mode === 'basic' && helpText" class="help-text" style="margin-top: 8px;">{{ helpText }}</div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { FilterSettings, FilterOperator, ColumnSchema } from '../../types'
import ExpressionEditor from '../common/ExpressionEditor.vue'

const props = defineProps<{
  nodeId: number
  settings: FilterSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: FilterSettings): void
}>()

const flowStore = useFlowStore()

// Both halves are kept in state, so switching modes never discards the other one.
const mode = ref<'basic' | 'advanced'>(
  props.settings.filter_input?.mode === 'advanced' ? 'advanced' : 'basic'
)
const advancedFilter = ref(props.settings.filter_input?.advanced_filter || '')
const basicFilter = ref({
  field: props.settings.filter_input?.basic_filter?.field || '',
  operator: (props.settings.filter_input?.basic_filter?.operator || 'equals') as FilterOperator,
  value: props.settings.filter_input?.basic_filter?.value || '',
  value2: props.settings.filter_input?.basic_filter?.value2 || ''
})

const modes = [
  { value: 'basic' as const, label: 'Basic' },
  { value: 'advanced' as const, label: 'Advanced' }
]

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

const expressionPlaceholder = computed(() =>
  columns.value.length ? `e.g. [${columns.value[0].name}] > 7` : 'e.g. [quantity] > 7'
)

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

function updateField(value: string) {
  basicFilter.value.field = value
  emitUpdate()
}

function updateOperator(value: FilterOperator) {
  basicFilter.value.operator = value
  emitUpdate()
}

function updateValue(value: string) {
  basicFilter.value.value = value
  emitUpdate()
}

function updateValue2(value: string) {
  basicFilter.value.value2 = value
  emitUpdate()
}

function updateAdvanced(value: string) {
  advancedFilter.value = value
  emitUpdate()
}

function setMode(value: 'basic' | 'advanced') {
  if (mode.value === value) return
  mode.value = value
  emitUpdate()
}

function emitUpdate() {
  const settings: FilterSettings = {
    ...props.settings,
    is_setup: true,
    filter_input: {
      mode: mode.value,
      basic_filter: { ...basicFilter.value },
      advanced_filter: advancedFilter.value
    }
  }
  emit('update:settings', settings)
}
</script>

<style scoped>
/* Layout comes from global styles in main.css; only the mode switch is local. */
.mode-field {
  margin-bottom: 12px;
}

.segmented {
  display: flex;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  overflow: hidden;
  width: fit-content;
}

.seg-btn {
  padding: 5px 12px;
  font-size: 12px;
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  border: none;
  border-right: 1px solid var(--color-border-primary);
  cursor: pointer;
}

.seg-btn:last-child {
  border-right: none;
}

.seg-btn:hover {
  background: var(--color-background-hover);
  color: var(--color-text-primary);
}

.seg-btn.active {
  background: var(--color-accent);
  color: var(--color-text-inverse);
}

/* The expression editor gets the full drawer width, like the Formula node. */
.expression-field {
  max-width: none;
}
</style>
