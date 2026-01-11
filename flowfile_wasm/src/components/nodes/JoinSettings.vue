<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">Join Settings</div>

    <div class="settings-section">
      <div class="settings-row">
        <span class="settings-label">Join Type</span>
        <select :value="joinType" @change="updateJoinType(($event.target as HTMLSelectElement).value as JoinType)" class="select">
          <option value="inner">inner</option>
          <option value="left">left</option>
          <option value="right">right</option>
          <option value="full">full</option>
          <option value="semi">semi</option>
          <option value="anti">anti</option>
          <option value="cross">cross</option>
        </select>
      </div>
    </div>

    <div class="listbox-subtitle" style="margin-top: 12px;">Join Columns</div>

    <div v-if="leftColumns.length === 0 || rightColumns.length === 0" class="help-text">
      Run upstream nodes first to see available columns.
    </div>

    <div v-else class="join-columns-section">
      <div
        v-for="(mapping, index) in joinMapping"
        :key="index"
        class="join-row"
      >
        <div class="join-column-pair">
          <div class="column-select-group">
            <label class="column-label">Left</label>
            <select :value="mapping.left_col" @change="updateLeftCol(index, ($event.target as HTMLSelectElement).value)" class="select">
              <option value="">Select...</option>
              <option v-for="col in leftColumns" :key="col.name" :value="col.name">
                {{ col.name }}
              </option>
            </select>
          </div>
          <span class="join-equals">=</span>
          <div class="column-select-group">
            <label class="column-label">Right</label>
            <select :value="mapping.right_col" @change="updateRightCol(index, ($event.target as HTMLSelectElement).value)" class="select">
              <option value="">Select...</option>
              <option v-for="col in rightColumns" :key="col.name" :value="col.name">
                {{ col.name }}
              </option>
            </select>
          </div>
          <button
            v-if="joinMapping.length > 1"
            class="remove-btn"
            @click="removeMapping(index)"
            title="Remove"
          >×</button>
        </div>
      </div>

      <button class="add-condition-btn" @click="addJoinCondition">
        + Add join condition
      </button>
    </div>

    <div class="settings-section" style="margin-top: 12px;">
      <div class="settings-row">
        <span class="settings-label">Left Suffix</span>
        <input type="text" :value="leftSuffix" @input="updateLeftSuffix(($event.target as HTMLInputElement).value)" class="input settings-input" />
      </div>
      <div class="settings-row">
        <span class="settings-label">Right Suffix</span>
        <input type="text" :value="rightSuffix" @input="updateRightSuffix(($event.target as HTMLInputElement).value)" class="input settings-input" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { JoinSettings, JoinType, JoinMapping, ColumnSchema } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: JoinSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: JoinSettings): void
}>()

const flowStore = useFlowStore()

// Initialize directly from props
const joinType = ref<JoinType>(props.settings.join_input?.join_type || props.settings.join_input?.how || 'inner')
const joinMapping = ref<JoinMapping[]>(
  props.settings.join_input?.join_mapping && props.settings.join_input.join_mapping.length > 0
    ? props.settings.join_input.join_mapping.map(m => ({ left_col: m.left_col, right_col: m.right_col }))
    : [{ left_col: '', right_col: '' }] // Start with one empty row
)
const leftSuffix = ref(props.settings.join_input?.left_suffix || '_left')
const rightSuffix = ref(props.settings.join_input?.right_suffix || '_right')

const leftColumns = computed<ColumnSchema[]>(() => {
  return flowStore.getLeftInputSchema(props.nodeId)
})

const rightColumns = computed<ColumnSchema[]>(() => {
  return flowStore.getRightInputSchema(props.nodeId)
})

// Ensure at least one row when columns become available
watch([leftColumns, rightColumns], () => {
  if (leftColumns.value.length > 0 && rightColumns.value.length > 0 && joinMapping.value.length === 0) {
    joinMapping.value = [{ left_col: '', right_col: '' }]
  }
})

function updateJoinType(value: JoinType) {
  joinType.value = value
  emitUpdate()
}

function updateLeftCol(index: number, value: string) {
  joinMapping.value[index].left_col = value
  emitUpdate()
}

function updateRightCol(index: number, value: string) {
  joinMapping.value[index].right_col = value
  emitUpdate()
}

function updateLeftSuffix(value: string) {
  leftSuffix.value = value
  emitUpdate()
}

function updateRightSuffix(value: string) {
  rightSuffix.value = value
  emitUpdate()
}

function addJoinCondition() {
  joinMapping.value.push({
    left_col: '',
    right_col: ''
  })
}

function removeMapping(index: number) {
  joinMapping.value.splice(index, 1)
  if (joinMapping.value.length === 0) {
    joinMapping.value = [{ left_col: '', right_col: '' }]
  }
  emitUpdate()
}

function emitUpdate() {
  const validMappings = joinMapping.value.filter(m => m.left_col && m.right_col)
  const settings: JoinSettings = {
    ...props.settings,
    is_setup: validMappings.length > 0,
    join_input: {
      join_type: joinType.value,
      how: joinType.value,
      join_mapping: joinMapping.value.map(m => ({ left_col: m.left_col, right_col: m.right_col })),
      left_suffix: leftSuffix.value,
      right_suffix: rightSuffix.value
    }
  }
  emit('update:settings', settings)
}
</script>

<style scoped>
.settings-section {
  padding: 8px 12px;
}

.settings-row {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
}

.settings-label {
  font-size: 12px;
  color: var(--text-secondary);
  min-width: 80px;
}

.settings-input {
  flex: 1;
  max-width: 120px;
}

.help-text {
  padding: 12px;
  color: var(--text-muted);
  font-size: 12px;
  font-style: italic;
}

.join-columns-section {
  padding: 8px 12px;
}

.join-row {
  margin-bottom: 8px;
}

.join-column-pair {
  display: flex;
  align-items: flex-end;
  gap: 8px;
}

.column-select-group {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.column-label {
  font-size: 10px;
  color: var(--text-muted);
  text-transform: uppercase;
}

.column-select-group .select {
  width: 100%;
}

.join-equals {
  color: var(--text-muted);
  font-size: 14px;
  padding-bottom: 6px;
}

.remove-btn {
  background: none;
  border: none;
  color: var(--text-muted);
  font-size: 18px;
  cursor: pointer;
  padding: 4px 8px;
  margin-bottom: 2px;
}

.remove-btn:hover {
  color: #ff5555;
}

.add-condition-btn {
  background: none;
  border: 1px dashed var(--border-color);
  color: var(--text-secondary);
  padding: 6px 12px;
  border-radius: 4px;
  cursor: pointer;
  font-size: 12px;
  width: 100%;
  margin-top: 4px;
}

.add-condition-btn:hover {
  border-color: var(--accent-color);
  color: var(--accent-color);
}
</style>
