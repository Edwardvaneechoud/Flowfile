<template>
  <div class="settings-form">
    <div class="form-group">
      <label>Join Type</label>
      <select v-model="joinType" @change="emitUpdate" class="select">
        <option value="inner">Inner Join</option>
        <option value="left">Left Join</option>
        <option value="right">Right Join</option>
        <option value="outer">Outer Join</option>
        <option value="cross">Cross Join</option>
        <option value="semi">Semi Join</option>
        <option value="anti">Anti Join</option>
      </select>
    </div>

    <div class="join-columns">
      <div class="column-side">
        <label>Left Table (Input 1)</label>
        <div v-if="leftColumns.length === 0" class="no-columns">
          Connect left input
        </div>
        <div v-else class="column-list">
          <div
            v-for="col in leftColumns"
            :key="col.name"
            class="column-chip"
            :class="{ selected: isLeftSelected(col.name) }"
            @click="selectLeftColumn(col.name)"
          >
            {{ col.name }}
          </div>
        </div>
      </div>

      <div class="column-side">
        <label>Right Table (Input 2)</label>
        <div v-if="rightColumns.length === 0" class="no-columns">
          Connect right input
        </div>
        <div v-else class="column-list">
          <div
            v-for="col in rightColumns"
            :key="col.name"
            class="column-chip"
            :class="{ selected: isRightSelected(col.name) }"
            @click="selectRightColumn(col.name)"
          >
            {{ col.name }}
          </div>
        </div>
      </div>
    </div>

    <div class="form-group">
      <label>Join Mappings</label>
    </div>

    <div v-if="joinMapping.length === 0" class="no-mappings">
      Click columns above to create join mappings
    </div>

    <div v-else class="mapping-list">
      <div v-for="(mapping, idx) in joinMapping" :key="idx" class="mapping-item">
        <span class="mapping-col">{{ mapping.left_col }}</span>
        <span class="mapping-arrow">=</span>
        <span class="mapping-col">{{ mapping.right_col }}</span>
        <button class="remove-btn" @click="removeMapping(idx)">×</button>
      </div>
    </div>

    <div class="suffix-group">
      <div class="form-group">
        <label>Left Suffix</label>
        <input type="text" v-model="leftSuffix" @input="emitUpdate" class="input" />
      </div>
      <div class="form-group">
        <label>Right Suffix</label>
        <input type="text" v-model="rightSuffix" @input="emitUpdate" class="input" />
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

const joinType = ref<JoinType>(props.settings.join_input?.join_type || 'inner')
const joinMapping = ref<JoinMapping[]>(props.settings.join_input?.join_mapping || [])
const leftSuffix = ref(props.settings.join_input?.left_suffix || '_left')
const rightSuffix = ref(props.settings.join_input?.right_suffix || '_right')
const selectedLeft = ref<string | null>(null)
const selectedRight = ref<string | null>(null)

const leftColumns = computed<ColumnSchema[]>(() => {
  return flowStore.getLeftInputSchema(props.nodeId)
})

const rightColumns = computed<ColumnSchema[]>(() => {
  return flowStore.getRightInputSchema(props.nodeId)
})

watch(() => props.settings.join_input, (newInput) => {
  if (newInput) {
    joinType.value = newInput.join_type || 'inner'
    joinMapping.value = [...(newInput.join_mapping || [])]
    leftSuffix.value = newInput.left_suffix || '_left'
    rightSuffix.value = newInput.right_suffix || '_right'
  }
}, { deep: true })

function isLeftSelected(name: string) {
  return selectedLeft.value === name
}

function isRightSelected(name: string) {
  return selectedRight.value === name
}

function selectLeftColumn(name: string) {
  selectedLeft.value = name
  tryAddMapping()
}

function selectRightColumn(name: string) {
  selectedRight.value = name
  tryAddMapping()
}

function tryAddMapping() {
  if (selectedLeft.value && selectedRight.value) {
    joinMapping.value.push({
      left_col: selectedLeft.value,
      right_col: selectedRight.value
    })
    selectedLeft.value = null
    selectedRight.value = null
    emitUpdate()
  }
}

function removeMapping(index: number) {
  joinMapping.value.splice(index, 1)
  emitUpdate()
}

function emitUpdate() {
  const settings: JoinSettings = {
    ...props.settings,
    is_setup: joinMapping.value.length > 0,
    join_input: {
      join_type: joinType.value,
      join_mapping: [...joinMapping.value],
      left_suffix: leftSuffix.value,
      right_suffix: rightSuffix.value
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

.select, .input {
  padding: 8px 12px;
  font-size: 13px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
}

.join-columns {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
}

.column-side label {
  display: block;
  font-size: 12px;
  font-weight: 500;
  margin-bottom: 8px;
}

.no-columns, .no-mappings {
  padding: 12px;
  text-align: center;
  color: var(--text-secondary);
  background: var(--bg-tertiary);
  border-radius: var(--radius-sm);
  font-size: 12px;
}

.column-list {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  max-height: 120px;
  overflow-y: auto;
}

.column-chip {
  padding: 4px 8px;
  font-size: 12px;
  background: var(--bg-tertiary);
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: all 0.15s;
}

.column-chip:hover {
  background: var(--border-color);
}

.column-chip.selected {
  background: var(--accent-color);
  color: white;
}

.mapping-list {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.mapping-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  background: var(--bg-tertiary);
  border-radius: var(--radius-sm);
}

.mapping-col {
  font-size: 13px;
  font-weight: 500;
}

.mapping-arrow {
  color: var(--text-secondary);
}

.remove-btn {
  margin-left: auto;
  background: none;
  border: none;
  color: var(--error-color);
  font-size: 18px;
  cursor: pointer;
  padding: 0 4px;
}

.suffix-group {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}
</style>
