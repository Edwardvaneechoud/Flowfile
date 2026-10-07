<template>
  <div class="formula-settings">
    <!-- Several entries: applied top to bottom, each can read the columns above it -->
    <ol v-if="entries.length > 1" class="entry-list" aria-label="Formulas, applied in order">
      <li
        v-for="(entry, index) in entries"
        :key="uids[index]"
        :class="['entry-item', { active: index === activeIndex }]"
      >
        <button
          type="button"
          class="entry-select"
          :aria-current="index === activeIndex ? 'true' : undefined"
          @click="activeIndex = index"
        >
          <span class="entry-position">{{ index + 1 }}</span>
          <span class="entry-name">{{ entry.field.name || 'unnamed' }}</span>
          <span class="entry-expression">{{ entry.function.trim() || 'empty' }}</span>
        </button>
        <span class="entry-actions">
          <button
            type="button"
            class="entry-action"
            :disabled="index === 0"
            :aria-label="`Move formula ${index + 1} up`"
            @click="moveEntry(index, -1)"
          ><i class="fas fa-arrow-up" /></button>
          <button
            type="button"
            class="entry-action"
            :disabled="index === entries.length - 1"
            :aria-label="`Move formula ${index + 1} down`"
            @click="moveEntry(index, 1)"
          ><i class="fas fa-arrow-down" /></button>
          <button
            type="button"
            class="entry-action entry-remove"
            :aria-label="`Remove formula ${index + 1}`"
            @click="removeEntry(index)"
          ><i class="fas fa-times" /></button>
        </span>
      </li>
    </ol>

    <!-- Output field + data type (mirrors the desktop selector-container) -->
    <div class="selector-container">
      <div class="selector-field">
        <label>Output field</label>
        <input
          v-model="active.field.name"
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
        <select v-model="active.field.data_type" class="ff-input" @change="emitUpdate">
          <option v-for="t in DATA_TYPES" :key="t" :value="t">{{ t }}</option>
        </select>
      </div>
    </div>

    <ExpressionEditor
      :key="uids[activeIndex]"
      :node-id="props.nodeId"
      :model-value="active.function"
      :extra-columns="earlierOutputs"
      @update:model-value="handleFormulaChange"
    />

    <button type="button" class="add-entry" @click="addEntry">+ Add formula</button>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { NodeFormulaSettings, ColumnSchema, FunctionInput } from '../../types'
import ExpressionEditor from '../common/ExpressionEditor.vue'
import { formulaEntries, withFormulaEntries } from '../../utils/formulaEntries'

const DATA_TYPES = ['Auto', 'String', 'Int64', 'Float64', 'Boolean', 'Date', 'Datetime']

const props = defineProps<{
  nodeId: number
  settings: NodeFormulaSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeFormulaSettings): void
}>()

const flowStore = useFlowStore()

function blankEntry(): FunctionInput {
  return { field: { name: '', data_type: 'Auto' }, function: '' }
}

function copyEntry(entry: FunctionInput): FunctionInput {
  return {
    field: { name: entry.field?.name ?? '', data_type: entry.field?.data_type || 'Auto' },
    function: entry.function ?? '',
  }
}

const initial = formulaEntries(props.settings).map(copyEntry)
const entries = ref<FunctionInput[]>(initial.length ? initial : [blankEntry()])
let nextUid = 0
const uids = ref<number[]>(entries.value.map(() => nextUid++))
const activeIndex = ref(0)

const active = computed(() => entries.value[Math.min(activeIndex.value, entries.value.length - 1)])

// Columns the entries above the active one produce, so it can reference them.
const earlierOutputs = computed<ColumnSchema[]>(() =>
  entries.value
    .slice(0, activeIndex.value)
    .filter(e => e.field.name.trim() && e.function.trim())
    .map(e => ({ name: e.field.name.trim(), data_type: e.field.data_type || 'Auto' }))
)

const columns = computed<ColumnSchema[]>(() => {
  const input = flowStore.getNodeInputSchema(props.nodeId)
  return [...input, ...earlierOutputs.value.filter(c => !input.some(i => i.name === c.name))]
})

function handleFormulaChange(value: string) {
  active.value.function = value
  emitUpdate()
}

function addEntry() {
  entries.value.push(blankEntry())
  uids.value.push(nextUid++)
  activeIndex.value = entries.value.length - 1
  emitUpdate()
}

function removeEntry(index: number) {
  entries.value.splice(index, 1)
  uids.value.splice(index, 1)
  if (activeIndex.value >= entries.value.length) activeIndex.value = entries.value.length - 1
  else if (activeIndex.value > index) activeIndex.value -= 1
  emitUpdate()
}

function moveEntry(index: number, delta: number) {
  const target = index + delta
  if (target < 0 || target >= entries.value.length) return
  for (const list of [entries.value, uids.value] as unknown[][]) {
    const [item] = list.splice(index, 1)
    list.splice(target, 0, item)
  }
  if (activeIndex.value === index) activeIndex.value = target
  else if (activeIndex.value === target) activeIndex.value = index
  emitUpdate()
}

function emitUpdate() {
  const out = entries.value.map(e => ({
    field: { name: e.field.name.trim(), data_type: e.field.data_type || 'Auto' },
    function: e.function,
  }))
  const live = out.filter(e => e.function.trim())
  emit('update:settings', {
    ...withFormulaEntries(props.settings, out),
    is_setup: live.length > 0 && live.every(e => e.field.name.length > 0),
  })
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

.entry-list {
  display: flex;
  flex-direction: column;
  gap: 4px;
  margin: 0;
  padding: 0;
  list-style: none;
}

.entry-item {
  display: flex;
  align-items: center;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  background-color: var(--color-background-primary);
}

.entry-item.active {
  border-color: var(--color-border-focus);
  background-color: var(--color-background-selected);
}

.entry-select {
  display: flex;
  align-items: baseline;
  gap: 8px;
  flex: 1;
  min-width: 0;
  padding: 6px 8px;
  font-size: 12px;
  text-align: left;
  color: var(--color-text-primary);
  background: none;
  border: none;
  cursor: pointer;
}

.entry-position {
  flex: 0 0 auto;
  color: var(--color-text-muted);
  font-variant-numeric: tabular-nums;
}

.entry-name {
  flex: 0 0 auto;
  max-width: 40%;
  overflow: hidden;
  font-weight: 600;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.entry-expression {
  flex: 1;
  min-width: 0;
  overflow: hidden;
  font-family: var(--font-mono, monospace);
  color: var(--color-text-secondary);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.entry-actions {
  display: flex;
  flex: 0 0 auto;
  padding-right: 4px;
}

.entry-action {
  padding: 4px 6px;
  font-size: 11px;
  color: var(--color-text-muted);
  background: none;
  border: none;
  border-radius: 3px;
  cursor: pointer;
}

.entry-action:hover:not(:disabled) {
  color: var(--color-text-primary);
  background-color: var(--color-background-hover);
}

.entry-action:disabled {
  opacity: 0.35;
  cursor: default;
}

.entry-remove:hover:not(:disabled) {
  color: var(--color-danger);
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

.add-entry {
  align-self: flex-start;
  padding: 6px 12px;
  font-size: 12px;
  color: var(--color-text-secondary);
  background: none;
  border: 1px dashed var(--color-border-primary);
  border-radius: 4px;
  cursor: pointer;
}

.add-entry:hover {
  color: var(--color-accent);
  border-color: var(--color-accent);
}
</style>
