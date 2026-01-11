<template>
  <div class="settings-form">
    <div class="form-group">
      <label>Unique Columns (Subset)</label>
      <div class="help-text">Select columns to check for uniqueness. Leave empty for all columns.</div>
    </div>

    <div v-if="columns.length === 0" class="no-columns">
      No input columns available. Connect an input node first.
    </div>

    <div v-else class="column-list">
      <div
        v-for="col in columns"
        :key="col.name"
        class="column-item"
        :class="{ selected: subset.includes(col.name) }"
        @click="toggleColumn(col.name)"
      >
        <input type="checkbox" :checked="subset.includes(col.name)" @click.stop />
        <span class="column-name">{{ col.name }}</span>
        <span class="column-type">{{ col.data_type }}</span>
      </div>
    </div>

    <div class="form-group">
      <label>Keep Strategy</label>
      <select v-model="keep" @change="emitUpdate" class="select">
        <option value="first">First occurrence</option>
        <option value="last">Last occurrence</option>
        <option value="any">Any occurrence</option>
        <option value="none">Remove all duplicates</option>
      </select>
    </div>

    <div class="form-group">
      <label class="checkbox-label">
        <input type="checkbox" v-model="maintainOrder" @change="emitUpdate" />
        <span>Maintain original order</span>
      </label>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { UniqueSettings, ColumnSchema } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: UniqueSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: UniqueSettings): void
}>()

const flowStore = useFlowStore()

const subset = ref<string[]>(props.settings.unique_input?.subset || [])
const keep = ref<'first' | 'last' | 'any' | 'none'>(props.settings.unique_input?.keep || 'first')
const maintainOrder = ref(props.settings.unique_input?.maintain_order ?? true)

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

watch(() => props.settings.unique_input, (newInput) => {
  if (newInput) {
    subset.value = [...(newInput.subset || [])]
    keep.value = newInput.keep || 'first'
    maintainOrder.value = newInput.maintain_order ?? true
  }
}, { deep: true })

function toggleColumn(name: string) {
  const idx = subset.value.indexOf(name)
  if (idx === -1) {
    subset.value.push(name)
  } else {
    subset.value.splice(idx, 1)
  }
  emitUpdate()
}

function emitUpdate() {
  const settings: UniqueSettings = {
    ...props.settings,
    is_setup: true,
    unique_input: {
      subset: [...subset.value],
      keep: keep.value,
      maintain_order: maintainOrder.value
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

.column-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
  max-height: 250px;
  overflow-y: auto;
  border: 1px solid var(--border-light);
  border-radius: var(--radius-sm);
}

.column-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  cursor: pointer;
  transition: background 0.15s;
}

.column-item:hover {
  background: var(--bg-hover);
}

.column-item.selected {
  background: var(--accent-light);
}

.column-item input {
  width: 16px;
  height: 16px;
}

.column-name {
  flex: 1;
  font-size: 13px;
}

.column-type {
  font-size: 11px;
  color: var(--text-secondary);
}

.select {
  padding: 8px 12px;
  font-size: 13px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
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
</style>
