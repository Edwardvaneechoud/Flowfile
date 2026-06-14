<template>
  <div class="settings-pane">
    <p class="settings-note">Rename columns. Leave a field unchanged to keep its name.</p>

    <div v-if="columns.length === 0" class="no-columns">
      No input columns available. Connect an input node first.
    </div>

    <div v-else class="rename-table">
      <div class="rename-head">
        <span>Original</span>
        <span>New name</span>
      </div>
      <div v-for="col in columns" :key="col.name" class="rename-row">
        <span class="original" :title="col.data_type">{{ col.name }}</span>
        <input
          v-model="newNames[col.name]"
          type="text"
          class="text-input"
          :placeholder="col.name"
          @input="emitUpdate"
        />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { NodeRenameSettings, ColumnSchema } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: NodeRenameSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeRenameSettings): void
}>()

const flowStore = useFlowStore()
const columns = computed<ColumnSchema[]>(() => flowStore.getNodeInputSchema(props.nodeId))

// old_name -> new_name (seeded from saved settings)
const newNames = ref<Record<string, string>>(
  Object.fromEntries((props.settings.rename_input || []).map(r => [r.old_name, r.new_name]))
)

// Seed empty entries for input columns so v-model binds cleanly.
watch(
  columns,
  cols => {
    for (const col of cols) {
      if (!(col.name in newNames.value)) newNames.value[col.name] = ''
    }
  },
  { immediate: true }
)

function emitUpdate() {
  const renameInput = columns.value
    .map(col => ({ old_name: col.name, new_name: (newNames.value[col.name] || '').trim() }))
    .filter(r => r.new_name && r.new_name !== r.old_name)
  emit('update:settings', {
    ...props.settings,
    is_setup: renameInput.length > 0,
    rename_input: renameInput,
  })
}
</script>

<style scoped>
.settings-pane {
  padding: 12px;
  display: flex;
  flex-direction: column;
  gap: 12px;
  color: var(--color-text-primary);
}

.settings-note {
  font-size: 12px;
  color: var(--color-text-tertiary);
  line-height: 1.5;
  margin: 0;
}

.no-columns {
  font-size: 12px;
  color: var(--color-text-muted);
}

.rename-table {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.rename-head {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10px;
  font-size: 11px;
  font-weight: 600;
  color: var(--color-text-secondary);
}

.rename-row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10px;
  align-items: center;
}

.original {
  font-size: 12px;
  color: var(--color-text-primary);
  font-family: var(--font-family-mono, 'Monaco', 'Menlo', monospace);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.text-input {
  padding: 5px 8px;
  font-size: 13px;
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
}

.text-input:focus {
  outline: none;
  border-color: var(--color-border-focus);
  box-shadow: 0 0 0 2px var(--color-focus-ring-accent);
}
</style>
