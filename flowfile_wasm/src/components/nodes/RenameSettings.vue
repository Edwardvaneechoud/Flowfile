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
}

.settings-note {
  font-size: 12px;
  color: #6272a4;
  line-height: 1.5;
  margin: 0;
}

.no-columns {
  font-size: 12px;
  color: #6272a4;
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
  font-size: 10px;
  color: #6272a4;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.rename-row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10px;
  align-items: center;
}

.original {
  font-size: 12px;
  color: #abb2bf;
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.text-input {
  padding: 5px 8px;
  font-size: 12px;
  background: #282c34;
  color: #abb2bf;
  border: 1px solid #3e4451;
  border-radius: 3px;
}

.text-input:focus {
  outline: none;
  border-color: #8be9fd;
}
</style>
