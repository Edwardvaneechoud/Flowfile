<template>
  <div class="settings-pane">
    <p class="settings-note">
      Adds a sequential row-number column to the start of the table.
    </p>

    <div class="field">
      <label>Column name</label>
      <input
        v-model="name"
        type="text"
        class="text-input"
        placeholder="record_id"
        @input="emitUpdate"
      />
    </div>

    <div class="field">
      <label>Start from</label>
      <input
        v-model.number="offset"
        type="number"
        class="text-input"
        @input="emitUpdate"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import type { NodeRecordIdSettings } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: NodeRecordIdSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeRecordIdSettings): void
}>()

const name = ref(props.settings.record_id_input?.name || 'record_id')
const offset = ref<number>(props.settings.record_id_input?.offset ?? 1)

function emitUpdate() {
  emit('update:settings', {
    ...props.settings,
    is_setup: name.value.trim().length > 0,
    record_id_input: {
      name: name.value.trim() || 'record_id',
      offset: Number.isFinite(offset.value) ? offset.value : 1,
    },
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

.field {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.field label {
  font-size: 10px;
  color: #6272a4;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.text-input {
  padding: 6px 8px;
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
