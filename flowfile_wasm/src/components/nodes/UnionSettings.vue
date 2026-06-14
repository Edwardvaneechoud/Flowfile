<template>
  <div class="settings-pane">
    <p class="settings-note">
      Union stacks the rows of all connected inputs into one table.
    </p>

    <div class="field">
      <label>Column matching</label>
      <select v-model="mode" class="select-sm" @change="emitUpdate">
        <option value="vertical">Matching columns (vertical)</option>
        <option value="diagonal">Union of columns (diagonal)</option>
      </select>
      <span class="hint">
        {{ mode === 'vertical'
          ? 'Inputs must share the same columns; types are widened to a common type.'
          : 'Takes the union of all columns, filling missing values with null.' }}
      </span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import type { NodeUnionSettings, UnionMode } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: NodeUnionSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeUnionSettings): void
}>()

const mode = ref<UnionMode>(props.settings.union_input?.mode || 'diagonal')

function emitUpdate() {
  emit('update:settings', {
    ...props.settings,
    is_setup: true,
    union_input: { mode: mode.value },
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

.select-sm {
  padding: 6px 8px;
  font-size: 12px;
  background: #282c34;
  color: #abb2bf;
  border: 1px solid #3e4451;
  border-radius: 3px;
}

.select-sm:focus {
  outline: none;
  border-color: #8be9fd;
}

.hint {
  font-size: 11px;
  color: #6272a4;
  line-height: 1.4;
}
</style>
