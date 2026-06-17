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
  color: var(--color-text-primary);
}

.settings-note {
  font-size: 12px;
  color: var(--color-text-tertiary);
  line-height: 1.5;
  margin: 0;
}

.field {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.field label {
  font-size: 11px;
  font-weight: 600;
  color: var(--color-text-secondary);
}

.select-sm {
  padding: 6px 8px;
  font-size: 13px;
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
}

.select-sm:focus {
  outline: none;
  border-color: var(--color-border-focus);
  box-shadow: 0 0 0 2px var(--color-focus-ring-accent);
}

.hint {
  font-size: 11px;
  color: var(--color-text-tertiary);
  line-height: 1.4;
}
</style>
