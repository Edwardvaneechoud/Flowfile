<template>
  <div class="settings-pane">
    <p class="settings-note">
      Cross join pairs every row of the left input with every row of the right input
      (cartesian product). No join keys are needed.
    </p>

    <div class="field">
      <label>Suffix for overlapping right columns</label>
      <input
        v-model="rightSuffix"
        type="text"
        class="text-input"
        placeholder="_right"
        @input="emitUpdate"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import type { NodeCrossJoinSettings } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: NodeCrossJoinSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeCrossJoinSettings): void
}>()

const rightSuffix = ref(props.settings.cross_join_input?.right_suffix || '_right')

function emitUpdate() {
  emit('update:settings', {
    ...props.settings,
    is_setup: true,
    cross_join_input: { right_suffix: rightSuffix.value || '_right' },
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
