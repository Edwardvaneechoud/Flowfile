<template>
  <div class="settings-form">
    <div class="form-group">
      <label>Number of Rows</label>
      <div class="help-text">Limit the output to the first N rows</div>
    </div>

    <div class="form-group">
      <input
        type="number"
        v-model.number="n"
        @input="emitUpdate"
        min="1"
        class="input"
        placeholder="10"
      />
    </div>

    <div class="presets">
      <span class="preset-label">Quick select:</span>
      <button
        v-for="preset in presets"
        :key="preset"
        class="preset-btn"
        :class="{ active: n === preset }"
        @click="setN(preset)"
      >
        {{ preset }}
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'
import type { HeadSettings } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: HeadSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: HeadSettings): void
}>()

const n = ref(props.settings.head_input?.n || 10)
const presets = [5, 10, 25, 50, 100, 500, 1000]

watch(() => props.settings.head_input?.n, (newN) => {
  if (newN !== undefined) {
    n.value = newN
  }
}, { deep: true })

function setN(value: number) {
  n.value = value
  emitUpdate()
}

function emitUpdate() {
  const settings: HeadSettings = {
    ...props.settings,
    is_setup: true,
    head_input: {
      n: n.value
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

.input {
  width: 100%;
  padding: 12px 16px;
  font-size: 16px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
  text-align: center;
}

.input:focus {
  outline: none;
  border-color: var(--accent-color);
}

.presets {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
}

.preset-label {
  font-size: 12px;
  color: var(--text-secondary);
}

.preset-btn {
  padding: 6px 12px;
  font-size: 12px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
  cursor: pointer;
  transition: all 0.15s;
}

.preset-btn:hover {
  border-color: var(--accent-color);
  background: var(--accent-light);
}

.preset-btn.active {
  background: var(--accent-color);
  color: white;
  border-color: var(--accent-color);
}
</style>
