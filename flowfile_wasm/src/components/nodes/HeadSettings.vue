<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">Settings</div>
    <div class="settings-row">
      <span class="settings-label">Sample size</span>
      <input
        type="number"
        :value="sampleSize"
        @input="updateSize(($event.target as HTMLInputElement).valueAsNumber)"
        min="1"
        class="input settings-input"
      />
    </div>
    <div class="presets-row">
      <button
        v-for="preset in presets"
        :key="preset"
        class="btn btn-small"
        :class="sampleSize === preset ? 'btn-primary' : 'btn-secondary'"
        @click="setSize(preset)"
      >
        {{ preset }}
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import type { HeadSettings } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: HeadSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: HeadSettings): void
}>()

// Initialize directly from props
const sampleSize = ref(props.settings.sample_size || props.settings.head_input?.n || 1000)
const presets = [10, 100, 500, 1000, 5000, 10000]

function updateSize(value: number) {
  if (!isNaN(value) && value > 0) {
    sampleSize.value = value
    emitUpdate()
  }
}

function setSize(value: number) {
  sampleSize.value = value
  emitUpdate()
}

function emitUpdate() {
  const settings: HeadSettings = {
    ...props.settings,
    is_setup: true,
    sample_size: sampleSize.value,
    head_input: {
      n: sampleSize.value
    }
  }
  emit('update:settings', settings)
}
</script>

<style scoped>
.settings-row {
  display: flex;
  align-items: center;
  padding: 8px 12px;
  gap: 12px;
}

.settings-label {
  font-size: 13px;
  color: var(--text-primary);
  min-width: 80px;
}

.settings-input {
  width: 120px;
  text-align: center;
}

.presets-row {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  padding: 8px 12px;
  border-top: 1px solid var(--border-light);
}
</style>
