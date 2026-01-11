<template>
  <div class="listbox-wrapper">
    <div class="form-group">
      <label>Sample size</label>
      <input
        type="number"
        v-model.number="sampleSize"
        @input="emitUpdate"
        min="1"
        class="input"
        style="text-align: center; font-size: 16px;"
      />
    </div>

    <div style="margin-top: 12px;">
      <div class="help-text">Quick select:</div>
      <div style="display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px;">
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

const sampleSize = ref(props.settings.sample_size || props.settings.head_input?.n || 10)
const presets = [5, 10, 25, 50, 100, 500, 1000]

watch(() => props.settings, (newSettings) => {
  if (newSettings.sample_size !== undefined) {
    sampleSize.value = newSettings.sample_size
  } else if (newSettings.head_input?.n !== undefined) {
    sampleSize.value = newSettings.head_input.n
  }
}, { deep: true })

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
/* Component uses global styles from main.css */
</style>
