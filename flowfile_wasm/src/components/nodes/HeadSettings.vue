<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">Settings</div>
    <div class="settings-row">
      <span class="settings-label">Method</span>
      <select
        :value="sampleMethod"
        @change="updateMethod(($event.target as HTMLSelectElement).value as SampleMethod)"
        class="input settings-input"
      >
        <option value="first">First rows</option>
        <option value="random">Random rows</option>
        <option value="random_fraction">Random %</option>
      </select>
    </div>

    <div v-if="sampleMethod === 'random_fraction'" class="settings-row">
      <span class="settings-label">Percentage</span>
      <input
        type="number"
        :value="fraction"
        @input="updateFraction(($event.target as HTMLInputElement).valueAsNumber)"
        min="0"
        max="100"
        class="input settings-input"
      />
    </div>
    <template v-else>
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
    </template>

    <div v-if="sampleMethod !== 'first'" class="settings-row">
      <span class="settings-label">Seed</span>
      <input
        type="number"
        :value="seed ?? ''"
        @input="updateSeed(($event.target as HTMLInputElement).value)"
        placeholder="random"
        class="input settings-input"
      />
    </div>
    <div v-if="sampleMethod !== 'first'" class="settings-hint">
      Leave the seed blank to draw a fresh sample on every run.
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import type { HeadSettings, SampleMethod } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: HeadSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: HeadSettings): void
}>()

const sampleSize = ref(props.settings.sample_size || props.settings.head_input?.n || 1000)
const sampleMethod = ref<SampleMethod>(props.settings.sample_method || 'first')
const fraction = ref(props.settings.fraction ?? 10)
const seed = ref<number | null>(props.settings.seed ?? null)
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

function updateMethod(value: SampleMethod) {
  sampleMethod.value = value
  emitUpdate()
}

function updateFraction(value: number) {
  if (!isNaN(value) && value > 0 && value <= 100) {
    fraction.value = value
    emitUpdate()
  }
}

function updateSeed(value: string) {
  seed.value = value === '' ? null : Number(value)
  emitUpdate()
}

function emitUpdate() {
  const settings: HeadSettings = {
    ...props.settings,
    is_setup: true,
    sample_method: sampleMethod.value,
    sample_size: sampleSize.value,
    fraction: fraction.value,
    seed: seed.value,
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

.settings-hint {
  padding: 0 12px 8px;
  font-size: 11px;
  color: var(--text-secondary);
}
</style>
