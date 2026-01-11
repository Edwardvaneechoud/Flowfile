<template>
  <div class="listbox-wrapper">
    <div v-if="columns.length === 0" class="no-columns">
      No input columns available. Connect an input node first.
    </div>

    <template v-else>
      <ul class="listbox">
        <li
          v-for="col in columnsWithSelection"
          :key="col.name"
          :class="{ 'is-selected': col.isSelected }"
          @click="toggleColumn(col.name)"
        >
          <label class="checkbox-label" style="width: 100%; justify-content: space-between;">
            <span style="display: flex; align-items: center; gap: 8px;">
              <input type="checkbox" :checked="col.isSelected" @change="toggleColumn(col.name)" @click.stop />
              <span>{{ col.name }}</span>
            </span>
            <span class="column-type">{{ col.data_type }}</span>
          </label>
        </li>
      </ul>
    </template>

    <div style="margin-top: 12px;">
      <div class="form-group">
        <label>Keep Strategy</label>
        <select :value="keep" @change="updateKeep(($event.target as HTMLSelectElement).value as 'first' | 'last' | 'any' | 'none')" class="select">
          <option value="first">First occurrence</option>
          <option value="last">Last occurrence</option>
          <option value="any">Any occurrence</option>
          <option value="none">Remove all duplicates</option>
        </select>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { UniqueSettings, ColumnSchema } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: UniqueSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: UniqueSettings): void
}>()

const flowStore = useFlowStore()

const subset = ref<string[]>(
  props.settings.unique_input?.subset ||
  props.settings.unique_input?.columns ||
  []
)
const keep = ref<'first' | 'last' | 'any' | 'none'>(
  props.settings.unique_input?.keep ||
  props.settings.unique_input?.strategy ||
  'any'
)
const maintainOrder = ref(props.settings.unique_input?.maintain_order ?? true)

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

const columnsWithSelection = computed(() => {
  return columns.value.map(col => ({
    name: col.name,
    data_type: col.data_type,
    isSelected: subset.value.includes(col.name)
  }))
})

function toggleColumn(name: string) {
  const idx = subset.value.indexOf(name)
  if (idx === -1) {
    subset.value.push(name)
  } else {
    subset.value.splice(idx, 1)
  }
  emitUpdate()
}

function updateKeep(value: 'first' | 'last' | 'any' | 'none') {
  keep.value = value
  emitUpdate()
}

function emitUpdate() {
  const settings: UniqueSettings = {
    ...props.settings,
    is_setup: true,
    unique_input: {
      subset: [...subset.value],
      keep: keep.value,
      maintain_order: maintainOrder.value
    }
  }
  emit('update:settings', settings)
}
</script>

<style scoped>
/* Component uses global styles from main.css */
</style>
