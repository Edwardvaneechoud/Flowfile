<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { useFlowStore } from '../../stores/flow-store'
import type { DatasetKind, VizSourceDescriptor } from '../../types/visuals'

const props = defineProps<{ modelValue: boolean }>()
const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  picked: [source: VizSourceDescriptor]
}>()

const flowStore = useFlowStore()
const { catalogDatasets, externalDatasets } = storeToRefs(flowStore)

interface Option {
  name: string
  kind: DatasetKind
}

const options = computed<Option[]>(() => {
  const out: Option[] = []
  for (const name of catalogDatasets.value.keys()) out.push({ name, kind: 'catalog' })
  for (const name of externalDatasets.value.keys()) out.push({ name, kind: 'external' })
  return out
})

const search = ref('')
const filtered = computed(() => {
  const q = search.value.trim().toLowerCase()
  return q ? options.value.filter((o) => o.name.toLowerCase().includes(q)) : options.value
})

watch(
  () => props.modelValue,
  (open) => {
    if (open) search.value = ''
  },
)

function close() {
  emit('update:modelValue', false)
}

function pick(opt: Option) {
  emit('picked', { source_type: 'table', dataset_name: opt.name, source_kind: opt.kind })
  close()
}
</script>

<template>
  <Teleport to="body">
    <Transition name="modal">
      <div v-if="modelValue" class="modal-overlay" @click.self="close">
        <div class="modal-container">
          <div class="modal-header">
            <div>
              <h2>New visualization</h2>
              <p class="modal-subtitle">Pick a catalog table to chart</p>
            </div>
            <button class="close-btn" title="Close" @click="close">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" /></svg>
            </button>
          </div>

          <div class="modal-content">
            <input
              v-if="options.length"
              v-model="search"
              class="picker-search"
              type="text"
              placeholder="Search tables..."
            />
            <ul v-if="filtered.length" class="picker-list">
              <li v-for="opt in filtered" :key="opt.kind + ':' + opt.name">
                <button class="picker-item" @click="pick(opt)">
                  <i class="fa-solid fa-table"></i>
                  <span class="picker-item-name">{{ opt.name }}</span>
                  <span class="picker-item-kind">{{ opt.kind === 'external' ? 'External' : 'Catalog' }}</span>
                </button>
              </li>
            </ul>
            <p v-else-if="options.length" class="picker-empty">No tables match your search.</p>
            <p v-else class="picker-empty">
              No catalog tables yet. Switch to the Catalog tab and upload a CSV first.
            </p>
          </div>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>

<style scoped>
.modal-overlay {
  position: fixed;
  inset: 0;
  z-index: 9999;
  background: rgba(0, 0, 0, 0.6);
  backdrop-filter: blur(4px);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
}
.modal-container {
  background: var(--color-background-primary);
  border-radius: 12px;
  width: 100%;
  max-width: 480px;
  display: flex;
  flex-direction: column;
  box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
  border: 1px solid var(--color-border-primary);
}
.modal-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  padding: 20px 24px;
  border-bottom: 1px solid var(--color-border-light);
}
.modal-header h2 {
  font-size: 18px;
  font-weight: 600;
  margin: 0;
  color: var(--color-text-primary);
}
.modal-subtitle {
  font-size: 13px;
  color: var(--color-text-secondary);
  margin: 2px 0 0;
}
.close-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border: none;
  border-radius: 6px;
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  cursor: pointer;
}
.close-btn:hover {
  background: var(--color-background-hover);
  color: var(--color-text-primary);
}
.close-btn svg {
  width: 16px;
  height: 16px;
}
.modal-content {
  padding: 16px 24px 24px;
  display: flex;
  flex-direction: column;
  gap: 12px;
  max-height: 60vh;
  overflow-y: auto;
}
.picker-search {
  width: 100%;
  padding: 8px 12px;
  font-size: 14px;
  color: var(--color-text-primary);
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 8px;
}
.picker-search:focus {
  outline: none;
  border-color: var(--color-accent);
}
.picker-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.picker-item {
  display: flex;
  align-items: center;
  gap: 10px;
  width: 100%;
  padding: 10px 12px;
  border: 1px solid var(--color-border-light);
  border-radius: 8px;
  background: var(--color-background-secondary);
  color: var(--color-text-primary);
  cursor: pointer;
  text-align: left;
  transition: all 0.15s;
}
.picker-item:hover {
  border-color: var(--color-accent);
  background: var(--color-background-hover);
}
.picker-item i {
  font-size: 15px;
  color: var(--color-text-secondary);
}
.picker-item-name {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 14px;
}
.picker-item-kind {
  font-size: 11px;
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}
.picker-empty {
  margin: 0;
  padding: 16px;
  text-align: center;
  font-size: 13px;
  color: var(--color-text-muted);
}
.modal-enter-active,
.modal-leave-active {
  transition: all 0.3s ease;
}
.modal-enter-from,
.modal-leave-to {
  opacity: 0;
}
.modal-enter-from .modal-container,
.modal-leave-to .modal-container {
  transform: scale(0.95) translateY(10px);
}
</style>
