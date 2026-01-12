<template>
  <div v-bind="$attrs">
    <!-- Description header -->
    <div class="custom-node-header" @click.stop>
      <div v-if="!editMode" class="description-display" @click.stop>
        <div class="edit-icon" title="Edit description" @click.stop="toggleEditMode(true)">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
            <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
          </svg>
        </div>
        <pre class="description-text">{{ descriptionSummary }}</pre>
      </div>
      <div v-else class="custom-node-header" @click.stop>
        <textarea
          v-model="description"
          class="description-input"
          @blur="toggleEditMode(false)"
          @click.stop
        ></textarea>
      </div>
    </div>

    <!-- Node body -->
    <div class="custom-node" @contextmenu.prevent="showContextMenu">
      <!-- Node button with icon -->
      <div class="component-wrapper">
        <div class="status-indicator" :class="statusClass">
          <span class="tooltip-text">{{ statusTooltip }}</span>
        </div>
        <button :class="['node-button', { selected: isSelected }]" @click="onClick">
          <img :src="iconUrl" :alt="data.label" width="50" height="50" />
        </button>
      </div>

      <!-- Input handles -->
      <div
        v-for="i in data.inputs"
        :key="`input-${i - 1}`"
        class="handle-input"
        :style="getHandleStyle(i - 1, data.inputs)"
      >
        <Handle :id="`input-${i - 1}`" type="target" :position="Position.Left" />
      </div>

      <!-- Output handles -->
      <div
        v-for="i in data.outputs"
        :key="`output-${i - 1}`"
        class="handle-output"
        :style="getHandleStyle(i - 1, data.outputs)"
      >
        <Handle :id="`output-${i - 1}`" type="source" :position="Position.Right" />
      </div>

      <!-- Context Menu -->
      <Teleport v-if="menuVisible" to="body">
        <div ref="menuEl" class="context-menu" :style="contextMenuStyle">
          <div class="context-menu-item" @click="runNode">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <polygon points="5 3 19 12 5 21 5 3"></polygon>
            </svg>
            <span>Run Now</span>
          </div>
          <div class="context-menu-divider"></div>
          <div class="context-menu-item context-menu-item-danger" @click="deleteNode">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M3 6h18"></path>
              <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"></path>
              <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
            </svg>
            <span>Delete</span>
          </div>
        </div>
      </Teleport>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, onUnmounted, nextTick } from 'vue'
import { Handle, Position } from '@vue-flow/core'
import type { NodeResult } from '../../types'
import { useFlowStore } from '../../stores/flow-store'

// Icon mapping to files
const iconMap: Record<string, string> = {
  read_csv: 'input_data.png',
  manual_input: 'manual_input.png',
  filter: 'filter.png',
  select: 'select.png',
  group_by: 'group_by.png',
  join: 'join.png',
  sort: 'sort.png',
  polars_code: 'polars_code.png',
  unique: 'unique.png',
  head: 'sample.png',
  preview: 'view.png',
  pivot: 'pivot.png',
  unpivot: 'unpivot.png',
  output: 'output.png'
}

interface NodeData {
  id: number
  type: string
  label: string
  inputs: number
  outputs: number
  result?: NodeResult
}

const props = defineProps<{
  data: NodeData
}>()

const emit = defineEmits<{
  (e: 'delete', id: number): void
  (e: 'run', id: number): void
}>()

const flowStore = useFlowStore()

// Description editing
const description = ref('')
const editMode = ref(false)
const CHAR_LIMIT = 100

// Context menu state
const menuVisible = ref(false)
const menuEl = ref<HTMLElement | null>(null)
const contextMenuX = ref(0)
const contextMenuY = ref(0)

// Computed properties
const isSelected = computed(() => flowStore.selectedNodeId === props.data.id)

const iconUrl = computed(() => {
  const iconFile = iconMap[props.data.type] || 'view.png'
  return new URL(`../../assets/icons/${iconFile}`, import.meta.url).href
})

const statusClass = computed(() => {
  if (!props.data.result) return 'unknown'
  if (props.data.result.success === true) return 'success'
  if (props.data.result.success === false) return 'failure'
  return 'unknown'  // undefined = not executed yet
})

const statusTooltip = computed(() => {
  if (!props.data.result) return 'Status unknown'
  if (props.data.result.success) {
    const rows = props.data.result.data?.total_rows
    return rows !== undefined ? `Operation successful: ${rows} rows` : 'Operation successful'
  }
  return `Operation failed: ${props.data.result.error}`
})

const descriptionSummary = computed(() => {
  if (!description.value) {
    return `${props.data.id}: ${props.data.label}`
  }
  if (description.value.length > CHAR_LIMIT) {
    const truncatePoint = description.value.lastIndexOf(' ', CHAR_LIMIT)
    const endPoint = truncatePoint > 0 ? truncatePoint : CHAR_LIMIT
    return description.value.substring(0, endPoint) + '...'
  }
  return description.value
})

const contextMenuStyle = computed(() => ({
  position: 'fixed' as const,
  zIndex: 10000,
  top: `${contextMenuY.value}px`,
  left: `${contextMenuX.value}px`
}))

// Methods
function toggleEditMode(state: boolean) {
  editMode.value = state
}

function onClick() {
  flowStore.selectNode(props.data.id)
}

function showContextMenu(event: MouseEvent) {
  event.preventDefault()
  event.stopPropagation()

  contextMenuX.value = event.clientX
  contextMenuY.value = event.clientY
  menuVisible.value = true

  setTimeout(() => {
    window.addEventListener('click', handleClickOutsideMenu)
  }, 0)

  nextTick(() => {
    updateMenuPosition()
  })
}

function updateMenuPosition() {
  if (!menuEl.value) return

  const menuRect = menuEl.value.getBoundingClientRect()
  const viewportWidth = window.innerWidth
  const viewportHeight = window.innerHeight

  let left = contextMenuX.value
  let top = contextMenuY.value

  if (left + menuRect.width > viewportWidth - 10) {
    left = viewportWidth - menuRect.width - 10
  }

  if (top + menuRect.height > viewportHeight - 10) {
    top = viewportHeight - menuRect.height - 10
  }

  contextMenuX.value = left
  contextMenuY.value = top
}

function handleClickOutsideMenu(event: MouseEvent) {
  if (menuEl.value && !menuEl.value.contains(event.target as Node)) {
    closeContextMenu()
  }
}

function closeContextMenu() {
  menuVisible.value = false
  window.removeEventListener('click', handleClickOutsideMenu)
}

function runNode() {
  emit('run', props.data.id)
  closeContextMenu()
}

function deleteNode() {
  emit('delete', props.data.id)
  closeContextMenu()
}

function getHandleStyle(index: number, total: number) {
  const topMargin = 30
  const bottomMargin = 25

  if (total === 1) {
    return { top: '55%', transform: 'translateY(-55%)' }
  }

  const spacing = (100 - topMargin - bottomMargin) / (total - 1)
  return { top: `${topMargin + spacing * index}%` }
}

onUnmounted(() => {
  window.removeEventListener('click', handleClickOutsideMenu)
})
</script>

<style scoped>
.custom-node {
  border-radius: 4px;
  padding: 1px;
  background-color: var(--bg-secondary);
  display: flex;
  flex-direction: column;
  align-items: center;
  position: relative;
}

.custom-node-header {
  font-weight: 100;
  font-size: small;
  width: 20px;
  white-space: nowrap;
  overflow: visible;
  text-overflow: ellipsis;
}

.description-display {
  position: relative;
  white-space: normal;
  min-width: 100px;
  max-width: 300px;
  width: auto;
  padding: 2px 4px;
  cursor: pointer;
  background-color: var(--bg-secondary);
  display: flex;
  align-items: flex-start;
  gap: 4px;
  border-radius: 4px;
  color: var(--text-primary);
}

.edit-icon {
  opacity: 0;
  transition: opacity 0.2s;
  color: var(--accent-color);
  cursor: pointer;
  display: flex;
  align-items: center;
  padding: 2px;
}

.description-display:hover .edit-icon {
  opacity: 1;
}

.edit-icon:hover {
  color: var(--accent-hover);
}

.description-text {
  margin: 0;
  white-space: pre-wrap;
  word-wrap: break-word;
  font-size: 12px;
}

.description-input {
  width: 200px;
  height: 75px;
  resize: both;
  padding: 4px;
  border: 1px solid var(--accent-color);
  border-radius: 4px;
  font-size: small;
  background-color: var(--bg-secondary);
  color: var(--text-primary);
}

.component-wrapper {
  position: relative;
  max-width: 60px;
  overflow: visible;
}

.status-indicator {
  position: relative;
  display: flex;
  align-items: center;
  margin-right: 8px;
}

.status-indicator::before {
  content: "";
  display: block;
  width: 10px;
  height: 10px;
  border-radius: 50%;
}

.status-indicator.success::before {
  background-color: #4caf50;
}

.status-indicator.failure::before {
  background-color: #f44336;
}

.status-indicator.warning::before {
  background-color: #f09f5dd1;
}

.status-indicator.unknown::before {
  background-color: var(--color-text-muted);
}

.status-indicator.running::before {
  background-color: #0909ca;
  animation: pulse 1.5s cubic-bezier(0.4, 0, 0.6, 1) infinite;
  box-shadow: 0 0 10px #0909ca;
}


@keyframes pulse {
  0% {
    transform: scale(1);
    opacity: 1;
    box-shadow: 0 0 5px #0909ca;
  }
  50% {
    transform: scale(1.3);
    opacity: 0.6;
    box-shadow: 0 0 15px #0909ca;
  }
  100% {
    transform: scale(1);
    opacity: 1;
    box-shadow: 0 0 5px #0909ca;
  }
}

.tooltip-text {
  visibility: hidden;
  width: 120px;
  background-color: #333;
  color: #fff;
  text-align: center;
  border-radius: 6px;
  padding: 5px 0;
  position: absolute;
  z-index: 1;
  bottom: 100%;
  left: 50%;
  margin-left: -60px;
  opacity: 0;
  transition: opacity 0.3s;
  font-size: 11px;
}

.status-indicator:hover .tooltip-text {
  visibility: visible;
  opacity: 1;
}

.node-button {
  background-color: #dedede;
  border-radius: 10px;
  border-width: 0px;
  cursor: pointer;
  padding: 4px;
  transition: all 0.2s;
}

.node-button:hover {
  background-color: #c8c8c8;
  transform: translateY(-1px);
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.node-button.selected {
  border: 2px solid var(--accent-color);
}

.node-button img {
  display: block;
}

.handle-input {
  position: absolute;
  left: -8px;
}

.handle-output {
  position: absolute;
  right: -8px;
}

/* Context menu styles */
.context-menu {
  position: fixed;
  z-index: 10000;
  background-color: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  padding: 4px 0;
  min-width: 120px;
}

.context-menu-item {
  padding: 8px 12px;
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
  font-size: 13px;
  transition: background-color 0.2s;
  color: var(--text-primary);
}

.context-menu-item:hover {
  background-color: var(--bg-hover);
}

.context-menu-item svg {
  color: var(--text-secondary);
}

.context-menu-divider {
  height: 1px;
  background-color: var(--border-color);
  margin: 4px 0;
}

.context-menu-item-danger {
  color: #dc3545;
}

.context-menu-item-danger:hover {
  background-color: rgba(220, 53, 69, 0.1);
}

.context-menu-item-danger svg {
  color: #dc3545;
}

:deep(.vue-flow__handle) {
  width: 10px;
  height: 10px;
  border: 2px solid var(--bg-secondary);
}

:deep(.vue-flow__handle:hover) {
  background: var(--accent-hover);
}
</style>
