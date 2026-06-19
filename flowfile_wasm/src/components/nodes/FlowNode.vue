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
          <img :src="iconUrl" :alt="data.label" width="40" />
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

      <!-- Context Menu: teleported to Canvas container (inherits CSS vars, avoids VueFlow transform) -->
      <Teleport v-if="menuVisible" to="#flowfile-context-menu-container">
        <div ref="menuEl" class="context-menu" :style="contextMenuStyle">
          <div class="context-menu-item" @click="editNode">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
              <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
            </svg>
            <span>Edit</span>
          </div>
          <div class="context-menu-item" @click="viewData">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M3 3h18v18H3z"></path>
              <path d="M3 9h18M3 15h18M9 3v18"></path>
            </svg>
            <span>View data</span>
          </div>
          <div class="context-menu-item" @click="learnAboutNode">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>
            <span>Learn about this node</span>
          </div>
          <div class="context-menu-divider"></div>
          <div class="context-menu-item" @click="runNode">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <polygon points="5 3 19 12 5 21 5 3"></polygon>
            </svg>
            <span>Run Now</span>
          </div>
          <div v-if="canSaveToCatalog" class="context-menu-item" @click="saveToCatalog">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>
              <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>
              <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
            </svg>
            <span>Save to catalog</span>
          </div>
          <div class="context-menu-divider"></div>
          <div class="context-menu-item" @click="copyNode">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
              <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
            </svg>
            <span>Copy</span>
          </div>
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
import { computed, ref, onUnmounted, nextTick, watch } from 'vue'
import { Handle, Position } from '@vue-flow/core'
import type { NodeResult } from '../../types'
import { useFlowStore } from '../../stores/flow-store'
import { iconUrls } from '../../utils/iconUrls'

const iconMap: Record<string, string> = {
  read: 'input_data.svg',
  manual_input: 'manual_input.svg',
  external_data: 'external_data.svg',
  read_from_catalog: 'catalog_reader.svg',
  filter: 'filter.svg',
  select: 'select.svg',
  formula: 'formula.svg',
  group_by: 'group_by.svg',
  join: 'join.svg',
  cross_join: 'cross_join.svg',
  union: 'union.svg',
  sort: 'sort.svg',
  polars_code: 'polars_code.svg',
  unique: 'unique.svg',
  dynamic_rename: 'dynamic_rename.svg',
  record_id: 'record_id.svg',
  head: 'sample.svg',
  explore_data: 'explore_data.svg',
  pivot: 'pivot.svg',
  unpivot: 'unpivot.svg',
  output: 'output.svg',
  external_output: 'external_output.svg',
  write_to_catalog: 'catalog_writer.svg'
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
  (e: 'edit', id: number): void
  (e: 'view-data', id: number): void
  (e: 'copy', id: number): void
  (e: 'save-to-catalog', id: number): void
  (e: 'show-info', type: string, position: { x: number; y: number }): void
}>()

const flowStore = useFlowStore()

// Source nodes carry their loaded CSV in fileContents, so they can be persisted
// to the catalog as a reusable table.
const SOURCE_NODE_TYPES = new Set(['read', 'manual_input', 'external_data', 'read_from_catalog'])
const canSaveToCatalog = computed(
  () => SOURCE_NODE_TYPES.has(props.data.type) && flowStore.hasFileContent(props.data.id)
)

const description = ref('')
const editMode = ref(false)
const CHAR_LIMIT = 100

function initDescription() {
  const node = flowStore.getNode(props.data.id)
  if (node) {
    description.value = node.description || (node.settings as any)?.description || ''
  }
}

initDescription()

watch(
  () => flowStore.getNode(props.data.id)?.description,
  (newDesc) => {
    if (!editMode.value && newDesc !== undefined) {
      description.value = newDesc || ''
    }
  }
)

const menuVisible = ref(false)
const menuEl = ref<HTMLElement | null>(null)
const contextMenuX = ref(0)
const contextMenuY = ref(0)

const isSelected = computed(() => flowStore.selectedNodeId === props.data.id)

const iconUrl = computed(() => {
  const iconFile = iconMap[props.data.type] || 'view.png'
  return iconUrls[iconFile] || new URL(`../../assets/icons/${iconFile}`, import.meta.url).href
})

const statusClass = computed(() => {
  const isDirty = flowStore.isNodeDirty(props.data.id)
  if (isDirty) return 'unknown'

  if (!props.data.result) return 'unknown'
  if (props.data.result.success === true) return 'success'
  if (props.data.result.success === false) return 'failure'
  return 'unknown'
})

const statusTooltip = computed(() => {
  const isDirty = flowStore.isNodeDirty(props.data.id)
  if (isDirty) return 'Node has changes - run flow to update'
  
  if (!props.data.result) return 'Not executed yet'
  if (props.data.result.success) {
    const rows = props.data.result.data?.total_rows
    return rows !== undefined ? `Success: ${rows} rows` : 'Success'
  }
  return `Failed: ${props.data.result.error}`
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

function toggleEditMode(state: boolean) {
  if (!state && editMode.value) {
    saveDescription()
  }
  editMode.value = state
}

function saveDescription() {
  flowStore.updateNodeDescription(props.data.id, description.value)
}

function onClick() {
  // Single click opens the Settings panel only (the Table stays in whatever
  // open/closed state it was). Matches the Canvas-level node-click handler.
  flowStore.selectNode(props.data.id)
  flowStore.showSettings = true
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

function editNode() {
  emit('edit', props.data.id)
  closeContextMenu()
}

function viewData() {
  emit('view-data', props.data.id)
  closeContextMenu()
}

function learnAboutNode(event: MouseEvent) {
  emit('show-info', props.data.type, { x: event.clientX, y: event.clientY })
  closeContextMenu()
}

function copyNode() {
  emit('copy', props.data.id)
  closeContextMenu()
}

function saveToCatalog() {
  emit('save-to-catalog', props.data.id)
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
  font-family: var(--font-family-base);
  font-size: 12px;
}

.description-input {
  width: 200px;
  height: 75px;
  resize: both;
  padding: 4px;
  border: 1px solid var(--accent-color);
  border-radius: 4px;
  font-family: var(--font-family-base);
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
  background-color: var(--color-gray-800);
  color: var(--color-text-inverse);
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
  transition: all var(--transition-normal);
}

.node-button:hover {
  background-color: var(--color-background-hover);
  transform: translateY(-1px);
  box-shadow: var(--shadow-sm);
}

.node-button.selected {
  border: 2px solid var(--color-accent);
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