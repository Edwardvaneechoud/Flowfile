<template>
  <div class="flow-tabs">
    <div class="flow-tabs__strip">
      <div
        v-for="tab in tabsStore.tabs"
        :key="tab.id"
        class="flow-tab"
        :class="{ active: tab.id === tabsStore.activeTabId }"
        :title="tabLabel(tab)"
        @click="tabsStore.switchTab(tab.id)"
        @dblclick="startRename(tab.id)"
        @contextmenu.prevent="openTabMenu($event, tab.id)"
      >
        <span class="flow-tab__dot" :class="{ filled: tabsStore.tabHasContent(tab) }"></span>
        <!-- keydown.stop: Canvas's window shortcuts (Ctrl+V pastes nodes) must not fire mid-rename -->
        <input
          v-if="renamingTabId === tab.id"
          :ref="setRenameInput"
          v-model="renameValue"
          class="flow-tab__rename"
          @keydown.stop
          @keydown.enter.prevent="commitRename"
          @keydown.escape="cancelRename"
          @blur="commitRename"
          @click.stop
          @dblclick.stop
          @contextmenu.stop
        />
        <span v-else class="flow-tab__name">{{ tabLabel(tab) }}</span>
        <button
          class="flow-tab__close"
          title="Close flow"
          @click.stop="tabsStore.closeTab(tab.id)"
        >
          <span class="material-icons">close</span>
        </button>
      </div>
    </div>
    <button class="flow-tabs__new" title="New flow tab" @click="tabsStore.newTab()">
      <span class="material-icons">add</span>
    </button>

    <Teleport to="body">
      <ContextMenu
        v-if="tabMenu"
        :position="{ x: tabMenu.x, y: tabMenu.y }"
        :options="tabMenuOptionsComputed"
        @select="onTabMenuSelect"
        @close="tabMenu = null"
      />
    </Teleport>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, nextTick, onMounted } from 'vue'
import { useFlowTabsStore, type FlowTab } from '../stores/flow-tabs-store'
import { useFlowStore } from '../stores/flow-store'
import ContextMenu from './common/ContextMenu.vue'
import { computeCloseTargets, tabMenuOptions, type TabCloseAction } from '../utils/flowTabActions'

const tabsStore = useFlowTabsStore()
const flowStore = useFlowStore()

// The active tab's label tracks the live flow name (updates instantly on Save /
// rename); inactive tabs show their stashed name.
function tabLabel(tab: FlowTab): string {
  return tab.id === tabsStore.activeTabId ? flowStore.currentFlowName : tab.name
}

function labelFor(id: string): string {
  if (id === tabsStore.activeTabId) return flowStore.currentFlowName
  return tabsStore.tabs.find((t) => t.id === id)?.name ?? ''
}

// Inline rename
const renamingTabId = ref<string | null>(null)
const renameValue = ref('')
const renameSeed = ref('')
const renameInput = ref<HTMLInputElement | null>(null)
// Function ref: only one rename input exists at a time, capture it directly.
const setRenameInput = (el: unknown) => {
  renameInput.value = (el as HTMLInputElement) ?? null
}

function startRename(id: string): void {
  renameSeed.value = labelFor(id)
  renameValue.value = renameSeed.value
  renamingTabId.value = id
  nextTick(() => {
    renameInput.value?.focus()
    renameInput.value?.select()
  })
}

function commitRename(): void {
  // Cleared first: Enter unmounts the input, whose blur re-enters this handler.
  if (renamingTabId.value === null) return
  const id = renamingTabId.value
  renamingTabId.value = null
  const trimmed = renameValue.value.trim()
  if (!trimmed || trimmed === renameSeed.value) return
  tabsStore.renameTab(id, trimmed)
}

function cancelRename(): void {
  renamingTabId.value = null
}

// Tab context menu
const tabMenu = ref<{ tabId: string; x: number; y: number } | null>(null)

function openTabMenu(event: MouseEvent, tabId: string): void {
  tabMenu.value = {
    tabId,
    // The menu doesn't self-clamp; keep it on-screen for right-edge tabs.
    x: Math.min(event.clientX, window.innerWidth - 200),
    y: event.clientY
  }
}

const tabMenuOptionsComputed = computed(() =>
  tabMenu.value
    ? tabMenuOptions(
        tabsStore.tabs.map((t) => t.id),
        tabMenu.value.tabId
      )
    : []
)

function onTabMenuSelect(action: string): void {
  const target = tabMenu.value?.tabId
  if (!target) return
  if (action === 'rename') {
    startRename(target)
  } else if (action === 'close') {
    tabsStore.closeTab(target)
  } else {
    tabsStore.closeTabs(
      computeCloseTargets(
        tabsStore.tabs.map((t) => t.id),
        target,
        action as TabCloseAction
      )
    )
  }
}

onMounted(() => {
  tabsStore.init()
})
</script>

<style scoped>
.flow-tabs {
  display: flex;
  align-items: stretch;
  gap: var(--spacing-1);
  height: 36px;
  padding: 0 var(--spacing-2);
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
  flex-shrink: 0;
  overflow: hidden;
}

.flow-tabs__strip {
  display: flex;
  align-items: stretch;
  gap: 2px;
  flex: 1;
  min-width: 0;
  overflow-x: auto;
  scrollbar-width: thin;
}

.flow-tab {
  display: flex;
  align-items: center;
  gap: var(--spacing-1-5);
  max-width: 200px;
  min-width: 110px;
  padding: 0 var(--spacing-2) 0 var(--spacing-3);
  margin-top: 4px;
  background: transparent;
  border: 1px solid transparent;
  border-bottom: none;
  border-radius: var(--border-radius-md) var(--border-radius-md) 0 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  cursor: pointer;
  user-select: none;
  transition: background var(--transition-fast), color var(--transition-fast);
}
.flow-tab:hover {
  background: var(--color-background-hover);
  color: var(--color-text-primary);
}
.flow-tab.active {
  background: var(--color-background-primary);
  border-color: var(--color-border-primary);
  color: var(--color-text-primary);
  font-weight: var(--font-weight-medium);
}

.flow-tab__dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  flex-shrink: 0;
  background: transparent;
  border: 1px solid var(--color-border-secondary);
}
.flow-tab__dot.filled {
  background: var(--color-accent);
  border-color: var(--color-accent);
}

.flow-tab__name {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.flow-tab__rename {
  flex: 1;
  width: 100%;
  min-width: 0;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  background: var(--color-background-primary);
  border: 1px solid var(--color-accent);
  border-radius: var(--border-radius-sm);
  padding: 1px 4px;
  outline: none;
}

.flow-tab__close {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 18px;
  height: 18px;
  border: none;
  background: transparent;
  border-radius: var(--border-radius-sm);
  color: var(--color-text-muted);
  cursor: pointer;
  opacity: 0;
  flex-shrink: 0;
  transition: all var(--transition-fast);
}
.flow-tab:hover .flow-tab__close,
.flow-tab.active .flow-tab__close {
  opacity: 1;
}
.flow-tab__close:hover {
  background: var(--color-background-tertiary);
  color: var(--color-danger);
}
.flow-tab__close .material-icons {
  font-size: 14px;
}

.flow-tabs__new {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  align-self: center;
  height: 28px;
  border: none;
  background: transparent;
  border-radius: var(--border-radius-md);
  color: var(--color-text-secondary);
  cursor: pointer;
  flex-shrink: 0;
  transition: all var(--transition-fast);
}
.flow-tabs__new:hover {
  background: var(--color-background-hover);
  color: var(--color-text-primary);
}
.flow-tabs__new .material-icons {
  font-size: 18px;
}
</style>
