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
        @dblclick="promptRename(tab.id)"
      >
        <span class="flow-tab__dot" :class="{ filled: tabsStore.tabHasContent(tab) }"></span>
        <span class="flow-tab__name">{{ tabLabel(tab) }}</span>
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
  </div>
</template>

<script setup lang="ts">
import { onMounted } from 'vue'
import { useFlowTabsStore, type FlowTab } from '../stores/flow-tabs-store'
import { useFlowStore } from '../stores/flow-store'

const tabsStore = useFlowTabsStore()
const flowStore = useFlowStore()

// The active tab's label tracks the live flow name (updates instantly on Save /
// rename); inactive tabs show their stashed name.
function tabLabel(tab: FlowTab): string {
  return tab.id === tabsStore.activeTabId ? flowStore.currentFlowName : tab.name
}

function promptRename(id: string): void {
  const current = id === tabsStore.activeTabId ? flowStore.currentFlowName : (tabsStore.tabs.find((t) => t.id === id)?.name ?? '')
  const name = window.prompt('Rename flow:', current)
  if (name !== null) tabsStore.renameTab(id, name)
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
