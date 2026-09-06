<template>
  <div class="settings-tabs">
    <template v-for="tab in tabs" :key="tab.key">
      <div v-if="tab.groupStart" class="tab-group-divider" aria-hidden="true"></div>
      <span v-if="tab.groupLabel" class="tab-group-label" aria-hidden="true">
        {{ tab.groupLabel }}
      </span>
      <button
        type="button"
        class="settings-tab"
        :class="{ active: activeTab === tab.key }"
        @click="emit('select', tab.key)"
      >
        <i :class="tab.icon"></i>
        <span>{{ tab.label }}</span>
      </button>
    </template>
  </div>
</template>

<script setup lang="ts">
// Header tab bar shared by the tabbed Settings pages (Compute, AI). Optional
// group captions/dividers render between clusters; the page owns which tabs
// are visible and what ?tab= they map to.
export interface SettingsTabItem {
  key: string;
  label: string;
  icon: string;
  groupStart?: boolean;
  groupLabel?: string | null;
}

defineProps<{
  tabs: SettingsTabItem[];
  activeTab: string;
}>();

const emit = defineEmits<{
  (e: "select", key: string): void;
}>();
</script>

<style scoped>
.settings-tabs {
  display: flex;
  gap: 2px;
  padding: var(--spacing-2) var(--spacing-4);
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
}

.tab-group-divider {
  width: 1px;
  align-self: stretch;
  margin: var(--spacing-1) var(--spacing-1);
  background: var(--color-border-primary);
}

.tab-group-label {
  flex-shrink: 0;
  align-self: center;
  margin: 0 2px 0 var(--spacing-2);
  font-size: 9px;
  font-weight: var(--font-weight-semibold);
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--color-text-tertiary);
  opacity: 0.7;
  white-space: nowrap;
  user-select: none;
  pointer-events: none;
}

.settings-tab {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-4);
  border: none;
  background: transparent;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  border-radius: var(--border-radius-md);
  transition: all var(--transition-fast);
}

.settings-tab:hover {
  background: var(--color-background-hover);
  color: var(--color-text-primary);
}

.settings-tab.active {
  background: var(--color-background-primary);
  color: var(--color-primary);
  box-shadow: var(--shadow-xs);
}
</style>
