<template>
  <div v-if="!hidden" class="tree-section">
    <button class="section-header" @click.stop="toggle">
      <i
        :class="expanded ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'"
        class="section-chevron"
      />
      <span class="section-title">{{ title }}</span>
      <span v-if="count !== undefined" class="section-count">{{ count }}</span>
    </button>
    <div v-if="expanded" class="section-content">
      <slot />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";

import { useCatalogTreeExpansion } from "../useCatalogTreeExpansion";

const props = withDefaults(
  defineProps<{
    title: string;
    count?: number;
    hidden?: boolean;
    defaultExpanded?: boolean;
    storageKey?: string;
    // Sections only render while they still hold matches, so a search opens them
    // by default and keeps that state out of the persisted map.
    searchActive?: boolean;
  }>(),
  {
    count: undefined,
    hidden: false,
    defaultExpanded: false,
    storageKey: undefined,
    searchActive: false,
  },
);

const treeState = useCatalogTreeExpansion();
const localExpanded = ref(props.defaultExpanded);
const expanded = computed({
  get: () => {
    if (!props.storageKey) return localExpanded.value;
    return props.searchActive
      ? treeState.isExpandedDuringSearch(props.storageKey, true)
      : treeState.isExpanded(props.storageKey, props.defaultExpanded);
  },
  set: (value) => {
    if (!props.storageKey) localExpanded.value = value;
    else if (props.searchActive) treeState.setExpandedDuringSearch(props.storageKey, value);
    else treeState.setExpanded(props.storageKey, value);
  },
});

function toggle() {
  expanded.value = !expanded.value;
}
</script>

<style scoped>
.tree-section {
  margin-top: var(--spacing-2);
}

.section-header {
  width: 100%;
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-2);
  border: none;
  background: transparent;
  color: var(--color-text-secondary);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.section-header:hover {
  background: var(--color-background-hover);
  color: var(--color-text-primary);
}

.section-chevron {
  width: 12px;
  font-size: 10px;
  text-align: center;
  color: var(--color-text-muted);
}

.section-title {
  flex: 1;
  text-align: left;
}

.section-count {
  font-size: 10px;
  color: var(--color-text-muted);
  background: var(--color-background-primary);
  padding: 0 6px;
  border-radius: var(--border-radius-full);
  line-height: 16px;
}

.section-content {
  margin-top: var(--spacing-1);
  padding-left: var(--spacing-2);
}
</style>
