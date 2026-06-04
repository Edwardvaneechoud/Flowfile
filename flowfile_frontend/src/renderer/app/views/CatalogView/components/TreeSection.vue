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
  }>(),
  {
    count: undefined,
    hidden: false,
    defaultExpanded: false,
    storageKey: undefined,
  },
);

const treeState = useCatalogTreeExpansion();
const localExpanded = ref(props.defaultExpanded);
const expanded = computed({
  get: () =>
    props.storageKey
      ? treeState.isExpanded(props.storageKey, props.defaultExpanded)
      : localExpanded.value,
  set: (value) => {
    if (props.storageKey) treeState.setExpanded(props.storageKey, value);
    else localExpanded.value = value;
  },
});

function toggle() {
  expanded.value = !expanded.value;
}

function expand() {
  expanded.value = true;
}

function collapse() {
  expanded.value = false;
}

defineExpose({ expand, collapse, toggle });
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
