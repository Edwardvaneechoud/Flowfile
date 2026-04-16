<template>
  <div class="ns-tree-item">
    <div
      class="ns-row"
      :class="{ selected: selectedId === node.id, selectable: node.level === 1 }"
      @click="handleRowClick"
    >
      <i
        v-if="node.children && node.children.length > 0"
        :class="expanded ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'"
        class="chevron"
        @click.stop="toggle"
      ></i>
      <span v-else class="chevron-placeholder"></span>
      <i
        :class="node.level === 0 ? 'fa-solid fa-box-archive' : 'fa-solid fa-layer-group'"
        class="ns-icon"
      ></i>
      <span class="ns-name">{{ node.name }}</span>
    </div>
    <div v-if="expanded && node.children && node.children.length > 0" class="ns-children">
      <namespace-tree-item
        v-for="child in node.children"
        :key="child.id"
        :node="child"
        :selected-id="selectedId"
        :initially-expanded="initiallyExpanded"
        @select="$emit('select', $event)"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import type { NamespaceTree } from "../../../types";

const props = defineProps<{
  node: NamespaceTree;
  selectedId: number | null;
  initiallyExpanded?: boolean;
}>();

const emit = defineEmits<{
  (e: "select", namespaceId: number): void;
}>();

const expanded = ref(props.initiallyExpanded ?? false);

const toggle = () => {
  expanded.value = !expanded.value;
};

const handleRowClick = () => {
  // Only allow selection on schema-level (level 1) namespaces
  if (props.node.level === 1) {
    emit("select", props.node.id);
  } else {
    toggle();
  }
};
</script>

<style scoped>
.ns-tree-item {
  display: flex;
  flex-direction: column;
}

.ns-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: 6px var(--spacing-2);
  border-radius: var(--border-radius-sm);
  cursor: pointer;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  transition: background-color var(--transition-fast);
}

.ns-row.selectable:hover {
  background-color: var(--color-background-tertiary);
}

.ns-row.selected {
  background-color: var(--color-accent-subtle, #e8f0fe);
  color: var(--color-accent, #1976d2);
  font-weight: var(--font-weight-medium);
}

.chevron,
.chevron-placeholder {
  width: 12px;
  text-align: center;
  font-size: 10px;
  color: var(--color-text-muted);
}

.chevron {
  cursor: pointer;
}

.ns-icon {
  font-size: 12px;
  color: var(--color-text-secondary);
}

.ns-row.selected .ns-icon {
  color: var(--color-accent, #1976d2);
}

.ns-name {
  flex: 1;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.ns-children {
  margin-left: var(--spacing-4);
  display: flex;
  flex-direction: column;
  gap: 2px;
}
</style>
