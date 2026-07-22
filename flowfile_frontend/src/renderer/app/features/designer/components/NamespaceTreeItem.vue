<template>
  <!-- TODO(ux): non-selectable group rows (level 0) should visually differ
       from selectable schemas (level 1); users currently click them
       expecting selection and nothing happens. -->
  <div class="ns-tree-item">
    <div
      class="ns-row"
      :class="{ selected: selectedId === node.id, selectable: isSelectable, disabled: isReadOnly }"
      :title="isReadOnly ? 'Read-only — you need edit (manage) access to save here' : undefined"
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
      <i v-if="isReadOnly" class="fa-solid fa-lock ns-lock"></i>
    </div>
    <div v-if="expanded && node.children && node.children.length > 0" class="ns-children">
      <namespace-tree-item
        v-for="child in node.children"
        :key="child.id"
        :node="child"
        :selected-id="selectedId"
        :initially-expanded="initiallyExpanded"
        :writable-only="writableOnly"
        :inherited-access="node.access ?? inheritedAccess"
        @select="$emit('select', $event)"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import type { NamespaceTree } from "../../../types";
import type { AccessInfo } from "../../../types/sharing.types";
import { useWritableNamespaces } from "../../../composables/useWritableNamespaces";

const props = defineProps<{
  node: NamespaceTree;
  selectedId: number | null;
  initiallyExpanded?: boolean;
  // Save-target mode: grey out and block selecting read-only (use-grant) schemas.
  // Leave unset for browse/read pickers (Open dialog) — a use grant is a valid read target.
  writableOnly?: boolean;
  // Parent namespace's access stamp: the backend stamps direct grants only, so an
  // unstamped schema inherits its catalog's grant level.
  inheritedAccess?: AccessInfo | null;
}>();

const emit = defineEmits<{
  (e: "select", namespaceId: number): void;
}>();

const { isWritableNamespace } = useWritableNamespaces();

const expanded = ref(props.initiallyExpanded ?? false);

// In save-target mode, read-only schemas stay visible so the hierarchy is
// legible, but are greyed out and not selectable.
const isReadOnly = computed(
  () =>
    (props.writableOnly ?? false) &&
    props.node.level === 1 &&
    !isWritableNamespace({ access: props.node.access ?? props.inheritedAccess }),
);
const isSelectable = computed(() => props.node.level === 1 && !isReadOnly.value);

const toggle = () => {
  expanded.value = !expanded.value;
};

const handleRowClick = () => {
  if (isSelectable.value) {
    emit("select", props.node.id);
  } else if (props.node.level !== 1) {
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

.ns-row.disabled {
  opacity: 0.55;
  cursor: not-allowed;
}

.ns-lock {
  font-size: 10px;
  color: var(--color-text-muted);
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
