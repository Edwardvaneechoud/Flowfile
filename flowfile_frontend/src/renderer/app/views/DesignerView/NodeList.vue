<template>
  <div class="nodes-wrapper" data-tutorial="node-list">
    <!-- Search Input -->
    <input v-model="searchQuery" type="text" placeholder="Search nodes..." class="search-input" />

    <div
      v-for="group in filteredGroups"
      :key="group.key"
      class="category-container"
      :data-tutorial-category="group.key"
    >
      <!-- Category Header -->
      <button class="category-header" @click="toggleGroup(group.key)">
        <span class="category-title">{{ group.label }}</span>
        <span v-if="group.isDynamic" class="category-chip" title="Custom category">custom</span>
        <el-icon class="category-icon">
          <ArrowDown v-if="isGroupOpen(group.key)" />
          <ArrowRight v-else />
        </el-icon>
      </button>

      <!-- Category Content -->
      <div v-if="isGroupOpen(group.key)" class="category-content">
        <NodeListItem
          v-for="node in group.nodes"
          :key="node.item"
          :node="node"
          :favorite="isFavorite(node.item)"
          :hidden-node="group.key === HIDDEN_GROUP_KEY"
          :tutorial-target="group.key !== FAVORITES_GROUP_KEY"
          :suppress-tooltip="nodeMenu !== null"
          @dragstart="onDragStart"
          @contextmenu="openNodeMenu"
        />
      </div>
    </div>

    <Teleport to="body">
      <ContextMenu
        v-if="nodeMenu"
        :position="{ x: nodeMenu.x, y: nodeMenu.y }"
        :options="menuOptions"
        @select="onMenuSelect"
        @close="nodeMenu = null"
      />
    </Teleport>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from "vue";
import { ArrowDown, ArrowRight } from "@element-plus/icons-vue";
import { useNodes } from "./useNodes";
import { FAVORITES_GROUP_KEY, HIDDEN_GROUP_KEY, usePaletteGroups } from "./usePaletteGroups";
import { readinessKey, useKernelReadiness } from "../../composables/useKernelReadiness";
import NodeListItem from "./NodeListItem.vue";
import ContextMenu from "../../components/common/ContextMenu/ContextMenu.vue";
import { nodeDocsMenuOptions, nodeDocsUrl, READ_MORE_ACTION } from "./nodeDocsLinks";
import { desktop } from "../../../lib/desktop";
import type { NodeTemplate } from "../../types";
import type { ContextMenuOption } from "../../components/common/ContextMenu/types";

const { nodes } = useNodes();
const {
  searchQuery,
  filteredGroups,
  isGroupOpen,
  toggleGroup,
  isFavorite,
  isHidden,
  toggleFavorite,
  toggleHidden,
} = usePaletteGroups(nodes);

// One batch readiness fetch for every kernel-env template with deps; the rows
// (NodeListItem) read the shared cache keyed by their own dependency set.
const { ensureReadiness } = useKernelReadiness();
watch(nodes, (all) => {
  const items: Record<string, string[]> = {};
  for (const node of all) {
    if (node.execution_environment === "kernel" && node.dependencies?.length) {
      items[readinessKey(node.dependencies)] = node.dependencies;
    }
  }
  if (Object.keys(items).length) void ensureReadiness(items);
});

const emit = defineEmits<{
  (e: "dragstart", event: DragEvent, node: NodeTemplate): void;
}>();

// One menu for the whole palette — only one can be open, and per-row menus would
// each register document-level listeners for all ~45 rows.
const nodeMenu = ref<{ node: NodeTemplate; x: number; y: number } | null>(null);

const FAVORITE_ACTION = "favorite";
const HIDE_ACTION = "hide";

const menuOptions = computed<ContextMenuOption[]>(() => {
  const item = nodeMenu.value?.node.item ?? "";
  return [
    {
      label: isFavorite(item) ? "Remove from favorites" : "Add to favorites",
      action: FAVORITE_ACTION,
    },
    { label: isHidden(item) ? "Unhide node" : "Hide node", action: HIDE_ACTION },
    ...nodeDocsMenuOptions(),
  ];
});

const openNodeMenu = (event: MouseEvent, node: NodeTemplate) => {
  nodeMenu.value = { node, x: event.clientX, y: event.clientY };
};

const onDragStart = (event: DragEvent, node: NodeTemplate) => {
  nodeMenu.value = null;
  emit("dragstart", event, node);
};

const onMenuSelect = (action: string) => {
  if (!nodeMenu.value) return;
  const node = nodeMenu.value.node;
  if (action === FAVORITE_ACTION) toggleFavorite(node.item);
  else if (action === HIDE_ACTION) toggleHidden(node.item);
  // Synchronous: in web mode this is window.open, which needs the click's
  // user-gesture attribution to survive the popup blocker.
  else if (action === READ_MORE_ACTION) void desktop.openExternal(nodeDocsUrl(node));
};
</script>

<style scoped>
.nodes-wrapper {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
  padding: var(--spacing-1-5);
  background-color: var(--color-background-primary);
  border-radius: var(--border-radius-lg);
  box-shadow: var(--shadow-sm);
}

/* Style for search input */
.search-input {
  padding: var(--spacing-2) var(--spacing-4);
  color: var(--color-text-primary);
  margin-bottom: var(--spacing-2);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  background-color: var(--color-background-primary);
  transition: border-color var(--transition-fast);
}

.search-input:focus {
  outline: none;
  border-color: var(--input-border-focus);
}

.category-container {
  overflow: hidden;
  border-radius: var(--border-radius-sm);
}

.category-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  padding: var(--spacing-2) var(--spacing-4);
  background-color: var(--color-background-muted);
  border: none;
  cursor: pointer;
  transition: background-color var(--transition-fast);
  height: 32px;
}

.category-header:hover {
  background-color: var(--color-background-tertiary);
}

.category-title {
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-normal);
  color: var(--color-text-primary);
  text-align: left;
}

/* Subtle marker distinguishing dynamic user-defined category groups. */
.category-chip {
  margin-left: var(--spacing-2);
  padding: 1px 6px;
  font-size: 10px;
  line-height: 1.4;
  color: var(--color-accent, #0891b2);
  background-color: var(--color-accent-soft, rgba(8, 145, 178, 0.12));
  border-radius: var(--border-radius-full, 999px);
  text-transform: uppercase;
  letter-spacing: 0.03em;
}

.category-icon {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  margin-left: auto;
}

.category-content {
  display: flex;
  flex-direction: column;
  background-color: var(--color-background-primary);
}

.category-content :deep(.node-item):last-child {
  border-bottom: none;
}

/* Custom scrollbar */
.nodes-wrapper::-webkit-scrollbar {
  width: 6px;
}

.nodes-wrapper::-webkit-scrollbar-track {
  background: transparent;
}

.nodes-wrapper::-webkit-scrollbar-thumb {
  background-color: var(--color-gray-300);
  border-radius: var(--border-radius-full);
}

.nodes-wrapper::-webkit-scrollbar-thumb:hover {
  background-color: var(--color-gray-400);
}
</style>
