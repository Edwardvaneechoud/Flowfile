<template>
  <div class="nodes-wrapper" data-tutorial="node-list">
    <!-- Search Input -->
    <input v-model="searchQuery" type="text" placeholder="Search nodes..." class="search-input" />

    <div
      v-for="(categoryInfo, category) in categories"
      v-show="!searchQuery || filteredNodes[category]"
      :key="category"
      class="category-container"
      :data-tutorial-category="category"
    >
      <!-- Category Header -->
      <button class="category-header" @click="toggleCategory(category as CategoryKey)">
        <span class="category-title">{{ categoryInfo.name }}</span>
        <el-icon class="category-icon">
          <ArrowDown v-if="isCategoryOpen(category as CategoryKey)" />
          <ArrowRight v-else />
        </el-icon>
      </button>

      <!-- Category Content -->
      <div
        v-if="isCategoryOpen(category as CategoryKey) && filteredNodes[category]"
        class="category-content"
      >
        <div
          v-for="node in filteredNodes[category]"
          :key="node.item"
          class="node-item"
          :data-tutorial-node="node.item"
          draggable="true"
          @dragstart="$emit('dragstart', $event, node)"
          @contextmenu.prevent="openNodeInfo(node, $event)"
        >
          <img :src="getImageUrl(node.image)" :alt="node.name" class="node-image" />
          <span class="node-name">{{ node.name }}</span>
          <button
            type="button"
            class="node-info-btn"
            :title="`About ${node.name}`"
            :aria-label="`About ${node.name}`"
            draggable="false"
            @click.stop="openNodeInfo(node, $event)"
            @mousedown.stop
            @dragstart.stop.prevent
          >
            <svg
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            >
              <circle cx="12" cy="12" r="10" />
              <line x1="12" y1="16" x2="12" y2="12" />
              <line x1="12" y1="8" x2="12.01" y2="8" />
            </svg>
          </button>
        </div>
      </div>
    </div>

    <!-- Node info popup: name, description and a docs link, surfaced by the per-item
         info button or by right-clicking a node. -->
    <NodeInfoCard
      v-if="nodeInfo"
      :name="nodeInfo.name"
      :intro="nodeInfo.intro"
      :docs-url="nodeInfo.docsUrl"
      :position="nodeInfo.position"
      @close="closeNodeInfo"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from "vue";
import { ArrowDown, ArrowRight } from "@element-plus/icons-vue";
import { getImageUrl } from "../../features/designer/utils";
import { useNodes } from "./useNodes";
import { NodeTemplate } from "../../types";
import NodeInfoCard from "./NodeInfoCard.vue";
import { nodeDocsUrl } from "./nodeDocs";

const { nodes } = useNodes();

type CategoryKey = "input" | "transform" | "combine" | "aggregate" | "ml" | "output" | "custom";

interface CategoryInfo {
  name: string;
  isOpen: boolean;
}

type Categories = {
  [K in CategoryKey]: CategoryInfo;
};

const categories: Categories = {
  input: { name: "Input Sources", isOpen: true },
  transform: { name: "Transformations", isOpen: true },
  combine: { name: "Combine Operations", isOpen: true },
  aggregate: { name: "Aggregations", isOpen: true },
  ml: { name: "Machine Learning", isOpen: true },
  output: { name: "Output Operations", isOpen: true },
  custom: { name: "User Defined Operations", isOpen: true },
};

const openCategories = ref<{ [K in CategoryKey]: boolean }>(
  Object.fromEntries(
    Object.keys(categories).map((key) => [key, categories[key as CategoryKey].isOpen]),
  ) as { [K in CategoryKey]: boolean },
);

const groupedNodes = computed(() => {
  return nodes.value.reduce(
    (acc, node) => {
      const group = node.node_group as CategoryKey;
      if (!acc[group]) {
        acc[group] = [];
      }
      acc[group].push(node);
      return acc;
    },
    {} as Record<CategoryKey, NodeTemplate[]>,
  );
});

const searchQuery = ref("");

const filteredNodes = computed(() => {
  if (!searchQuery.value) return groupedNodes.value;

  const query = searchQuery.value.toLowerCase();
  const filtered = {} as Record<CategoryKey, NodeTemplate[]>;
  for (const category in groupedNodes.value) {
    const nodesArray = groupedNodes.value[category as CategoryKey];
    const filteredArray = nodesArray.filter(
      (node) =>
        node.name.toLowerCase().includes(query) ||
        (node.tags ?? []).some((tag) => tag.toLowerCase().includes(query)),
    );
    if (filteredArray.length) {
      filtered[category as CategoryKey] = filteredArray;
    }
  }
  return filtered;
});

const isCategoryOpen = (category: CategoryKey) => {
  if (searchQuery.value) return !!filteredNodes.value[category];
  return openCategories.value[category];
};

const toggleCategory = (category: CategoryKey) => {
  if (searchQuery.value) return;
  openCategories.value[category] = !openCategories.value[category];
};

// Node info popup, surfaced by a node's info button or by right-clicking it. Anchors
// at the click position (clamped to the viewport inside NodeInfoCard).
const nodeInfo = ref<{
  name: string;
  intro: string;
  docsUrl: string;
  position: { x: number; y: number };
} | null>(null);

const openNodeInfo = (node: NodeTemplate, event: MouseEvent) => {
  nodeInfo.value = {
    name: node.name,
    intro: node.drawer_intro ?? "",
    docsUrl: nodeDocsUrl(node.node_group),
    position: { x: event.clientX, y: event.clientY },
  };
};

const closeNodeInfo = () => {
  nodeInfo.value = null;
};

defineEmits(["dragstart"]);
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

.category-icon {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.category-content {
  display: flex;
  flex-direction: column;
  background-color: var(--color-background-primary);
}

.node-item {
  display: flex;
  align-items: center;
  padding: var(--spacing-2) var(--spacing-4);
  cursor: pointer;
  user-select: none;
  transition: background-color var(--transition-fast);
  border-bottom: 1px solid var(--color-border-light);
  height: 32px;
}

.node-item:last-child {
  border-bottom: none;
}

.node-item:hover {
  background-color: var(--color-background-tertiary);
}

.node-image {
  width: 24px;
  height: 24px;
  margin-right: var(--spacing-2-5);
}

.node-name {
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

/* Info affordance: hidden until the row is hovered (or the button is focused),
   so it stays out of the way while keeping node docs one click away. */
.node-info-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  width: 22px;
  height: 22px;
  margin-left: auto;
  padding: 0;
  border: none;
  background: transparent;
  border-radius: var(--border-radius-md);
  color: var(--color-text-muted);
  cursor: pointer;
  opacity: 0;
  transition:
    opacity var(--transition-fast),
    background-color var(--transition-fast),
    color var(--transition-fast);
}

.node-item:hover .node-info-btn {
  opacity: 1;
}

.node-info-btn:hover {
  background-color: var(--color-background-muted);
  color: var(--color-accent);
}

.node-info-btn:focus-visible {
  opacity: 1;
  outline: 2px solid var(--color-accent);
  outline-offset: 1px;
}

.node-info-btn svg {
  width: 14px;
  height: 14px;
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
