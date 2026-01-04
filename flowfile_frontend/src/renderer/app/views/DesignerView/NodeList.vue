<template>
  <div class="nodes-wrapper">
    <!-- Search Input -->
    <input v-model="searchQuery" type="text" placeholder="Search nodes..." class="search-input" />

    <div v-for="(categoryInfo, category) in categories" :key="category" class="category-container">
      <!-- Category Header -->
      <button class="category-header" @click="toggleCategory(category as CategoryKey)">
        <span class="category-title">{{ categoryInfo.name }}</span>
        <el-icon class="category-icon">
          <ArrowDown v-if="openCategories[category as CategoryKey]" />
          <ArrowRight v-else />
        </el-icon>
      </button>

      <!-- Category Content -->
      <div
        v-if="openCategories[category as CategoryKey] && filteredNodes[category]"
        class="category-content"
      >
        <div
          v-for="node in filteredNodes[category]"
          :key="node.item"
          class="node-item"
          draggable="true"
          @dragstart="$emit('dragstart', $event, node)"
        >
          <img :src="getImageUrl(node.image)" :alt="node.name" class="node-image" />
          <span class="node-name">{{ node.name }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from "vue";
import { ArrowDown, ArrowRight } from "@element-plus/icons-vue";
import { getImageUrl } from "../../features/designer/utils";
import { useNodes } from "./useNodes";
import { NodeTemplate } from "../../types";

const { nodes } = useNodes();

type CategoryKey = "input" | "transform" | "combine" | "aggregate" | "output" | "custom";

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

// Reactive search query
const searchQuery = ref("");

// Compute filtered nodes based on the search query
const filteredNodes = computed(() => {
  // If no search query, return all grouped nodes
  if (!searchQuery.value) return groupedNodes.value;

  const query = searchQuery.value.toLowerCase();
  const filtered = {} as Record<CategoryKey, NodeTemplate[]>;
  // Loop through each category and filter nodes
  for (const category in groupedNodes.value) {
    const nodesArray = groupedNodes.value[category as CategoryKey];
    const filteredArray = nodesArray.filter((node) => node.name.toLowerCase().includes(query));
    if (filteredArray.length) {
      filtered[category as CategoryKey] = filteredArray;
    }
  }
  return filtered;
});

const toggleCategory = (category: CategoryKey) => {
  openCategories.value[category] = !openCategories.value[category];
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
