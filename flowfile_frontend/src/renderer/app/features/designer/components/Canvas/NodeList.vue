<template>
  <div class="nodes-wrapper">
    <!-- Search Input -->
    <input
      type="text"
      v-model="searchQuery"
      placeholder="Search nodes..."
      class="search-input"
    />

    <div
      v-for="(categoryInfo, category) in categories"
      :key="category"
      class="category-container"
    >
      <!-- Category Header -->
      <button
        class="category-header"
        @click="toggleCategory(category as CategoryKey)"
      >
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
          <img
            :src="getImageUrl(node.image)"
            :alt="node.name"
            class="node-image"
          />
          <span class="node-name">{{ node.name }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from "vue";
import { ArrowDown, ArrowRight } from "@element-plus/icons-vue";
import { getImageUrl } from "../../utils";
import { useNodes } from "./useNodes";
import { NodeTemplate } from "../../types";

const { nodes } = useNodes();

type CategoryKey = "input" | "transform" | "combine" | "aggregate" | "output";

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
};

const openCategories = ref<{ [K in CategoryKey]: boolean }>(
  Object.fromEntries(
    Object.keys(categories).map((key) => [
      key,
      categories[key as CategoryKey].isOpen,
    ])
  ) as { [K in CategoryKey]: boolean }
);

const groupedNodes = computed(() => {
  return nodes.value.reduce((acc, node) => {
    const group = node.node_group as CategoryKey;
    if (!acc[group]) {
      acc[group] = [];
    }
    acc[group].push(node);
    return acc;
  }, {} as Record<CategoryKey, NodeTemplate[]>);
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
    const filteredArray = nodesArray.filter((node) =>
      node.name.toLowerCase().includes(query)
    );
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
  gap: 5px;
  padding: 6px;
  background-color: #fff;
  border-radius: 8px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
}

/* Style for search input */
.search-input {
  padding: 8px 16px;
  margin-bottom: 8px;
  border: 1px solid #ccc;
  border-radius: 4px;
}

.category-container {
  overflow: hidden;
  border-radius: 4px;
}

.category-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  padding: 8px 16px;
  background-color: #fafafa;
  border: none;
  cursor: pointer;
  transition: background-color 0.2s ease;
  height: 32px;
}

.category-header:hover {
  background-color: #f5f5f5;
}

.category-title {
  font-size: small;
  font-weight: 200;
  color: #333;
  text-align: left;
}

.category-icon {
  font-size: 12px;
  color: #666;
}

.category-content {
  display: flex;
  flex-direction: column;
  background-color: #fff;
}

.node-item {
  display: flex;
  align-items: center;
  padding: 8px 16px;
  cursor: pointer;
  user-select: none;
  transition: background-color 0.2s ease;
  border-bottom: 1px solid #eee;
  height: 32px;
}

.node-item:last-child {
  border-bottom: none;
}

.node-item:hover {
  background-color: #f5f5f5;
}

.node-image {
  width: 24px;
  height: 24px;
  margin-right: 10px;
}

.node-name {
  font-size: 12px;
  color: #333;
}

/* Custom scrollbar */
.nodes-wrapper::-webkit-scrollbar {
  width: 8px;
}

.nodes-wrapper::-webkit-scrollbar-track {
  background: transparent;
}

.nodes-wrapper::-webkit-scrollbar-thumb {
  background-color: rgba(0, 0, 0, 0.1);
  border-radius: 4px;
}

.nodes-wrapper::-webkit-scrollbar-thumb:hover {
  background-color: rgba(0, 0, 0, 0.2);
}
</style>
