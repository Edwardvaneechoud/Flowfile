<template>
  <div class="tree-node">
    <div class="tree-row" :class="{ expanded }" @click="toggle">
      <i :class="expanded ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'" class="chevron"></i>
      <i :class="node.level === 0 ? 'fa-solid fa-box-archive' : 'fa-solid fa-layer-group'" class="ns-icon"></i>
      <span class="ns-name">{{ node.name }}</span>
      <span class="ns-count" v-if="totalFlows > 0">{{ totalFlows }}</span>
      <div class="tree-actions" @click.stop>
        <button
          v-if="node.level === 0"
          class="action-btn"
          title="Add schema"
          @click="$emit('createSchema', node.id)"
        >
          <i class="fa-solid fa-plus"></i>
        </button>
        <button
          v-if="node.level === 1"
          class="action-btn"
          title="Register flow"
          @click="$emit('registerFlow', node.id)"
        >
          <i class="fa-solid fa-file-circle-plus"></i>
        </button>
      </div>
    </div>

    <div v-if="expanded" class="tree-children">
      <!-- Child namespaces (schemas) -->
      <CatalogTreeNode
        v-for="child in node.children"
        :key="child.id"
        :node="child"
        :selected-flow-id="selectedFlowId"
        @select-flow="$emit('selectFlow', $event)"
        @toggle-favorite="$emit('toggleFavorite', $event)"
        @toggle-follow="$emit('toggleFollow', $event)"
        @register-flow="$emit('registerFlow', $event)"
        @create-schema="$emit('createSchema', $event)"
      />

      <!-- Flows under this namespace -->
      <div
        v-for="flow in node.flows"
        :key="'f-' + flow.id"
        class="tree-flow"
        :class="{ selected: selectedFlowId === flow.id, 'file-missing': !flow.file_exists }"
        @click.stop="$emit('selectFlow', flow.id)"
      >
        <i class="fa-solid fa-diagram-project flow-icon"></i>
        <span class="flow-name">{{ flow.name }}</span>
        <i
          v-if="!flow.file_exists"
          class="fa-solid fa-triangle-exclamation missing-icon"
          title="Flow file not found on disk"
        ></i>
        <div class="flow-actions" @click.stop>
          <button
            class="action-btn star-btn"
            :class="{ active: flow.is_favorite }"
            :title="flow.is_favorite ? 'Unfavorite' : 'Favorite'"
            @click="$emit('toggleFavorite', flow.id)"
          >
            <i :class="flow.is_favorite ? 'fa-solid fa-star' : 'fa-regular fa-star'"></i>
          </button>
          <span
            v-if="flow.last_run_success !== null"
            class="run-indicator"
            :class="flow.last_run_success ? 'success' : 'failure'"
            :title="flow.last_run_success ? 'Last run succeeded' : 'Last run failed'"
          ></span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import type { NamespaceTree } from "../../types";

const props = defineProps<{
  node: NamespaceTree;
  selectedFlowId: number | null;
}>();

defineEmits<{
  selectFlow: [id: number];
  toggleFavorite: [id: number];
  toggleFollow: [id: number];
  registerFlow: [namespaceId: number];
  createSchema: [parentId: number];
}>();

const expanded = ref(true);
const toggle = () => { expanded.value = !expanded.value; };

const totalFlows = computed(() => {
  let count = props.node.flows.length;
  for (const child of props.node.children) {
    count += child.flows.length;
  }
  return count;
});
</script>

<style scoped>
.tree-node {
  user-select: none;
}

.tree-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-2);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: background var(--transition-fast);
}

.tree-row:hover {
  background: var(--color-background-hover);
}

.chevron {
  width: 14px;
  font-size: 10px;
  color: var(--color-text-muted);
  text-align: center;
  flex-shrink: 0;
}

.ns-icon {
  color: var(--color-accent);
  font-size: var(--font-size-sm);
  flex-shrink: 0;
}

.ns-name {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  flex: 1;
}

.ns-count {
  font-size: 11px;
  color: var(--color-text-muted);
  background: var(--color-background-tertiary);
  padding: 0 6px;
  border-radius: var(--border-radius-full);
  line-height: 18px;
}

.tree-actions {
  display: flex;
  gap: 2px;
  opacity: 0;
  transition: opacity var(--transition-fast);
}

.tree-row:hover .tree-actions {
  opacity: 1;
}

.tree-children {
  padding-left: var(--spacing-4);
}

.tree-flow {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-2);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.tree-flow:hover {
  background: var(--color-background-hover);
}

.tree-flow.selected {
  background: var(--color-primary-light, rgba(59, 130, 246, 0.1));
}

.flow-icon {
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
  flex-shrink: 0;
}

.flow-name {
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  flex: 1;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.flow-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  opacity: 0;
  transition: opacity var(--transition-fast);
}

.tree-flow:hover .flow-actions,
.tree-flow.selected .flow-actions {
  opacity: 1;
}

.action-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
  border-radius: var(--border-radius-sm);
  font-size: 12px;
  transition: all var(--transition-fast);
}

.action-btn:hover {
  background: var(--color-background-tertiary);
  color: var(--color-primary);
}

.star-btn.active {
  color: #f59e0b;
}

.run-indicator {
  width: 8px;
  height: 8px;
  border-radius: var(--border-radius-full);
  flex-shrink: 0;
}

.run-indicator.success { background: #22c55e; }
.run-indicator.failure { background: #ef4444; }

.tree-flow.file-missing { opacity: 0.55; }
.tree-flow.file-missing .flow-icon { color: #f59e0b; }

.missing-icon {
  font-size: 11px;
  color: #f59e0b;
  flex-shrink: 0;
}
</style>
