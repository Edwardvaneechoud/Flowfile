<template>
  <div class="flow-picker">
    <div class="flow-picker-pane">
      <div class="pane-header">Namespace</div>
      <div v-if="treeLoading" class="pane-status">Loading namespaces...</div>
      <div v-else-if="treeError" class="pane-status error">{{ treeError }}</div>
      <div v-else class="pane-body">
        <namespace-tree-item
          v-for="node in tree"
          :key="node.id"
          :node="node"
          :selected-id="selectedNamespaceId"
          :initially-expanded="true"
          @select="handleSelectNamespace"
        />
      </div>
    </div>
    <div class="flow-picker-pane">
      <div class="pane-header">Flows</div>
      <div v-if="!selectedNamespaceId" class="pane-status">
        Select a namespace to see its flows.
      </div>
      <div v-else-if="flowsLoading" class="pane-status">Loading flows...</div>
      <div v-else-if="flowsError" class="pane-status error">{{ flowsError }}</div>
      <div v-else-if="flows.length === 0" class="pane-status">
        This namespace has no registered flows.
      </div>
      <div v-else class="pane-body flow-list">
        <button
          v-for="flow in flows"
          :key="flow.id"
          type="button"
          class="flow-row"
          :class="{ selected: flow.id === modelValue }"
          @click="handleSelectFlow(flow)"
        >
          <i class="fa-regular fa-file-lines flow-icon"></i>
          <div class="flow-meta">
            <div class="flow-name">{{ flow.name }}</div>
            <div class="flow-path">{{ flow.flow_path }}</div>
          </div>
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, onMounted } from "vue";
import NamespaceTreeItem from "./NamespaceTreeItem.vue";
import { CatalogApi } from "../../../api";
import type { FlowRegistration, NamespaceTree } from "../../../types";

const props = defineProps<{
  modelValue: number | null;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: number | null): void;
  (e: "select-flow", payload: { flow: FlowRegistration; namespaceName: string | null }): void;
  (e: "select-namespace", payload: { namespaceId: number; namespaceName: string | null }): void;
  (e: "flows-loaded", flows: FlowRegistration[]): void;
}>();

const tree = ref<NamespaceTree[]>([]);
const treeLoading = ref(false);
const treeError = ref<string | null>(null);

const selectedNamespaceId = ref<number | null>(null);
const flows = ref<FlowRegistration[]>([]);
const flowsLoading = ref(false);
const flowsError = ref<string | null>(null);

const findNamespaceName = (nodes: NamespaceTree[], targetId: number): string | null => {
  for (const node of nodes) {
    if (node.id === targetId) return node.name;
    if (node.children?.length) {
      const hit = findNamespaceName(node.children, targetId);
      if (hit) return hit;
    }
  }
  return null;
};

/**
 * Pick a sensible default namespace on mount so the "save as new" path has a
 * pre-filled target — mirrors ``CatalogNamespacePicker``'s default logic:
 * prefer ``General > Local Flows``, fall back to ``General > default``.
 */
const findDefaultNamespace = (nodes: NamespaceTree[]): NamespaceTree | null => {
  for (const node of nodes) {
    if (node.name === "General" && node.parent_id === null) {
      const local = node.children.find((c) => c.name === "Local Flows");
      if (local) return local;
      const def = node.children.find((c) => c.name === "default");
      if (def) return def;
    }
  }
  return null;
};

const loadTree = async () => {
  treeLoading.value = true;
  treeError.value = null;
  try {
    tree.value = await CatalogApi.getNamespaceTree();
    if (selectedNamespaceId.value === null) {
      const defaultNs = findDefaultNamespace(tree.value);
      if (defaultNs) {
        handleSelectNamespace(defaultNs.id);
      }
    }
  } catch (err: unknown) {
    console.error("Failed to load namespace tree", err);
    treeError.value = "Failed to load namespaces";
  } finally {
    treeLoading.value = false;
  }
};

const loadFlows = async (namespaceId: number) => {
  flowsLoading.value = true;
  flowsError.value = null;
  try {
    flows.value = await CatalogApi.getFlows(namespaceId);
    emit("flows-loaded", flows.value);
  } catch (err: unknown) {
    console.error("Failed to load catalog flows", err);
    flowsError.value = "Failed to load flows";
    flows.value = [];
    emit("flows-loaded", []);
  } finally {
    flowsLoading.value = false;
  }
};

const handleSelectNamespace = (namespaceId: number) => {
  selectedNamespaceId.value = namespaceId;
  // Changing namespace invalidates any previously picked flow.
  if (props.modelValue !== null) {
    emit("update:modelValue", null);
  }
  emit("select-namespace", {
    namespaceId,
    namespaceName: findNamespaceName(tree.value, namespaceId),
  });
  loadFlows(namespaceId);
};

const handleSelectFlow = (flow: FlowRegistration) => {
  emit("update:modelValue", flow.id);
  const namespaceName =
    flow.namespace_id != null ? findNamespaceName(tree.value, flow.namespace_id) : null;
  emit("select-flow", { flow, namespaceName });
};

watch(
  () => props.modelValue,
  (newValue) => {
    // Clear namespace-level selection if the parent resets modelValue.
    if (newValue === null && flows.value.every((f) => f.id !== newValue)) {
      // intentionally no-op: namespace stays selected so the flow list is still visible.
    }
  },
);

onMounted(loadTree);

defineExpose({ reload: loadTree });
</script>

<style scoped>
.flow-picker {
  display: flex;
  gap: var(--spacing-3);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-primary);
  overflow: hidden;
  min-height: 320px;
}

.flow-picker-pane {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  background-color: var(--color-background-primary);
}

.flow-picker-pane + .flow-picker-pane {
  border-left: 1px solid var(--color-border-light);
}

.pane-header {
  padding: var(--spacing-2) var(--spacing-3);
  font-size: var(--font-size-xs, 11px);
  font-weight: var(--font-weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--color-text-muted);
  border-bottom: 1px solid var(--color-border-light);
  background-color: var(--color-background-muted, #f9f9fb);
}

.pane-body {
  padding: var(--spacing-2);
  overflow-y: auto;
  max-height: 320px;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.pane-status {
  padding: var(--spacing-3);
  text-align: center;
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
}

.pane-status.error {
  color: var(--color-danger, #e53935);
}

.flow-list {
  gap: var(--spacing-1);
}

.flow-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2);
  border: 1px solid transparent;
  border-radius: var(--border-radius-sm);
  background: transparent;
  text-align: left;
  cursor: pointer;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  transition: background-color var(--transition-fast);
}

.flow-row:hover {
  background-color: var(--color-background-tertiary);
}

.flow-row.selected {
  background-color: var(--color-accent-subtle, #e8f0fe);
  border-color: var(--color-accent, #1976d2);
  color: var(--color-accent, #1976d2);
  font-weight: var(--font-weight-medium);
}

.flow-icon {
  font-size: 13px;
  color: var(--color-text-secondary);
  flex-shrink: 0;
}

.flow-row.selected .flow-icon {
  color: var(--color-accent, #1976d2);
}

.flow-meta {
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.flow-name {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.flow-path {
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-muted);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  font-family: var(--font-family-mono);
}
</style>
