<template>
  <div class="namespace-picker">
    <div v-if="loading" class="ns-loading">Loading namespaces...</div>
    <div v-else-if="error" class="ns-error">{{ error }}</div>
    <div v-else class="ns-tree">
      <namespace-tree-item
        v-for="node in tree"
        :key="node.id"
        :node="node"
        :selected-id="selectedId"
        :initially-expanded="true"
        @select="handleSelect"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from "vue";
import NamespaceTreeItem from "./NamespaceTreeItem.vue";
import { CatalogApi } from "../../../api";
import type { NamespaceTree } from "../../../types";

const props = defineProps<{
  modelValue: number | null;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: number | null): void;
  (e: "select", namespaceId: number): void;
}>();

const tree = ref<NamespaceTree[]>([]);
const loading = ref(false);
const error = ref<string | null>(null);
const selectedId = ref<number | null>(props.modelValue);

watch(
  () => props.modelValue,
  (newValue) => {
    selectedId.value = newValue;
  },
);

const loadTree = async () => {
  loading.value = true;
  error.value = null;
  try {
    tree.value = await CatalogApi.getNamespaceTree();
    // Auto-select the most appropriate "General" child when nothing is selected.
    if (selectedId.value === null) {
      const defaultNs = findDefaultNamespace(tree.value);
      if (defaultNs) {
        selectedId.value = defaultNs.id;
        emit("update:modelValue", defaultNs.id);
        emit("select", defaultNs.id);
      }
    }
  } catch (err: any) {
    console.error("Failed to load namespace tree", err);
    error.value = "Failed to load namespaces";
  } finally {
    loading.value = false;
  }
};

/**
 * Pick a sensible default namespace: prefer ``General > Local Flows``
 * (the home for disk-backed flows introduced alongside the save UX rework),
 * and fall back to ``General > default`` for older catalogs where the
 * Local Flows namespace hasn't been seeded yet.
 */
const findDefaultNamespace = (nodes: NamespaceTree[]): NamespaceTree | null => {
  for (const node of nodes) {
    if (node.name === "General" && node.parent_id === null) {
      const local = node.children.find((c) => c.name === "Local Flows");
      if (local) return local;
      const defaultChild = node.children.find((c) => c.name === "default");
      if (defaultChild) return defaultChild;
    }
  }
  return null;
};

const handleSelect = (namespaceId: number) => {
  selectedId.value = namespaceId;
  emit("update:modelValue", namespaceId);
  emit("select", namespaceId);
};

onMounted(() => {
  loadTree();
});

defineExpose({ reload: loadTree });
</script>

<style scoped>
.namespace-picker {
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-primary);
  padding: var(--spacing-2);
  max-height: 320px;
  overflow-y: auto;
  font-family: var(--font-family-base);
}

.ns-loading,
.ns-error {
  padding: var(--spacing-3);
  text-align: center;
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
}

.ns-error {
  color: var(--color-danger, #e53935);
}

.ns-tree {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
</style>
