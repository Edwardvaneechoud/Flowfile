<template>
  <div class="register-row">
    <el-checkbox
      v-model="registered"
      title="Registered flows show up in the catalog and keep run history"
    >
      Also register in catalog
    </el-checkbox>
    <template v-if="registered">
      <span class="register-in">in</span>
      <el-popover ref="popoverRef" trigger="click" placement="top-start" :width="340">
        <template #reference>
          <button type="button" class="ns-chip" title="Change the catalog namespace">
            <i class="fa-solid fa-layer-group ns-chip-icon"></i>
            <span class="ns-chip-label">{{ chipLabel }}</span>
            <i class="fa-solid fa-chevron-down ns-chip-caret"></i>
          </button>
        </template>
        <div class="ns-popover-body">
          <div v-if="loading" class="ns-status">Loading namespaces...</div>
          <div v-else-if="error" class="ns-status ns-status-error">{{ error }}</div>
          <div v-else class="ns-tree">
            <namespace-tree-item
              v-for="node in tree"
              :key="node.id"
              :node="node"
              :selected-id="namespaceId"
              :initially-expanded="true"
              :writable-only="true"
              @select="handleSelect"
            />
          </div>
        </div>
      </el-popover>
    </template>
  </div>
</template>

<script setup lang="ts">
/**
 * One-line "Also register in catalog" option for the Save/Create dialogs'
 * File System tab: a checkbox plus a compact chip showing the target
 * namespace at a glance, with the full namespace tree tucked into a popover
 * instead of permanently occupying the dialog.
 */
import { computed, onMounted, ref } from "vue";
import NamespaceTreeItem from "./NamespaceTreeItem.vue";
import { CatalogApi } from "../../../api";
import {
  filterSelectableNamespaces,
  findDefaultSaveNamespace,
  findNamespacePath,
} from "../../../types";
import type { NamespaceTree } from "../../../types";
import { useWritableNamespaces } from "../../../composables/useWritableNamespaces";

const props = defineProps<{
  modelValue: boolean;
  namespaceId: number | null;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (e: "update:namespaceId", value: number | null): void;
}>();

const { isWritableNamespace } = useWritableNamespaces();

const registered = computed({
  get: () => props.modelValue,
  set: (value: boolean) => emit("update:modelValue", value),
});

const tree = ref<NamespaceTree[]>([]);
const loading = ref(false);
const error = ref<string | null>(null);
const popoverRef = ref<{ hide: () => void } | null>(null);

const loadTree = async () => {
  loading.value = true;
  error.value = null;
  try {
    tree.value = filterSelectableNamespaces(await CatalogApi.getNamespaceTree());
    if (props.namespaceId === null) {
      const defaultNs = findDefaultSaveNamespace(tree.value, isWritableNamespace);
      if (defaultNs) emit("update:namespaceId", defaultNs.id);
    }
  } catch (err) {
    console.error("Failed to load namespace tree", err);
    error.value = "Failed to load namespaces";
  } finally {
    loading.value = false;
  }
};

const chipLabel = computed(() => {
  if (props.namespaceId !== null) {
    const path = findNamespacePath(tree.value, props.namespaceId);
    if (path.length) return path.join(" / ");
  }
  return loading.value ? "Loading..." : "Choose namespace";
});

const handleSelect = (namespaceId: number) => {
  emit("update:namespaceId", namespaceId);
  popoverRef.value?.hide();
};

// Dotted ancestry label (e.g. "General.default") — same contract as
// CatalogNamespacePicker so callers can build catalog refs from either picker.
const getNamespacePath = (id: number | null): string | null => {
  if (id === null) return null;
  const path = findNamespacePath(tree.value, id);
  return path.length ? path.join(".") : null;
};

onMounted(loadTree);

defineExpose({ reload: loadTree, getNamespacePath });
</script>

<style scoped>
.register-row {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-3);
  min-height: 28px;
}

.register-in {
  font-size: var(--font-size-sm);
  color: var(--color-text-muted);
}

.ns-chip {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  max-width: 320px;
  padding: 3px var(--spacing-3);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-tertiary);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  font-family: inherit;
  cursor: pointer;
  transition:
    border-color var(--transition-fast),
    color var(--transition-fast);
}

.ns-chip:hover {
  border-color: var(--color-accent, #1976d2);
  color: var(--color-accent, #1976d2);
}

.ns-chip-icon {
  font-size: 11px;
  color: var(--color-text-secondary);
}

.ns-chip:hover .ns-chip-icon {
  color: var(--color-accent, #1976d2);
}

.ns-chip-label {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.ns-chip-caret {
  font-size: 9px;
  color: var(--color-text-muted);
}

.ns-popover-body {
  max-height: 300px;
  overflow-y: auto;
}

.ns-status {
  padding: var(--spacing-3);
  text-align: center;
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
}

.ns-status-error {
  color: var(--color-danger, #e53935);
}

.ns-tree {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
</style>
