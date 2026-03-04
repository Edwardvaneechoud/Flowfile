<template>
  <div v-if="visible" class="modal-overlay" @click.self="$emit('close')">
    <div class="modal-card modal-card-lg">
      <h3>Register Table</h3>
      <input v-model="name" class="input-field" placeholder="Table name" />
      <input v-model="description" class="input-field" placeholder="Description (optional)" />
      <div class="namespace-selector">
        <label class="field-label">Catalog / Schema</label>
        <select v-model="selectedNamespaceId" class="input-field">
          <option v-for="ns in schemaNamespaces" :key="ns.id" :value="ns.id">
            {{ ns.label }}
          </option>
        </select>
        <p v-if="schemaNamespaces.length === 0" class="ns-hint">
          No schemas available. Create a catalog and schema first.
        </p>
      </div>
      <div class="file-browser-section">
        <label class="field-label">Source data file</label>
        <div v-if="path" class="selected-file-badge">
          <i class="fa-solid fa-table"></i>
          <span>{{ path }}</span>
          <button class="clear-file-btn" title="Clear" @click="path = ''">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>
        <div class="file-browser-container">
          <FileBrowser
            :allowed-file-types="['csv', 'txt', 'tsv', 'parquet', 'xlsx', 'xls']"
            mode="open"
            context="dataFiles"
            :is-visible="visible"
            @file-selected="handleFileSelected"
            @update:model-value="handleFileUpdate"
          />
        </div>
      </div>
      <div class="modal-actions">
        <button class="btn-secondary" @click="$emit('close')">Cancel</button>
        <button
          class="btn-primary"
          :disabled="!name.trim() || !path.trim()"
          @click="submit"
        >
          Register
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";
import FileBrowser from "../../components/common/FileBrowser/fileBrowser.vue";

const props = defineProps<{
  visible: boolean;
  namespaceId: number | null;
  defaultNamespaceId: number | null;
}>();

const emit = defineEmits<{
  close: [];
}>();

const catalogStore = useCatalogStore();
const name = ref("");
const path = ref("");
const description = ref("");
const selectedNamespaceId = ref<number | null>(null);

/** Collect all schema-level (level 1) namespaces from the tree for the dropdown. */
const schemaNamespaces = computed(() => {
  const result: { id: number; label: string }[] = [];
  for (const catalog of catalogStore.tree) {
    for (const schema of catalog.children) {
      result.push({ id: schema.id, label: `${catalog.name} / ${schema.name}` });
    }
  }
  return result;
});

watch(
  () => props.visible,
  (val) => {
    if (val) {
      name.value = "";
      path.value = "";
      description.value = "";
      selectedNamespaceId.value = props.namespaceId;
    }
  },
);

function handleFileSelected(fileInfo: { name: string; path: string }) {
  path.value = fileInfo.path;
  if (!name.value.trim()) {
    const baseName = fileInfo.name.replace(/\.(csv|txt|tsv|parquet|xlsx|xls)$/i, "");
    name.value = baseName;
  }
}

/** Capture single-click file selection from the FileBrowser. */
function handleFileUpdate(file: { name: string; path: string; is_directory: boolean } | null) {
  if (file && !file.is_directory) {
    handleFileSelected(file);
  }
}

async function submit() {
  if (!name.value.trim() || !path.value.trim()) return;
  try {
    const nsId = selectedNamespaceId.value ?? props.defaultNamespaceId;
    await CatalogApi.registerTable(
      {
        name: name.value.trim(),
        description: description.value.trim() || null,
        namespace_id: nsId,
      },
      path.value.trim(),
    );
    emit("close");
    await Promise.all([
      catalogStore.loadTree(),
      catalogStore.loadAllTables(),
      catalogStore.loadStats(),
    ]);
  } catch (e: any) {
    alert(e?.response?.data?.detail ?? "Failed to register table");
  }
}
</script>

<style scoped>
.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.4);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal-card {
  background: var(--color-background-primary);
  border-radius: var(--border-radius-lg);
  padding: var(--spacing-6);
  width: 400px;
  max-width: 90vw;
  box-shadow: var(--shadow-lg);
}

.modal-card-lg {
  width: 700px;
  max-height: 85vh;
  overflow-y: auto;
}

.modal-card h3 {
  margin: 0 0 var(--spacing-4) 0;
  font-size: var(--font-size-lg);
}

.input-field {
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  margin-bottom: var(--spacing-3);
  box-sizing: border-box;
}

.input-field:focus {
  outline: none;
  border-color: var(--color-primary);
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.15);
}

.modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-2);
  margin-top: var(--spacing-2);
}

.btn-primary {
  padding: var(--spacing-2) var(--spacing-4);
  background: var(--color-primary);
  color: #fff;
  border: none;
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  cursor: pointer;
  transition: opacity var(--transition-fast);
}

.btn-primary:hover {
  opacity: 0.9;
}
.btn-primary:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.btn-secondary {
  padding: var(--spacing-2) var(--spacing-4);
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  cursor: pointer;
}

.btn-secondary:hover {
  background: var(--color-background-hover);
}

.file-browser-section {
  margin-bottom: var(--spacing-3);
}

.field-label {
  display: block;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin-bottom: var(--spacing-2);
}

.selected-file-badge {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  background: rgba(59, 130, 246, 0.08);
  border: 1px solid rgba(59, 130, 246, 0.25);
  border-radius: var(--border-radius-md);
  margin-bottom: var(--spacing-2);
  font-size: var(--font-size-sm);
  color: var(--color-primary);
}

.selected-file-badge span {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-family: monospace;
  font-size: var(--font-size-xs);
}

.clear-file-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 20px;
  height: 20px;
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
  border-radius: var(--border-radius-sm);
  font-size: 10px;
}

.clear-file-btn:hover {
  color: var(--color-text-primary);
  background: var(--color-background-hover);
}

.file-browser-container {
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  height: 350px;
  overflow: hidden;
}

.namespace-selector {
  margin-bottom: var(--spacing-3);
}

.namespace-selector select {
  appearance: auto;
}

.ns-hint {
  margin: var(--spacing-1) 0 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}
</style>
