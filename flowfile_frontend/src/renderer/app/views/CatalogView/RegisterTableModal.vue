<template>
  <div v-if="visible" class="modal-overlay" @click.self="$emit('close')">
    <div class="modal-card">
      <div class="modal-header">
        <h3>Register Table</h3>
        <button class="close-btn" @click="$emit('close')">
          <i class="fa-solid fa-xmark"></i>
        </button>
      </div>
      <div class="modal-body">
        <aside class="form-panel">
          <div class="form-group">
            <label class="field-label">Table name</label>
            <input v-model="name" class="input-field" placeholder="Table name" />
          </div>
          <div class="form-group">
            <label class="field-label">Description</label>
            <input v-model="description" class="input-field" placeholder="Optional" />
          </div>
          <div class="form-group">
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
          <div v-if="path" class="selected-file-badge">
            <i class="fa-solid fa-table"></i>
            <span>{{ fileName }}</span>
            <button class="clear-file-btn" title="Clear" @click="path = ''">
              <i class="fa-solid fa-xmark"></i>
            </button>
          </div>
          <div v-else class="no-file-hint">
            <i class="fa-solid fa-arrow-right"></i>
            <span>Select a Parquet file from the browser</span>
          </div>
          <p class="parquet-hint">
            Only Parquet files can be registered directly. For CSV, Excel, or other formats, use a
            Flow to transform and output to the catalog.
          </p>
        </aside>
        <div class="browser-panel">
          <div class="file-browser-container">
            <FileBrowser
              :allowed-file-types="['parquet']"
              mode="open"
              context="dataFiles"
              :is-visible="visible"
              @file-selected="handleFileSelected"
              @update:model-value="handleFileUpdate"
            />
          </div>
        </div>
      </div>
      <div class="modal-footer">
        <button class="btn-secondary" @click="$emit('close')">Cancel</button>
        <button
          class="btn-primary"
          :disabled="!name.trim() || !path.trim() || submitting"
          @click="submit"
        >
          {{ submitting ? "Registering..." : "Register" }}
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";
import FileBrowser from "../../components/common/FileBrowser/fileBrowser.vue";

const props = defineProps<{
  visible: boolean;
  namespaceId: number | null;
  defaultNamespaceId: number | null;
}>();

const emit = defineEmits(["close"]);

const catalogStore = useCatalogStore();
const name = ref("");
const path = ref("");
const description = ref("");
const selectedNamespaceId = ref<number | null>(null);
const submitting = ref(false);

const fileName = computed(() => {
  if (!path.value) return "";
  const parts = path.value.replace(/\\/g, "/").split("/");
  return parts[parts.length - 1] || path.value;
});

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
    const baseName = fileInfo.name.replace(/\.parquet$/i, "");
    name.value = baseName;
  }
}

function handleFileUpdate(file: { name: string; path: string; is_directory: boolean } | null) {
  if (file && !file.is_directory) {
    handleFileSelected(file);
  }
}

async function submit() {
  if (!name.value.trim() || !path.value.trim()) return;
  submitting.value = true;
  try {
    const nsId = selectedNamespaceId.value ?? props.defaultNamespaceId;
    await CatalogApi.registerTable({
      name: name.value.trim(),
      file_path: path.value.trim(),
      description: description.value.trim() || null,
      namespace_id: nsId,
    });
    ElMessage.success("Table registered successfully");
    emit("close");
    await Promise.all([
      catalogStore.loadTree(),
      catalogStore.loadAllTables(),
      catalogStore.loadStats(),
    ]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to register table");
  } finally {
    submitting.value = false;
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
  box-shadow: var(--shadow-lg);
  width: 90vw;
  max-width: 1400px;
  height: 75vh;
  max-height: 800px;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.modal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-4) var(--spacing-5);
  border-bottom: 1px solid var(--color-border-light);
  flex-shrink: 0;
}

.modal-header h3 {
  margin: 0;
  font-size: var(--font-size-lg);
}

.close-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
  border-radius: var(--border-radius-sm);
  font-size: 14px;
}

.close-btn:hover {
  color: var(--color-text-primary);
  background: var(--color-background-hover);
}

.modal-body {
  display: flex;
  flex: 1;
  min-height: 0;
  overflow: hidden;
}

.form-panel {
  width: 260px;
  flex-shrink: 0;
  padding: var(--spacing-4) var(--spacing-5);
  border-right: 1px solid var(--color-border-light);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  overflow-y: auto;
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}

.field-label {
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.input-field {
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  box-sizing: border-box;
}

.input-field:focus {
  outline: none;
  border-color: var(--color-primary);
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.15);
}

select.input-field {
  appearance: auto;
}

.ns-hint {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.selected-file-badge {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  background: rgba(59, 130, 246, 0.08);
  border: 1px solid rgba(59, 130, 246, 0.25);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  color: var(--color-primary);
  margin-top: auto;
}

.selected-file-badge span {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-family: var(--font-family-mono);
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
  flex-shrink: 0;
}

.clear-file-btn:hover {
  color: var(--color-text-primary);
  background: var(--color-background-hover);
}

.no-file-hint {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  border: 1px dashed var(--color-border-primary);
  border-radius: var(--border-radius-md);
  margin-top: auto;
}

.parquet-hint {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  line-height: 1.4;
}

.browser-panel {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
}

.file-browser-container {
  flex: 1;
  min-height: 0;
  overflow: hidden;
}

.modal-footer {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-2);
  padding: var(--spacing-3) var(--spacing-5);
  border-top: 1px solid var(--color-border-light);
  flex-shrink: 0;
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
</style>
