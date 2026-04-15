<template>
  <div v-if="visible" class="modal-overlay" @click.self="$emit('close')">
    <div class="modal-card">
      <div class="modal-header">
        <h3>Create Virtual Table</h3>
        <button class="close-btn" @click="$emit('close')">
          <i class="fa-solid fa-xmark"></i>
        </button>
      </div>
      <div class="modal-body">
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
          <p v-if="schemaNamespaces.length === 0" class="hint">
            No schemas available. Create a catalog and schema first.
          </p>
        </div>
        <div class="form-group">
          <label class="field-label">Producer flow</label>
          <select v-model="selectedFlowId" class="input-field">
            <option :value="null" disabled>Select a registered flow</option>
            <option v-for="flow in flows" :key="flow.id" :value="flow.id">
              {{ flow.name }}
            </option>
          </select>
          <p v-if="flows.length === 0" class="hint">
            No registered flows available. Register a flow first.
          </p>
        </div>

        <div class="virtual-info">
          <i class="fa-solid fa-bolt"></i>
          <span
            >A virtual table stores no data on disk. When queried, it runs the selected flow
            on-demand to produce results.</span
          >
        </div>

        <!-- Laziness blockers shown after creation -->
        <div v-if="createdTable?.laziness_blockers?.length" class="laziness-blockers">
          <div class="blockers-header">
            <i class="fa-solid fa-circle-info"></i>
            <span
              >This virtual table is <strong>not optimized</strong>. The following nodes prevent
              full lazy execution:</span
            >
          </div>
          <ul class="blocker-list">
            <li v-for="(reason, i) in createdTable.laziness_blockers" :key="i">{{ reason }}</li>
          </ul>
        </div>
      </div>
      <div class="modal-footer">
        <button class="btn-secondary" @click="$emit('close')">
          {{ createdTable ? "Done" : "Cancel" }}
        </button>
        <button
          v-if="!createdTable"
          class="btn-primary"
          :disabled="!canSubmit || submitting"
          @click="submit"
        >
          {{ submitting ? "Creating..." : "Create" }}
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
import type { CatalogTable, FlowRegistration } from "../../types";

const props = defineProps<{
  visible: boolean;
  namespaceId: number | null;
  defaultNamespaceId: number | null;
}>();

defineEmits(["close"]);

const catalogStore = useCatalogStore();
const name = ref("");
const description = ref("");
const selectedNamespaceId = ref<number | null>(null);
const selectedFlowId = ref<number | null>(null);
const submitting = ref(false);
const createdTable = ref<CatalogTable | null>(null);

const flows = computed((): FlowRegistration[] => catalogStore.allFlows);

const schemaNamespaces = computed(() => {
  const result: { id: number; label: string }[] = [];
  for (const catalog of catalogStore.tree) {
    for (const schema of catalog.children) {
      result.push({ id: schema.id, label: `${catalog.name} / ${schema.name}` });
    }
  }
  return result;
});

const canSubmit = computed(() => {
  return name.value.trim() && selectedFlowId.value !== null;
});

watch(
  () => props.visible,
  (val) => {
    if (val) {
      name.value = "";
      description.value = "";
      selectedNamespaceId.value = props.namespaceId;
      selectedFlowId.value = null;
      createdTable.value = null;
    }
  },
);

async function submit() {
  if (!canSubmit.value) return;
  submitting.value = true;
  try {
    const nsId = selectedNamespaceId.value ?? props.defaultNamespaceId;
    const result = await CatalogApi.createVirtualTable({
      name: name.value.trim(),
      namespace_id: nsId,
      description: description.value.trim() || null,
      producer_registration_id: selectedFlowId.value!,
    });
    createdTable.value = result;
    ElMessage.success("Virtual table created successfully");
    await Promise.all([
      catalogStore.loadTree(),
      catalogStore.loadAllTables(),
      catalogStore.loadStats(),
    ]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to create virtual table");
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
  width: 480px;
  max-width: 90vw;
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
  padding: var(--spacing-4) var(--spacing-5);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  overflow-y: auto;
  max-height: 60vh;
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

.hint {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.virtual-info {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 8px 10px;
  background: rgba(59, 130, 246, 0.08);
  border: 1px solid rgba(59, 130, 246, 0.25);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-xs);
  color: var(--color-primary);
  line-height: 1.4;
}

.laziness-blockers {
  padding: 10px 12px;
  background: rgba(245, 158, 11, 0.08);
  border: 1px solid rgba(245, 158, 11, 0.3);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.blockers-header {
  display: flex;
  align-items: flex-start;
  gap: 6px;
  margin-bottom: 6px;
  color: var(--color-warning);
}

.blocker-list {
  margin: 0;
  padding-left: 20px;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.blocker-list li {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
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
