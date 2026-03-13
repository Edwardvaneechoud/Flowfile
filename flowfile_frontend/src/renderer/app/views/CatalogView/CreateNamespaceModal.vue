<template>
  <div v-if="visible" class="modal-overlay" @click.self="$emit('close')">
    <div class="modal-card">
      <h3>{{ parentId ? "Create Schema" : "Create Catalog" }}</h3>
      <input
        v-model="name"
        class="input-field"
        :placeholder="parentId ? 'Schema name' : 'Catalog name'"
        @keyup.enter="submit"
      />
      <input v-model="description" class="input-field" placeholder="Description (optional)" />
      <div class="modal-actions">
        <button class="btn-secondary" @click="$emit('close')">Cancel</button>
        <button class="btn-primary" :disabled="!name.trim()" @click="submit">Create</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from "vue";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";

const props = defineProps<{
  visible: boolean;
  parentId: number | null;
}>();

const emit = defineEmits<{
  close: [];
}>();

const catalogStore = useCatalogStore();
const name = ref("");
const description = ref("");

watch(
  () => props.visible,
  (val) => {
    if (val) {
      name.value = "";
      description.value = "";
    }
  },
);

async function submit() {
  if (!name.value.trim()) return;
  try {
    await CatalogApi.createNamespace({
      name: name.value.trim(),
      parent_id: props.parentId,
      description: description.value.trim() || null,
    });
    emit("close");
    await Promise.all([catalogStore.loadTree(), catalogStore.loadStats()]);
  } catch (e: any) {
    alert(e?.response?.data?.detail ?? "Failed to create namespace");
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
</style>
