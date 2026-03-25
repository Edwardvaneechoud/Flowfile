<template>
  <div v-if="visible" class="modal-overlay" @click.self="$emit('close')">
    <div class="modal-container">
      <div class="modal-header">
        <h3 class="modal-title">{{ parentId ? "Create Schema" : "Create Catalog" }}</h3>
      </div>
      <div class="modal-content">
        <input
          v-model="name"
          class="input-field"
          :placeholder="parentId ? 'Schema name' : 'Catalog name'"
          @keyup.enter="submit"
        />
        <input v-model="description" class="input-field" placeholder="Description (optional)" />
      </div>
      <div class="modal-actions">
        <button class="btn btn-secondary" @click="$emit('close')">Cancel</button>
        <button class="btn btn-primary" :disabled="!name.trim()" @click="submit">Create</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from "vue";
import { ElMessage } from "element-plus";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";

const props = defineProps<{
  visible: boolean;
  parentId: number | null;
}>();

const emit = defineEmits(["close"]);

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
    ElMessage.error(e?.response?.data?.detail ?? "Failed to create namespace");
  }
}
</script>

<style scoped>
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
  box-shadow: 0 0 0 2px color-mix(in srgb, var(--color-info) 15%, transparent);
}
</style>
