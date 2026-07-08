<template>
  <div class="sample-input-editor">
    <div class="editor-tabs">
      <button
        class="editor-tab"
        :class="{ active: mode === 'grid' }"
        type="button"
        @click="mode = 'grid'"
      >
        <i class="fa-solid fa-table-cells"></i> Grid
      </button>
      <button
        class="editor-tab"
        :class="{ active: mode === 'csv' }"
        type="button"
        @click="mode = 'csv'"
      >
        <i class="fa-solid fa-paste"></i> Paste CSV
      </button>
    </div>

    <p class="storage-notice">
      <i class="fa-solid fa-circle-info"></i>
      Sample data is stored inside the node file (max {{ MAX_SAMPLE_ROWS }} rows ×
      {{ MAX_SAMPLE_COLS }} cols).
    </p>

    <SampleDataGrid v-if="mode === 'grid'" :table="props.table" @update:table="onTableUpdate" />

    <div v-else class="csv-panel">
      <textarea
        v-model="csvText"
        class="csv-textarea"
        rows="8"
        placeholder="col_a,col_b&#10;1,hello&#10;2,world"
      ></textarea>
      <button class="csv-apply" type="button" @click="applyCsv">
        <i class="fa-solid fa-check"></i> Apply to grid
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import SampleDataGrid from "./SampleDataGrid.vue";
import { MAX_SAMPLE_COLS, MAX_SAMPLE_ROWS, parseCsv, type SampleTable } from "../../sampleData";

const props = defineProps<{ table: SampleTable }>();
const emit = defineEmits<{ (e: "update:table", value: SampleTable): void }>();

const mode = ref<"grid" | "csv">("grid");
const csvText = ref("");

function onTableUpdate(next: SampleTable) {
  emit("update:table", next);
}

function applyCsv() {
  if (!csvText.value.trim()) return;
  emit("update:table", parseCsv(csvText.value));
  mode.value = "grid";
}
</script>

<style scoped>
.sample-input-editor {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.editor-tabs {
  display: flex;
  gap: 0.25rem;
}

.editor-tab {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  padding: 0.25rem 0.625rem;
  font-size: 0.75rem;
  border: 1px solid transparent;
  border-radius: 4px;
  background: transparent;
  color: var(--color-text-secondary, #6b7280);
  cursor: pointer;
}

.editor-tab.active {
  background: var(--color-background-secondary, #f3f4f6);
  color: var(--color-text-primary, #374151);
  border-color: var(--color-border-primary, #d1d5db);
}

.storage-notice {
  display: flex;
  align-items: center;
  gap: 0.375rem;
  margin: 0;
  font-size: 0.6875rem;
  color: var(--color-text-secondary, #6b7280);
}

.csv-panel {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.csv-textarea {
  width: 100%;
  font-family: var(--font-family-mono, monospace);
  font-size: 0.75rem;
  padding: 0.5rem;
  border: 1px solid var(--color-border-primary, #d1d5db);
  border-radius: 6px;
  resize: vertical;
}

.csv-apply {
  align-self: flex-start;
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  padding: 0.375rem 0.75rem;
  font-size: 0.75rem;
  border: none;
  border-radius: 4px;
  background: var(--color-button-primary, #4a6cf7);
  color: #fff;
  cursor: pointer;
}
</style>
