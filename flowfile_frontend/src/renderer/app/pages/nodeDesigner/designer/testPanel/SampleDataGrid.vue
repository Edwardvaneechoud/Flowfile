<template>
  <div class="sample-grid">
    <div class="grid-toolbar">
      <button class="grid-btn" type="button" :disabled="atColCap" @click="addColumn">
        <i class="fa-solid fa-plus"></i> Column
      </button>
      <button class="grid-btn" type="button" :disabled="atRowCap" @click="addRow">
        <i class="fa-solid fa-plus"></i> Row
      </button>
      <span class="grid-info">{{ table.columns.length }} cols · {{ table.rows.length }} rows</span>
    </div>

    <div class="grid-scroll">
      <table class="grid-table">
        <thead>
          <tr>
            <th class="row-num-head">#</th>
            <th v-for="col in table.columns" :key="col.name" class="col-head">
              <div class="col-head-top">
                <input
                  :value="col.name"
                  class="col-name-input"
                  type="text"
                  @change="renameColumn(col, ($event.target as HTMLInputElement).value)"
                />
                <button
                  v-if="table.columns.length > 1"
                  class="col-del-btn"
                  type="button"
                  title="Delete column"
                  @click="deleteColumn(col.name)"
                >
                  <i class="fa-solid fa-xmark"></i>
                </button>
              </div>
              <select
                :value="col.dtype"
                class="dtype-select"
                @change="setDtype(col, ($event.target as HTMLSelectElement).value)"
              >
                <option v-for="dt in SAMPLE_DTYPES" :key="dt" :value="dt">{{ dt }}</option>
              </select>
            </th>
            <th class="actions-head"></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, rIdx) in table.rows" :key="rIdx">
            <td class="row-num">{{ rIdx + 1 }}</td>
            <td v-for="col in table.columns" :key="col.name" class="cell">
              <input
                :value="row[col.name] ?? ''"
                class="cell-input"
                type="text"
                @input="setCell(rIdx, col.name, ($event.target as HTMLInputElement).value)"
              />
            </td>
            <td class="row-actions">
              <button
                v-if="table.rows.length > 1"
                class="row-del-btn"
                type="button"
                title="Delete row"
                @click="deleteRow(rIdx)"
              >
                <i class="fa-solid fa-xmark"></i>
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import {
  MAX_SAMPLE_COLS,
  MAX_SAMPLE_ROWS,
  SAMPLE_DTYPES,
  type SampleColumn,
  type SampleDtype,
  type SampleTable,
} from "../../sampleData";

const props = defineProps<{ table: SampleTable }>();
const emit = defineEmits<{ (e: "update:table", value: SampleTable): void }>();

const atColCap = computed(() => props.table.columns.length >= MAX_SAMPLE_COLS);
const atRowCap = computed(() => props.table.rows.length >= MAX_SAMPLE_ROWS);

function clone(): SampleTable {
  return {
    columns: props.table.columns.map((c) => ({ ...c })),
    rows: props.table.rows.map((r) => ({ ...r })),
  };
}

function addColumn() {
  if (atColCap.value) return;
  const next = clone();
  let n = next.columns.length + 1;
  const taken = new Set(next.columns.map((c) => c.name));
  while (taken.has(`column_${n}`)) n += 1;
  const name = `column_${n}`;
  next.columns.push({ name, dtype: "str" });
  next.rows.forEach((r) => (r[name] = ""));
  emit("update:table", next);
}

function addRow() {
  if (atRowCap.value) return;
  const next = clone();
  const row: Record<string, string> = {};
  next.columns.forEach((c) => (row[c.name] = ""));
  next.rows.push(row);
  emit("update:table", next);
}

function deleteColumn(name: string) {
  const next = clone();
  next.columns = next.columns.filter((c) => c.name !== name);
  next.rows.forEach((r) => delete r[name]);
  emit("update:table", next);
}

function deleteRow(index: number) {
  const next = clone();
  next.rows.splice(index, 1);
  emit("update:table", next);
}

function renameColumn(col: SampleColumn, raw: string) {
  const newName = raw.trim();
  if (!newName || newName === col.name) return;
  const next = clone();
  const target = next.columns.find((c) => c.name === col.name);
  if (!target || next.columns.some((c) => c.name === newName)) return;
  const oldName = target.name;
  target.name = newName;
  next.rows.forEach((r) => {
    r[newName] = r[oldName] ?? "";
    delete r[oldName];
  });
  emit("update:table", next);
}

function setDtype(col: SampleColumn, dtype: string) {
  const next = clone();
  const target = next.columns.find((c) => c.name === col.name);
  if (target) target.dtype = dtype as SampleDtype;
  emit("update:table", next);
}

function setCell(rowIndex: number, colName: string, value: string) {
  const next = clone();
  next.rows[rowIndex][colName] = value;
  emit("update:table", next);
}
</script>

<style scoped>
.sample-grid {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.grid-toolbar {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.grid-btn {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  padding: 0.25rem 0.625rem;
  font-size: 0.75rem;
  border: 1px solid var(--color-border-primary, #d1d5db);
  border-radius: 4px;
  background: var(--color-background-primary, #fff);
  color: var(--color-text-primary, #374151);
  cursor: pointer;
}

.grid-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.grid-info {
  margin-left: auto;
  font-size: 0.6875rem;
  color: var(--color-text-secondary, #6b7280);
}

.grid-scroll {
  overflow-x: auto;
  border: 1px solid var(--color-border-primary, #e5e7eb);
  border-radius: 6px;
  max-height: 240px;
  overflow-y: auto;
}

.grid-table {
  border-collapse: collapse;
  width: 100%;
  font-size: 0.75rem;
}

.grid-table th,
.grid-table td {
  border: 1px solid var(--color-border-light, #e5e7eb);
  padding: 0;
}

.row-num-head,
.row-num {
  width: 32px;
  text-align: center;
  background: var(--color-background-secondary, #f3f4f6);
  color: var(--color-text-secondary, #6b7280);
  font-weight: 500;
}

.col-head {
  background: var(--color-background-secondary, #f3f4f6);
  padding: 0.25rem;
  min-width: 100px;
}

.col-head-top {
  display: flex;
  align-items: center;
  gap: 0.25rem;
}

.col-name-input {
  flex: 1;
  min-width: 0;
  border: none;
  background: transparent;
  font-weight: 600;
  font-size: 0.75rem;
  color: var(--color-text-primary, #374151);
}

.col-name-input:focus {
  outline: 1px solid var(--color-accent, #0891b2);
}

.col-del-btn,
.row-del-btn {
  border: none;
  background: transparent;
  color: var(--color-text-secondary, #6b7280);
  cursor: pointer;
  font-size: 0.6875rem;
  padding: 0.125rem;
}

.col-del-btn:hover,
.row-del-btn:hover {
  color: var(--color-text-danger, #dc2626);
}

.dtype-select {
  width: 100%;
  margin-top: 0.25rem;
  font-size: 0.6875rem;
  border: 1px solid var(--color-border-primary, #d1d5db);
  border-radius: 3px;
  background: var(--color-background-primary, #fff);
}

.cell-input {
  width: 100%;
  border: none;
  background: transparent;
  padding: 0.25rem 0.375rem;
  font-size: 0.75rem;
  color: var(--color-text-primary, #374151);
}

.cell-input:focus {
  outline: 1px solid var(--color-accent, #0891b2);
}

.actions-head,
.row-actions {
  width: 28px;
  text-align: center;
}
</style>
