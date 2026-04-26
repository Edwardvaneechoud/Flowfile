<template>
  <div class="sql-editor-panel">
    <!-- Toolbar -->
    <div class="sql-toolbar">
      <el-button type="primary" size="small" :loading="executing" @click="runQuery">
        <i v-if="!executing" class="fa-solid fa-play" style="margin-right: 4px"></i>
        Run (Ctrl+Enter)
      </el-button>
      <el-button
        type="success"
        size="small"
        :disabled="!canSaveAsVirtual"
        @click="showSaveDialog = true"
      >
        <i class="fa-solid fa-bolt" style="margin-right: 4px"></i>
        Save as Virtual Table
      </el-button>
      <div class="toolbar-spacer"></div>
      <label class="limit-label">
        Limit:
        <el-input-number
          v-model="maxRows"
          :min="1"
          :max="100000"
          :step="1000"
          size="small"
          style="width: 130px"
        />
      </label>
    </div>

    <!-- Editor -->
    <div class="sql-editor-area">
      <codemirror
        v-model="queryText"
        placeholder="SELECT * FROM my_table LIMIT 100"
        :style="{ height: '200px' }"
        :autofocus="true"
        :indent-with-tab="false"
        :tab-size="2"
        :extensions="extensions"
        @ready="handleReady"
      />
    </div>

    <!-- Results Section -->
    <div v-if="result || error" class="sql-results-section">
      <!-- Result Info -->
      <div class="result-header">
        <span v-if="result && !result.error" class="result-info">
          {{ result.total_rows.toLocaleString() }} rows
          <span v-if="result.truncated"> (showing {{ result.rows.length.toLocaleString() }})</span>
          &middot; {{ result.execution_time_ms.toFixed(0) }}ms
          <span v-if="result.used_tables.length">
            &middot; Tables: {{ result.used_tables.join(", ") }}</span
          >
        </span>
      </div>

      <!-- Error display -->
      <div v-if="error || result?.error" class="sql-error">
        <i class="fa-solid fa-circle-exclamation"></i>
        {{ error || result?.error }}
      </div>

      <!-- Explore results -->
      <div v-else class="result-content">
        <SqlExplorePanel
          :result="result!"
          :source-query="lastExecutedQuery"
          :save-namespace-id="saveForm.namespaceId"
        />
      </div>
    </div>

    <!-- Empty state -->
    <div v-else class="sql-empty-state">
      <i class="fa-solid fa-database empty-icon"></i>
      <h3>SQL Editor</h3>
      <p>Write SQL queries against your catalog tables. Press Ctrl+Enter to run.</p>
    </div>

    <!-- Query History -->
    <div v-if="history.length > 0" class="sql-history">
      <details>
        <summary>History ({{ history.length }})</summary>
        <div class="history-list">
          <button
            v-for="(item, idx) in history"
            :key="idx"
            class="history-item"
            @click="loadHistoryItem(item)"
          >
            <span class="history-query">{{ item.query }}</span>
            <span class="history-time">{{ formatTimeAgo(item.timestamp) }}</span>
          </button>
        </div>
      </details>
    </div>

    <!-- Save as Virtual Table Dialog -->
    <el-dialog v-model="showSaveDialog" title="Save as Virtual Table" width="440px" append-to-body>
      <div class="save-dialog-body">
        <div class="save-dialog-info">
          <i class="fa-solid fa-bolt"></i>
          <span>
            This saves your SQL query as a virtual table. When read, the query is re-executed
            against the catalog.
          </span>
        </div>
        <el-form label-position="top" @submit.prevent="saveAsVirtualTable">
          <el-form-item label="Table name" required>
            <el-input v-model="saveForm.name" placeholder="my_virtual_table" />
          </el-form-item>
          <el-form-item label="Catalog / Schema">
            <el-select
              v-model="saveForm.namespaceId"
              placeholder="Select namespace"
              clearable
              style="width: 100%"
            >
              <el-option
                v-for="ns in schemaNamespaces"
                :key="ns.id"
                :label="ns.label"
                :value="ns.id"
              />
            </el-select>
          </el-form-item>
          <el-form-item label="Description">
            <el-input
              v-model="saveForm.description"
              placeholder="Optional description"
              type="textarea"
              :rows="2"
            />
          </el-form-item>
        </el-form>
      </div>
      <template #footer>
        <el-button @click="showSaveDialog = false">Cancel</el-button>
        <el-button
          type="primary"
          :disabled="!saveForm.name.trim()"
          :loading="savingVirtual"
          @click="saveAsVirtualTable"
        >
          Save
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, computed, onMounted, watch } from "vue";
import { EditorView, keymap } from "@codemirror/view";
import { Extension, Prec } from "@codemirror/state";
import { Codemirror } from "vue-codemirror";
import { sql } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import { ElMessage } from "element-plus";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";
import SqlExplorePanel from "./SqlExplorePanel.vue";
import type { SqlQueryResult } from "../../types";

const catalogStore = useCatalogStore();

const props = defineProps<{
  initialQuery?: string;
}>();

const queryText = ref(props.initialQuery || "SELECT * FROM ");
const maxRows = ref(10_000);

watch(
  () => props.initialQuery,
  (newQuery) => {
    if (newQuery) {
      queryText.value = newQuery;
    }
  },
);
const executing = ref(false);
const result = ref<SqlQueryResult | null>(null);
const error = ref<string | null>(null);
const editorView = ref<EditorView | null>(null);

// Query history (localStorage)
interface HistoryItem {
  query: string;
  timestamp: number;
}
const HISTORY_KEY = "flowfile_sql_history";
const MAX_HISTORY = 50;

const history = ref<HistoryItem[]>([]);

function loadHistory() {
  try {
    const raw = localStorage.getItem(HISTORY_KEY);
    if (raw) history.value = JSON.parse(raw);
  } catch {
    history.value = [];
  }
}

function saveToHistory(query: string) {
  const trimmed = query.trim();
  if (!trimmed) return;
  history.value = [
    { query: trimmed, timestamp: Date.now() },
    ...history.value.filter((h) => h.query !== trimmed),
  ].slice(0, MAX_HISTORY);
  localStorage.setItem(HISTORY_KEY, JSON.stringify(history.value));
}

function loadHistoryItem(item: HistoryItem) {
  queryText.value = item.query;
}

function formatTimeAgo(ts: number): string {
  const diff = Math.floor((Date.now() - ts) / 1000);
  if (diff < 60) return "just now";
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

// CodeMirror extensions
const runKeymap = keymap.of([
  {
    key: "Ctrl-Enter",
    mac: "Cmd-Enter",
    run: () => {
      runQuery();
      return true;
    },
  },
]);

const tableSchema = computed(() => {
  const schema: Record<string, string[]> = {};
  for (const t of catalogStore.allTables) {
    schema[t.full_table_name ?? t.name] = (t.schema_columns ?? []).map((c) => c.name);
  }
  return schema;
});

const extensions = computed<Extension[]>(() => [
  sql({ schema: tableSchema.value, upperCaseKeywords: true }),
  oneDark,
  Prec.highest(runKeymap),
]);

function handleReady(payload: { view: EditorView }) {
  editorView.value = payload.view;
}

const lastExecutedQuery = ref<string>("");

async function runQuery() {
  const q = queryText.value.trim();
  if (!q) return;

  executing.value = true;
  error.value = null;
  result.value = null;

  try {
    const res = await CatalogApi.executeSqlQuery(q, maxRows.value);
    if (res.error) {
      error.value = res.error;
    } else {
      result.value = res;
      lastExecutedQuery.value = q;
    }
    saveToHistory(q);
  } catch (e: any) {
    error.value = e?.response?.data?.detail ?? e?.message ?? "Unknown error";
  } finally {
    executing.value = false;
  }
}

// Save as Virtual Table
const showSaveDialog = ref(false);
const savingVirtual = ref(false);
const saveForm = reactive({
  name: "",
  namespaceId: null as number | null,
  description: "",
});

const canSaveAsVirtual = computed(() => result.value !== null && !result.value.error);

const schemaNamespaces = computed(() => {
  const items: { id: number; label: string }[] = [];
  for (const catalog of catalogStore.tree) {
    for (const schema of catalog.children) {
      items.push({ id: schema.id, label: `${catalog.name} / ${schema.name}` });
    }
  }
  return items;
});

watch(showSaveDialog, (val) => {
  if (val) {
    saveForm.name = "";
    saveForm.description = "";
    saveForm.namespaceId = null;
  }
});

async function saveAsVirtualTable() {
  const trimmedName = saveForm.name.trim();
  if (!trimmedName) return;

  savingVirtual.value = true;
  try {
    await CatalogApi.createQueryVirtualTable({
      name: trimmedName,
      sql_query: queryText.value.trim(),
      namespace_id: saveForm.namespaceId,
      description: saveForm.description.trim() || null,
    });
    ElMessage.success(`Virtual table "${trimmedName}" created`);
    showSaveDialog.value = false;
    await Promise.all([catalogStore.loadTree(), catalogStore.loadAllTables()]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to create virtual table");
  } finally {
    savingVirtual.value = false;
  }
}

onMounted(() => {
  loadHistory();
});
</script>

<style scoped>
.sql-editor-panel {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
}

.sql-toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-bottom: 1px solid var(--color-border-light, #e4e7ed);
  background: var(--color-background-secondary, #f5f7fa);
}

.toolbar-spacer {
  flex: 1;
}

.limit-label {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
  color: var(--color-text-secondary, #606266);
}

.sql-editor-area {
  flex-shrink: 0;
  border-bottom: 1px solid var(--color-border-light, #e4e7ed);
  font-size: 13px;
}

.sql-results-section {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow: hidden;
}

.result-header {
  display: flex;
  align-items: center;
  padding: 6px 12px;
  border-bottom: 1px solid var(--color-border-light, #e4e7ed);
  background: var(--color-background-secondary, #f5f7fa);
}

.result-info {
  font-size: 12px;
  color: var(--color-text-secondary, #909399);
}

.result-content {
  flex: 1;
  min-height: 0;
  overflow: hidden;
}

.sql-error {
  padding: 12px 16px;
  margin: 8px 12px;
  background: color-mix(in srgb, var(--color-danger) 8%, transparent);
  border: 1px solid color-mix(in srgb, var(--color-danger) 30%, transparent);
  border-radius: 4px;
  color: var(--color-danger);
  font-size: 13px;
}

.sql-error i {
  margin-right: 6px;
}

.sql-empty-state {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  color: var(--color-text-secondary, #909399);
}

.empty-icon {
  font-size: 40px;
  margin-bottom: 12px;
  opacity: 0.4;
}

.sql-empty-state h3 {
  margin: 0 0 4px;
  font-size: 16px;
  font-weight: 600;
}

.sql-empty-state p {
  margin: 0;
  font-size: 13px;
}

.sql-history {
  border-top: 1px solid var(--color-border-light, #e4e7ed);
  padding: 6px 12px;
  flex-shrink: 0;
}

.sql-history summary {
  font-size: 12px;
  color: var(--color-text-secondary, #909399);
  cursor: pointer;
  user-select: none;
}

.history-list {
  max-height: 120px;
  overflow-y: auto;
  margin-top: 4px;
}

.history-item {
  display: flex;
  align-items: center;
  width: 100%;
  padding: 4px 8px;
  border: none;
  background: transparent;
  cursor: pointer;
  font-size: 12px;
  text-align: left;
  border-radius: 3px;
}

.history-item:hover {
  background: var(--color-background-hover, #f0f0f0);
}

.history-query {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-family: monospace;
  color: var(--color-text-primary, #303133);
}

.history-time {
  flex-shrink: 0;
  margin-left: 8px;
  color: var(--color-text-secondary, #909399);
}

.save-dialog-body {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.save-dialog-info {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 10px 12px;
  background: color-mix(in srgb, var(--el-color-primary) 8%, transparent);
  border: 1px solid color-mix(in srgb, var(--el-color-primary) 25%, transparent);
  border-radius: 4px;
  font-size: 13px;
  color: var(--el-color-primary);
  line-height: 1.5;
}

.save-dialog-info i {
  margin-top: 2px;
  flex-shrink: 0;
}
</style>
