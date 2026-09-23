<template>
  <div :class="['sql-editor-panel', { 'is-resizing': resizing }]">
    <!-- Open queries: double-click a tab to rename it, "+" starts a new one. -->
    <div class="sql-tabbar">
      <el-tabs
        :model-value="store.activeTabId ?? undefined"
        type="card"
        closable
        class="sql-tabs"
        @tab-change="onTabChange"
        @tab-remove="onTabRemove"
      >
        <el-tab-pane v-for="tab in store.tabs" :key="tab.id" :name="tab.id">
          <template #label>
            <span class="sql-tab-label" :title="tabTitle(tab)" @dblclick.stop="startRename(tab.id)">
              <i class="sql-tab-icon" :class="tabIcon(tab)"></i>
              <!-- keydown.stop: el-tabs closes the tab on Backspace/Delete inside its label -->
              <input
                v-if="renamingTabId === tab.id"
                ref="renameInputRef"
                v-model="renameDraft"
                class="sql-tab-rename"
                aria-label="Query name"
                @keydown.enter.prevent="commitRename"
                @keydown.esc.prevent="cancelRename"
                @keydown.stop
                @blur="commitRename"
                @click.stop
                @mousedown.stop
              />
              <span v-else class="sql-tab-name">{{ tab.name }}</span>
              <span
                v-if="isEditedSinceSaved(tab) && renamingTabId !== tab.id"
                class="sql-tab-edited"
                aria-label="Edited since saved"
                >&bull;</span
              >
            </span>
          </template>
        </el-tab-pane>
      </el-tabs>
      <button class="sql-tab-add" title="New query" aria-label="New query" @click="store.newTab()">
        <i class="fa-solid fa-plus"></i>
      </button>
    </div>

    <!-- Toolbar -->
    <div class="sql-toolbar">
      <el-button type="primary" size="small" :loading="active?.executing" @click="store.runQuery()">
        <i v-if="!active?.executing" class="fa-solid fa-play" style="margin-right: 4px"></i>
        Run (Ctrl+Enter)
      </el-button>
      <!-- A query is stored by saving it as a virtual table; tabs themselves are browser-only drafts. -->
      <el-button
        v-if="active?.link"
        type="success"
        size="small"
        :disabled="!canUpdateLinked"
        :loading="updatingVirtual"
        title="Store the current SQL in the linked virtual table"
        @click="updateLinkedTable"
      >
        <i v-if="!updatingVirtual" class="fa-solid fa-bolt" style="margin-right: 4px"></i>
        Update virtual table
      </el-button>
      <el-button
        v-else
        type="success"
        size="small"
        :disabled="!canSaveAsVirtual"
        @click="openSaveDialog"
      >
        <i class="fa-solid fa-bolt" style="margin-right: 4px"></i>
        Save as Virtual Table
      </el-button>

      <span
        v-if="active && !active.link"
        class="storage-marker"
        title="Query tabs are drafts kept in this browser only. Save as Virtual Table to store the query in the catalog."
      >
        <i class="fa-regular fa-file-lines"></i>
        <span class="storage-marker-text">Draft · kept in this browser only</span>
      </span>
      <span
        v-else-if="active?.link"
        class="storage-marker is-linked"
        :class="{ 'is-edited': activeEdited }"
        :title="
          activeEdited
            ? 'The virtual table still holds the query as it was last saved. Update it to store your edits.'
            : 'This query is stored in the catalog as a virtual table.'
        "
      >
        <i class="fa-solid fa-bolt"></i>
        <span v-if="!activeEdited" class="storage-marker-text">Saved as</span>
        <!-- Only tables in the namespace tree can be opened; the rest show their name. -->
        <button
          v-if="linkedInTree"
          class="storage-marker-link"
          title="Open the virtual table in the catalog"
          @click="emit('viewTable', active.link.tableId)"
        >
          {{ linkedName }}
        </button>
        <span v-else class="storage-marker-name">{{ linkedName }}</span>
        <span v-if="activeEdited" class="storage-marker-text">· edited since saved</span>
      </span>

      <div class="toolbar-spacer"></div>
      <label class="limit-label">
        Limit:
        <el-input-number
          :model-value="store.maxRows"
          :min="1"
          :max="MAX_ROWS_LIMIT"
          :step="1000"
          size="small"
          style="width: 130px"
          @update:model-value="store.setMaxRows"
        />
      </label>

      <el-dropdown
        trigger="click"
        placement="bottom-end"
        max-height="360px"
        @command="onHistoryCommand"
      >
        <el-button size="small" class="icon-btn" title="Query history" aria-label="Query history">
          <i class="fa-solid fa-clock-rotate-left"></i>
        </el-button>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item
              v-for="(item, idx) in store.history"
              :key="idx"
              :command="idx"
              :title="item.query"
            >
              <span class="history-query">{{ item.query }}</span>
              <span class="history-time">{{ formatTimeAgo(item.timestamp) }}</span>
            </el-dropdown-item>
            <el-dropdown-item v-if="!store.history.length" disabled>
              No queries run yet
            </el-dropdown-item>
            <el-dropdown-item v-else divided command="clear">
              <i class="fa-solid fa-trash menu-icon"></i> Clear history
            </el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>

      <el-dropdown trigger="click" placement="bottom-end" @command="onMoreCommand">
        <el-button size="small" class="icon-btn" title="More actions" aria-label="More actions">
          <i class="fa-solid fa-ellipsis"></i>
        </el-button>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item command="rename">
              <i class="fa-solid fa-pen menu-icon"></i> Rename
            </el-dropdown-item>
            <el-dropdown-item command="duplicate">
              <i class="fa-solid fa-copy menu-icon"></i> Duplicate as draft
            </el-dropdown-item>
            <el-dropdown-item v-if="active?.link" command="save-new">
              <i class="fa-solid fa-bolt menu-icon"></i> Save as new virtual table…
            </el-dropdown-item>
            <el-dropdown-item divided command="download">
              <i class="fa-solid fa-download menu-icon"></i> Download .sql
            </el-dropdown-item>
            <el-dropdown-item command="open">
              <i class="fa-solid fa-folder-open menu-icon"></i> Open .sql file…
            </el-dropdown-item>
            <el-dropdown-item divided command="close">
              <i class="fa-solid fa-xmark menu-icon"></i> Close query
            </el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
      <input
        ref="fileInputRef"
        type="file"
        accept=".sql,text/plain"
        hidden
        @change="onFileChosen"
      />
    </div>

    <div ref="bodyRef" class="sql-body">
      <!-- Editor (keyed per tab so one tab's undo history never leaks into another) -->
      <div class="sql-editor-area" :style="{ height: `${editorHeight}px` }">
        <codemirror
          v-if="active"
          :key="active.id"
          :model-value="active.query"
          placeholder="SELECT * FROM my_table LIMIT 100"
          :style="editorStyle"
          :autofocus="true"
          :indent-with-tab="false"
          :tab-size="2"
          :extensions="extensions"
          @update:model-value="onQueryInput"
          @update="onEditorUpdate"
          @ready="handleReady"
        />
      </div>

      <div
        class="sql-splitter"
        role="separator"
        aria-orientation="horizontal"
        aria-label="Resize editor"
        :aria-valuenow="editorHeight"
        :aria-valuemin="MIN_EDITOR_HEIGHT"
        :aria-valuemax="maxEditorHeight"
        tabindex="0"
        title="Drag to resize · double-click to reset"
        @pointerdown="startResize"
        @dblclick="store.setEditorHeight(DEFAULT_EDITOR_HEIGHT)"
        @keydown="onSplitterKeydown"
      ></div>

      <!-- Results Section -->
      <div v-if="active && (active.result || active.error)" class="sql-results-section">
        <!-- Result Info -->
        <div class="result-header">
          <span v-if="active.result && !active.result.error" class="result-info">
            {{ active.result.total_rows.toLocaleString() }} rows
            <span v-if="active.result.truncated">
              (showing {{ active.result.rows.length.toLocaleString() }})</span
            >
            &middot; {{ active.result.execution_time_ms.toFixed(0) }}ms
            <span v-if="active.result.used_tables.length">
              &middot; Tables: {{ active.result.used_tables.join(", ") }}</span
            >
          </span>
        </div>

        <!-- Error display -->
        <div v-if="active.error || active.result?.error" class="sql-error">
          <i class="fa-solid fa-circle-exclamation"></i>
          {{ active.error || active.result?.error }}
        </div>

        <!-- Explore results -->
        <div v-else-if="active.result" class="result-content">
          <SqlExplorePanel
            :key="`${active.id}:${active.runSeq}`"
            :result="active.result"
            :source-query="active.lastExecutedQuery"
            :save-namespace-id="saveForm.namespaceId"
          />
        </div>
      </div>

      <!-- Empty state -->
      <div v-else class="sql-empty-state">
        <i
          class="empty-icon"
          :class="active?.executing ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-database'"
        ></i>
        <h3>{{ active?.executing ? "Running query…" : "SQL Editor" }}</h3>
        <p v-if="!active?.executing">
          Write SQL queries against your catalog tables. Press Ctrl+Enter to run.
        </p>
      </div>
    </div>

    <!-- Save as Virtual Table Dialog -->
    <el-dialog
      v-model="showSaveDialog"
      :title="active?.link ? 'Save as new virtual table' : 'Save as Virtual Table'"
      width="440px"
      append-to-body
    >
      <div class="save-dialog-body">
        <div class="save-dialog-info">
          <i class="fa-solid fa-bolt"></i>
          <span>
            This saves your SQL query as a virtual table. When read, the query is re-executed
            against the catalog. It is how a query is stored: query tabs are drafts kept only in
            this browser.
          </span>
        </div>
        <el-form label-position="top" @submit.prevent="saveAsVirtualTable">
          <el-form-item label="Table name" required :error="sqlNameError">
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
          :disabled="!saveForm.name.trim() || !!sqlNameError"
          :loading="savingVirtual"
          @click="saveAsVirtualTable"
        >
          Save
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script lang="ts">
// Caret per tab, session-local: the editor remounts on every tab switch and whenever the panel does.
const tabSelections = new Map<string, { anchor: number; head: number }>();
</script>

<script setup lang="ts">
import { ref, reactive, computed, nextTick, onMounted, onBeforeUnmount } from "vue";
import { EditorView, keymap, type ViewUpdate } from "@codemirror/view";
import { Extension, Prec } from "@codemirror/state";
import { Codemirror } from "vue-codemirror";
import { sql } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import { ElMessage, ElMessageBox, type TabPaneName } from "element-plus";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";
import {
  MIN_EDITOR_HEIGHT,
  clampEditorHeight,
  hasUnsavedWork,
  isAutoTabName,
  isEditedSinceSaved,
  sqlFileName,
  tabNameFromFileName,
  tableDisplayName,
  useSqlEditorStore,
  type SqlTab,
} from "../../stores/sql-editor-store";
import { DEFAULT_EDITOR_HEIGHT, MAX_ROWS_LIMIT } from "../../stores/sql-editor-store-persistence";
import { useWritableNamespaces } from "../../composables/useWritableNamespaces";
import { catalogSaveErrorMessage } from "../../composables/saveError";
import { validateCatalogName } from "../../composables/catalogNameValidation";
import SqlExplorePanel from "./SqlExplorePanel.vue";

const emit = defineEmits<{ viewTable: [tableId: number] }>();

const catalogStore = useCatalogStore();
const store = useSqlEditorStore();
// Hydrate before the first render so the editor mounts on the restored tab.
store.ensureHydrated();

const active = computed(() => store.active);

// Prefer the catalog's current name so a rename elsewhere shows up; fall back to the name at save time.
function linkName(tab: SqlTab): string {
  if (!tab.link) return "";
  const table = catalogStore.allTables.find((t) => t.id === tab.link!.tableId);
  return table ? tableDisplayName(table) : tab.link.tableName;
}

const linkedName = computed(() => (active.value ? linkName(active.value) : ""));
const linkedInTree = computed(
  () => !!active.value?.link && !!catalogStore.findTableInTree(active.value.link.tableId),
);
const activeEdited = computed(() => !!active.value && isEditedSinceSaved(active.value));

function tabIcon(tab: SqlTab): string {
  if (tab.executing) return "fa-solid fa-spinner fa-spin";
  return tab.link ? "fa-solid fa-bolt" : "fa-solid fa-file-code";
}

function tabTitle(tab: SqlTab): string {
  if (!tab.link) return `${tab.name} — draft, kept in this browser only`;
  const edited = isEditedSinceSaved(tab) ? " (edited since saved)" : "";
  return `${tab.name} — virtual table ${linkName(tab)}${edited}`;
}

function onTabChange(name: TabPaneName) {
  store.setActive(String(name));
}

async function onTabRemove(name: TabPaneName) {
  const tab = store.tabs.find((t) => t.id === String(name));
  if (!tab) return;
  if (hasUnsavedWork(tab)) {
    const message = tab.link
      ? `Close "${tab.name}"? Your edits since the last save will be discarded; virtual table "${linkName(tab)}" keeps its saved query.`
      : `Close "${tab.name}"? It is a draft kept only in this browser, so its SQL will be discarded. Save it as a virtual table to keep it.`;
    try {
      await ElMessageBox.confirm(message, "Close query", {
        confirmButtonText: "Close",
        cancelButtonText: "Cancel",
        type: "warning",
      });
    } catch {
      return;
    }
  }
  tabSelections.delete(tab.id);
  store.closeTab(tab.id);
}

// Inline rename (double-click a tab, or More → Rename).
const renamingTabId = ref<string | null>(null);
const renameDraft = ref("");
const renameInputRef = ref<HTMLInputElement[] | HTMLInputElement | null>(null);

function startRename(id: string) {
  const tab = store.tabs.find((t) => t.id === id);
  if (!tab) return;
  store.setActive(id);
  renamingTabId.value = id;
  renameDraft.value = tab.name;
  void nextTick(() => {
    const el = Array.isArray(renameInputRef.value) ? renameInputRef.value[0] : renameInputRef.value;
    el?.focus();
    el?.select();
  });
}

function commitRename() {
  if (renamingTabId.value == null) return;
  store.renameTab(renamingTabId.value, renameDraft.value);
  renamingTabId.value = null;
}

function cancelRename() {
  renamingTabId.value = null;
}

// History
function formatTimeAgo(ts: number): string {
  const diff = Math.floor((Date.now() - ts) / 1000);
  if (diff < 60) return "just now";
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function onHistoryCommand(command: number | "clear") {
  if (command === "clear") {
    store.clearHistory();
    return;
  }
  const item = store.history[command];
  if (item && active.value) store.setQuery(active.value.id, item.query);
}

// Tab file actions
const fileInputRef = ref<HTMLInputElement | null>(null);
const MAX_SQL_FILE_BYTES = 1_000_000;

function downloadActive() {
  const tab = active.value;
  if (!tab) return;
  const url = URL.createObjectURL(new Blob([tab.query], { type: "application/sql" }));
  const a = document.createElement("a");
  a.href = url;
  a.download = sqlFileName(tab.name);
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

async function onFileChosen(event: Event) {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0];
  input.value = "";
  if (!file) return;
  if (file.size > MAX_SQL_FILE_BYTES) {
    ElMessage.error("That file is too large to open in the SQL editor (limit 1 MB).");
    return;
  }
  try {
    store.openQuery(await file.text(), tabNameFromFileName(file.name));
  } catch (e: any) {
    ElMessage.error(e?.message ?? "Could not read the file");
  }
}

function onMoreCommand(command: string) {
  const tab = active.value;
  if (!tab) return;
  if (command === "rename") startRename(tab.id);
  else if (command === "duplicate") store.duplicateTab(tab.id);
  else if (command === "save-new") openSaveDialog();
  else if (command === "download") downloadActive();
  else if (command === "open") fileInputRef.value?.click();
  else if (command === "close") void onTabRemove(tab.id);
}

// Editor
const editorStyle = { height: "100%" };

const runKeymap = keymap.of([
  {
    key: "Ctrl-Enter",
    mac: "Cmd-Enter",
    run: () => {
      void store.runQuery();
      return true;
    },
  },
]);

const tableSchema = computed(() => {
  const schema: Record<string, string[]> = {};
  for (const t of catalogStore.allTables) {
    const cols = (t.schema_columns ?? []).map((c) => c.name);
    schema[t.full_table_name ?? t.name] = cols;
    if (t.qualified_name) schema[t.qualified_name] = cols;
  }
  return schema;
});

const extensions = computed<Extension[]>(() => [
  sql({ schema: tableSchema.value, upperCaseKeywords: true }),
  oneDark,
  Prec.highest(runKeymap),
]);

function onQueryInput(value: string) {
  if (active.value) store.setQuery(active.value.id, value);
}

function onEditorUpdate(update: ViewUpdate) {
  if (!update.selectionSet || !active.value) return;
  const { anchor, head } = update.state.selection.main;
  tabSelections.set(active.value.id, { anchor, head });
}

function handleReady({ view }: { view: EditorView }) {
  const sel = active.value ? tabSelections.get(active.value.id) : undefined;
  if (!sel) return;
  const len = view.state.doc.length;
  view.dispatch({
    selection: { anchor: Math.min(sel.anchor, len), head: Math.min(sel.head, len) },
    scrollIntoView: true,
  });
}

// Resizable split between editor and results
const SPLITTER_SIZE = 8;
const bodyRef = ref<HTMLElement | null>(null);
const bodyHeight = ref(0);
const resizing = ref(false);
let bodyObserver: ResizeObserver | null = null;

function clampToBody(height: number): number {
  return bodyHeight.value > 0
    ? clampEditorHeight(height, bodyHeight.value - SPLITTER_SIZE)
    : Math.max(MIN_EDITOR_HEIGHT, height);
}

const editorHeight = computed(() => clampToBody(store.editorHeight));
const maxEditorHeight = computed(() => clampToBody(Number.MAX_SAFE_INTEGER));

function startResize(e: PointerEvent) {
  if (e.button !== 0) return;
  e.preventDefault();
  const handle = e.currentTarget as HTMLElement;
  const startY = e.clientY;
  const startHeight = editorHeight.value;
  handle.setPointerCapture(e.pointerId);
  resizing.value = true;

  const onMove = (ev: PointerEvent) => {
    store.setEditorHeight(clampToBody(startHeight + ev.clientY - startY));
  };
  const onEnd = () => {
    resizing.value = false;
    handle.removeEventListener("pointermove", onMove);
    handle.removeEventListener("pointerup", onEnd);
    handle.removeEventListener("pointercancel", onEnd);
  };
  handle.addEventListener("pointermove", onMove);
  handle.addEventListener("pointerup", onEnd);
  handle.addEventListener("pointercancel", onEnd);
}

function onSplitterKeydown(e: KeyboardEvent) {
  const step = e.shiftKey ? 60 : 20;
  const next: Record<string, number> = {
    ArrowUp: editorHeight.value - step,
    ArrowDown: editorHeight.value + step,
    Home: MIN_EDITOR_HEIGHT,
    End: maxEditorHeight.value,
  };
  if (!(e.key in next)) return;
  e.preventDefault();
  store.setEditorHeight(clampToBody(next[e.key]));
}

const flushPersist = () => store.flushPersist();

onMounted(() => {
  window.addEventListener("pagehide", flushPersist);
  if (bodyRef.value && typeof ResizeObserver !== "undefined") {
    bodyObserver = new ResizeObserver((entries) => {
      bodyHeight.value = entries[0]?.contentRect.height ?? 0;
    });
    bodyObserver.observe(bodyRef.value);
  }
});

onBeforeUnmount(() => {
  window.removeEventListener("pagehide", flushPersist);
  bodyObserver?.disconnect();
  store.flushPersist();
});

// Save as Virtual Table: the one place a query is stored beyond this browser.
const showSaveDialog = ref(false);
const savingVirtual = ref(false);
const updatingVirtual = ref(false);
const saveForm = reactive({
  name: "",
  namespaceId: null as number | null,
  description: "",
});

const canSaveAsVirtual = computed(() => !!active.value?.result && !active.value.result.error);
const canUpdateLinked = computed(
  () => !!active.value && activeEdited.value && !!active.value.query.trim(),
);

const sqlNameError = computed(() => validateCatalogName(saveForm.name, "Table") ?? "");

const { writableSchemaNamespaces: schemaNamespaces } = useWritableNamespaces();

function openSaveDialog() {
  const tab = active.value;
  if (!tab) return;
  // A tab the user named is a good default table name, when it is a valid one.
  const named = !tab.link && !isAutoTabName(tab.name) && !validateCatalogName(tab.name, "Table");
  saveForm.name = named ? tab.name : "";
  saveForm.description = "";
  saveForm.namespaceId = null;
  showSaveDialog.value = true;
}

async function saveAsVirtualTable() {
  const trimmedName = saveForm.name.trim();
  const tab = active.value;
  if (!trimmedName || sqlNameError.value || !tab) return;
  const tabId = tab.id;
  const query = tab.query.trim();

  savingVirtual.value = true;
  try {
    const table = await CatalogApi.createQueryVirtualTable({
      name: trimmedName,
      sql_query: query,
      namespace_id: saveForm.namespaceId,
      description: saveForm.description.trim() || null,
    });
    store.linkTab(tabId, table, query);
    ElMessage.success(`Virtual table "${trimmedName}" created`);
    showSaveDialog.value = false;
    await Promise.all([catalogStore.loadTree(), catalogStore.loadAllTables()]);
  } catch (e: any) {
    ElMessage.error(catalogSaveErrorMessage(e, "Failed to create virtual table"));
  } finally {
    savingVirtual.value = false;
  }
}

async function updateLinkedTable() {
  const tab = active.value;
  if (!tab?.link || !canUpdateLinked.value) return;
  const { id: tabId, link } = tab;
  const name = linkName(tab);
  const query = tab.query.trim();
  try {
    await ElMessageBox.confirm(
      `Replace the SQL of virtual table "${name}"? Flows, charts and schedules that read it will use the new query.`,
      "Update virtual table",
      { confirmButtonText: "Update", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return;
  }

  updatingVirtual.value = true;
  try {
    const table = await CatalogApi.updateQueryVirtualTable(link.tableId, { sql_query: query });
    store.linkTab(tabId, table, query);
    ElMessage.success(`Virtual table "${table.name}" updated`);
    await Promise.all([catalogStore.loadTree(), catalogStore.loadAllTables()]);
  } catch (e: any) {
    if (e?.response?.status === 404) {
      store.unlinkTab(tabId);
      ElMessage.warning(
        `Virtual table "${name}" no longer exists. This query is a draft again; save it as a new virtual table to keep it.`,
      );
    } else {
      ElMessage.error(catalogSaveErrorMessage(e, "Failed to update virtual table"));
    }
  } finally {
    updatingVirtual.value = false;
  }
}
</script>

<style scoped>
.sql-editor-panel {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
}

.sql-editor-panel.is-resizing {
  cursor: row-resize;
  user-select: none;
}

/* Query tabs follow the notebook's tab strip: transparent inactive tabs with a
   divider, the active tab raised with an accent top border. */
.sql-tabbar {
  display: flex;
  align-items: flex-end;
  gap: 6px;
  padding: 6px 12px 0;
  background: var(--color-background-secondary, #f5f7fa);
  border-bottom: 1px solid var(--color-border-light, #e4e7ed);
}

.sql-tabs {
  flex: 0 1 auto;
  min-width: 0;
}

.sql-tabs :deep(.el-tabs__header) {
  margin: 0;
  border-bottom: none;
}

.sql-tabs :deep(.el-tabs__nav-wrap)::after {
  display: none;
}

.sql-tabs :deep(.el-tabs__nav.is-top) {
  border: none;
}

.sql-tabs :deep(.el-tabs__content) {
  display: none;
}

.sql-tabs :deep(.el-tabs__item.is-top) {
  height: 30px;
  line-height: 30px;
  padding: 0 var(--spacing-4, 16px);
  border: none;
  border-right: 1px solid var(--color-border-light, #e4e7ed);
  border-radius: var(--border-radius-lg, 8px) var(--border-radius-lg, 8px) 0 0;
  color: var(--color-text-secondary, #606266);
  font-size: var(--font-size-xs, 12px);
  font-weight: var(--font-weight-medium, 500);
  transition: all var(--transition-fast, 0.15s);
}

.sql-tabs :deep(.el-tabs__item.is-top:not(.is-active):hover) {
  background-color: var(--color-background-hover, #f0f0f0);
  color: var(--color-text-primary, #303133);
}

.sql-tabs :deep(.el-tabs__item.is-active) {
  background-color: var(--color-background-primary, #ffffff);
  color: var(--color-text-primary, #303133);
  border-top: 2px solid var(--color-primary, #409eff);
  box-shadow: var(--shadow-xs, none);
}

.sql-tab-label {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1, 4px);
  max-width: 220px;
}

.sql-tab-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.sql-tab-icon {
  font-size: var(--font-size-xs, 12px);
  color: var(--color-primary, #409eff);
}

.sql-tab-edited {
  color: var(--el-color-warning, #e6a23c);
  font-weight: 700;
}

.sql-tab-rename {
  width: 140px;
  height: 20px;
  padding: 0 4px;
  border: 1px solid var(--color-primary, #409eff);
  border-radius: var(--border-radius-sm, 4px);
  background: var(--color-background-primary, #ffffff);
  color: var(--color-text-primary, #303133);
  font: inherit;
  outline: none;
}

.sql-tab-add {
  display: inline-flex;
  flex-shrink: 0;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  margin-bottom: 3px;
  padding: 0;
  border: 1px solid var(--color-border-light, #e4e7ed);
  border-radius: var(--border-radius-md, 6px);
  background: transparent;
  color: var(--color-text-secondary, #606266);
  cursor: pointer;
  transition: all var(--transition-fast, 0.15s);
}

.sql-tab-add:hover {
  color: var(--color-primary, #409eff);
  border-color: var(--color-primary, #409eff);
}

.sql-toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-bottom: 1px solid var(--color-border-light, #e4e7ed);
  background: var(--color-background-primary, #ffffff);
}

.sql-toolbar .el-dropdown + .el-dropdown,
.limit-label + .el-dropdown {
  margin-left: 0;
}

.toolbar-spacer {
  flex: 1;
}

/* Where the active query is stored: a browser-only draft or a catalog virtual table. */
.storage-marker {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
  overflow: hidden;
  font-size: 12px;
  white-space: nowrap;
  color: var(--color-text-secondary, #909399);
}

.storage-marker > i {
  flex-shrink: 0;
}

.storage-marker.is-linked > i {
  color: var(--el-color-success, #67c23a);
}

.storage-marker.is-edited > i,
.storage-marker.is-edited .storage-marker-text {
  color: var(--el-color-warning, #e6a23c);
}

.storage-marker-text {
  overflow: hidden;
  text-overflow: ellipsis;
}

.storage-marker-name,
.storage-marker-link {
  min-width: 0;
  overflow: hidden;
  padding: 0;
  border: none;
  background: none;
  color: var(--color-text-primary, #303133);
  font: inherit;
  font-family: monospace;
  text-overflow: ellipsis;
}

.storage-marker-link {
  cursor: pointer;
}

.storage-marker-link:hover {
  color: var(--el-color-primary, #409eff);
  text-decoration: underline;
}

.limit-label {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
  color: var(--color-text-secondary, #606266);
}

.icon-btn {
  padding-left: 9px;
  padding-right: 9px;
}

.menu-icon {
  width: 16px;
  margin-right: 6px;
  text-align: center;
}

.history-query {
  max-width: 420px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-family: monospace;
  font-size: 12px;
}

.history-time {
  flex-shrink: 0;
  margin-left: 16px;
  font-size: 12px;
  color: var(--color-text-secondary, #909399);
}

.sql-body {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow: hidden;
}

.sql-editor-area {
  flex-shrink: 0;
  overflow: hidden;
  font-size: 13px;
}

.sql-splitter {
  flex: 0 0 8px;
  position: relative;
  cursor: row-resize;
  background: var(--color-background-secondary, #f5f7fa);
  border-top: 1px solid var(--color-border-light, #e4e7ed);
  border-bottom: 1px solid var(--color-border-light, #e4e7ed);
  touch-action: none;
}

/* Grip: a short centred bar that lights up on hover, focus and while dragging. */
.sql-splitter::after {
  content: "";
  position: absolute;
  top: 50%;
  left: 50%;
  width: 40px;
  height: 3px;
  border-radius: 2px;
  background: var(--color-border-primary, #c0c4cc);
  transform: translate(-50%, -50%);
  transition: background var(--transition-fast, 0.15s);
}

.sql-splitter:hover::after,
.sql-splitter:focus-visible::after,
.is-resizing .sql-splitter::after {
  background: var(--color-primary, #409eff);
}

.sql-splitter:hover,
.sql-splitter:focus-visible,
.is-resizing .sql-splitter {
  outline: none;
  background: color-mix(in srgb, var(--el-color-primary) 10%, transparent);
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
  min-height: 0;
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
