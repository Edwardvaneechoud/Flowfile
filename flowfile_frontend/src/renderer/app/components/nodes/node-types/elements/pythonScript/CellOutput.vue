<template>
  <div v-if="output" class="cell-output">
    <!-- Execution meta (time, count) -->
    <div v-if="output.execution_count" class="output-meta">
      [{{ output.execution_count }}] {{ formatTime(output.execution_time_ms) }}
    </div>

    <!-- Error block — show prominently -->
    <div v-if="output.error" class="output-error">
      <pre>{{ output.error }}</pre>
    </div>

    <!-- Display outputs -->
    <div v-for="(disp, index) in output.display_outputs" :key="index" class="display-item">
      <div v-if="disp.title" class="display-title">{{ disp.title }}</div>

      <!-- Image rendering -->
      <img
        v-if="disp.mime_type === 'image/png'"
        :src="'data:image/png;base64,' + disp.data"
        class="display-image"
        alt="Output image"
      />

      <!-- HTML rendering (plotly, custom HTML) -->
      <iframe
        v-else-if="disp.mime_type === 'text/html'"
        :srcdoc="disp.data"
        class="display-iframe"
        sandbox="allow-scripts"
        @load="autoResizeIframe($event)"
      ></iframe>

      <!-- Interactive table (flowfile_ctx.display(df)) -->
      <div v-else-if="disp.mime_type === TABLE_MIME && tablePayloads[index]" class="display-table">
        <NotebookDataTable
          :columns="tablePayloads[index]!.columns"
          :rows="tablePayloads[index]!.data"
          @selection-change="selectedRows[index] = $event"
        />
        <div class="display-table-footer">
          <span v-if="tablePayloads[index]!.truncated">
            showing {{ formatCount(tablePayloads[index]!.loaded_rows) }} of
            {{ formatCount(tablePayloads[index]!.total_rows) }} rows
          </span>
          <TableExportMenu
            class="display-table-export"
            :selected-count="selectedRows[index]?.length ?? 0"
            :copy-table-label="copyTableLabel(tablePayloads[index]!)"
            :all-rows-label="`All rows (${formatCount(tablePayloads[index]!.data.length)})`"
            :all-rows-disabled="
              tablePayloads[index]!.truncated
                ? `Showing ${formatCount(tablePayloads[index]!.loaded_rows)} of ${formatCount(tablePayloads[index]!.total_rows)} rows — only loaded rows are available here.`
                : null
            "
            @copy="onCopy(index, $event)"
            @download="onDownload(index, $event)"
          />
        </div>
      </div>

      <!-- Full Graphic Walker explorer (flowfile_ctx.explore(df)) -->
      <div
        v-else-if="disp.mime_type === EXPLORE_MIME && tablePayloads[index]"
        class="display-explore"
      >
        <VueGraphicWalker
          :data="tablePayloads[index]!.data"
          :fields="tablePayloads[index]!.fields"
          :appearance="appearance"
          default-tab="vis"
          :spec-list="[]"
        />
        <div v-if="tablePayloads[index]!.truncated" class="display-table-footer">
          showing {{ formatCount(tablePayloads[index]!.loaded_rows) }} of
          {{ formatCount(tablePayloads[index]!.total_rows) }} rows
        </div>
      </div>

      <!-- Plain text — NO v-html for security -->
      <div v-else-if="disp.mime_type === 'text/plain'" class="display-text">
        <pre>{{ disp.data }}</pre>
      </div>

      <!-- Fallback for other mime types -->
      <div v-else class="display-text">
        <pre>{{ disp.data }}</pre>
      </div>
    </div>

    <!-- stdout -->
    <div v-if="output.stdout" class="output-stdout">
      <pre>{{ output.stdout }}</pre>
    </div>

    <!-- stderr — hide when there's an error (error contains traceback) -->
    <div v-if="output.stderr && !output.error" class="output-stderr">
      <pre>{{ output.stderr }}</pre>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { computed, defineAsyncComponent, shallowReactive } from "vue";
import type { CellOutput } from "../../../../../types/node.types";
import NotebookDataTable from "./NotebookDataTable.vue";
import TableExportMenu from "../../../../common/TableExportMenu/TableExportMenu.vue";
import { useGraphicWalkerAppearance } from "@/composables/useGraphicWalkerAppearance";
import {
  DOWNLOAD_DEFAULT_ROWS,
  buildDelimited,
  copyRows,
  rowsForCellCap,
  saveCsv,
} from "../../../../../utils/tableExport";
import { TABLE_MIME, EXPLORE_MIME, isTableMime, parseTablePayload } from "./notebookDisplay";
import type { TablePayload } from "./notebookDisplay";

// Lazy — GW pulls in React; load only when an explore() output appears.
const VueGraphicWalker = defineAsyncComponent(
  () => import("../exploreData/vueGraphicWalker/VueGraphicWalker.vue"),
);

interface Props {
  output: CellOutput;
}

const props = defineProps<Props>();

const appearance = useGraphicWalkerAppearance();

// Parse each table/explore payload; malformed -> null (falls through to text).
const tablePayloads = computed(() =>
  (props.output.display_outputs ?? []).map((d) =>
    isTableMime(d.mime_type) ? parseTablePayload(d.data) : null,
  ),
);

const formatCount = (n: number): string => n.toLocaleString();

const selectedRows = shallowReactive<Record<number, Record<string, unknown>[]>>({});

const copyTableLabel = (p: TablePayload): string => {
  const cap = rowsForCellCap(p.columns.length);
  if (p.data.length > cap) return `First ${formatCount(cap)} rows`;
  return p.truncated ? `Loaded rows (${formatCount(p.data.length)})` : "Whole table";
};

function onCopy(index: number, scope: "selection" | "table") {
  const p = tablePayloads.value[index]!;
  const rows =
    scope === "selection"
      ? (selectedRows[index] ?? [])
      : p.data.slice(0, rowsForCellCap(p.columns.length));
  return copyRows(p.columns, rows, scope === "table" ? p.total_rows : rows.length, formatCount);
}

function onDownload(index: number, scope: "first" | "all") {
  const p = tablePayloads.value[index]!;
  const rows = scope === "all" ? p.data : p.data.slice(0, DOWNLOAD_DEFAULT_ROWS);
  return saveCsv(buildDelimited(p.columns, rows, ","), "notebook_table.csv");
}

const formatTime = (ms: number): string => {
  if (ms < 1) return "<1ms";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
};

const autoResizeIframe = (event: Event) => {
  const iframe = event.target as HTMLIFrameElement;
  try {
    const height = iframe.contentWindow?.document?.body?.scrollHeight;
    if (height) {
      iframe.style.height = `${Math.min(height + 20, 600)}px`;
    }
  } catch {
    // Cross-origin restriction — keep default height
  }
};
</script>

<style scoped>
.cell-output {
  padding: 0.25rem 0 0.25rem 0.5rem;
  font-size: 0.8rem;
}

.output-meta {
  font-size: 0.7rem;
  color: var(--el-text-color-secondary);
  text-align: right;
  padding-right: 0.5rem;
  margin-bottom: 0.15rem;
}

.output-error pre {
  background: #2d1117;
  color: #f85149;
  padding: 0.5rem 0.75rem;
  border-radius: 3px;
  font-size: 0.8rem;
  font-family: "Fira Code", monospace;
  overflow-x: auto;
  white-space: pre-wrap;
  word-break: break-word;
  margin: 0;
}

.display-image {
  max-width: 100%;
  border-radius: 3px;
  background: white; /* for transparent PNGs */
}

.display-iframe {
  width: 100%;
  min-height: 300px;
  border: none;
  border-radius: 3px;
}

.display-text pre {
  background: #1e1e2e;
  color: #cdd6f4;
  padding: 0.5rem 0.75rem;
  border-radius: 3px;
  font-size: 0.8rem;
  font-family: "Fira Code", monospace;
  overflow-x: auto;
  white-space: pre-wrap;
  margin: 0;
}

/* Definite-height box; inner widget fills via flex. */
.display-table,
.display-explore {
  display: flex;
  flex-direction: column;
  border: 1px solid var(--el-border-color, #dcdfe6);
  border-radius: 3px;
  overflow: hidden;
  background: var(--el-bg-color, #fff);
}

.display-table {
  height: 440px;
}

.display-explore {
  height: 560px;
}

.display-table > :first-child,
.display-explore > :first-child {
  flex: 1 1 auto;
  min-height: 0;
}

.display-table-footer {
  flex: 0 0 auto;
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 2px 8px;
  font-size: 0.7rem;
  color: var(--el-text-color-secondary);
  border-top: 1px solid var(--el-border-color-lighter, #ebeef5);
}

.display-table-export {
  margin-left: auto;
}

.output-stdout pre,
.output-stderr pre {
  font-size: 0.8rem;
  font-family: "Fira Code", monospace;
  padding: 0.25rem 0.5rem;
  white-space: pre-wrap;
  max-height: 200px;
  overflow-y: auto;
  margin: 0;
}

.output-stdout pre {
  color: var(--el-text-color-regular);
}

.output-stderr pre {
  color: #e6a23c;
}

.display-title {
  font-size: 0.75rem;
  font-weight: 500;
  color: var(--el-text-color-primary);
  margin: 0.25rem 0;
}

.display-item {
  margin-bottom: 0.5rem;
}

.display-item:last-child {
  margin-bottom: 0;
}
</style>
