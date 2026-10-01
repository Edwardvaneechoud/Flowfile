<template>
  <div v-if="output" class="cell-output">
    <!-- Execution meta (time, count) -->
    <div v-if="output.execution_count" class="output-meta">
      [{{ output.execution_count }}] {{ formatExecutionTime(output.execution_time_ms) }}
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
            :row-count="tablePayloads[index]!.data.length"
            :column-count="tablePayloads[index]!.columns.length"
            @copy="onCopy(index, $event)"
            @download="onDownload(index)"
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
import {
  TABLE_MIME,
  EXPLORE_MIME,
  formatExecutionTime,
  isTableMime,
  parseTablePayload,
} from "./notebookDisplay";

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

function onCopy(index: number, scope: "selection" | "table") {
  const p = tablePayloads.value[index]!;
  const rows =
    scope === "selection"
      ? (selectedRows[index] ?? [])
      : p.data.slice(0, rowsForCellCap(p.columns.length));
  return copyRows(p.columns, rows, scope === "table" ? p.total_rows : rows.length, formatCount);
}

function onDownload(index: number) {
  const p = tablePayloads.value[index]!;
  const rows = p.data.slice(0, DOWNLOAD_DEFAULT_ROWS);
  return saveCsv(buildDelimited(p.columns, rows, ","), "notebook_table.csv");
}

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
  font-size: var(--font-size-sm);
}

.output-meta {
  font-size: var(--font-size-2xs);
  color: var(--color-text-muted);
  font-variant-numeric: tabular-nums;
  text-align: right;
  padding-right: 0.5rem;
  margin-bottom: 0.15rem;
}

.output-error pre,
.display-text pre,
.output-stdout pre,
.output-stderr pre {
  margin: 0;
  font-family: var(--font-family-mono);
  font-size: 12px;
  line-height: 1.5;
  white-space: pre-wrap;
  overflow-x: auto;
}

.output-error pre {
  padding: 8px 12px;
  border: 1px solid color-mix(in srgb, var(--color-danger) 25%, transparent);
  border-radius: var(--border-radius-md);
  background: var(--color-danger-light);
  color: var(--color-danger-dark);
  word-break: break-word;
}

.display-image {
  max-width: 100%;
  border-radius: var(--border-radius-md);
  background: white; /* for transparent PNGs */
}

.display-iframe {
  width: 100%;
  min-height: 300px;
  border: none;
  border-radius: var(--border-radius-md);
}

.display-text pre {
  padding: 8px 12px;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
  color: var(--color-text-primary);
}

/* Tables size to their rows; the explorer gets a definite-height box its widget fills. */
.display-table,
.display-explore {
  display: flex;
  flex-direction: column;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  overflow: hidden;
  background: var(--color-background-primary);
}

.display-explore {
  height: 560px;
}

.display-explore > :first-child {
  flex: 1 1 auto;
  min-height: 0;
}

.display-table-footer {
  flex: 0 0 auto;
  display: flex;
  align-items: center;
  gap: 8px;
  min-height: 28px;
  padding: 2px 8px 2px 12px;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  background: var(--color-background-secondary);
  border-top: 1px solid var(--color-border-primary);
}

.display-table-export {
  margin-left: auto;
}

.output-stdout pre,
.output-stderr pre {
  padding: 4px 2px;
  max-height: 200px;
  overflow-y: auto;
}

.output-stdout pre {
  color: var(--color-text-secondary);
}

.output-stderr pre {
  color: var(--color-warning-dark);
}

.display-title {
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-secondary);
  margin: 0.25rem 0;
}

.display-item {
  margin-bottom: 0.5rem;
}

.display-item:last-child {
  margin-bottom: 0;
}
</style>
