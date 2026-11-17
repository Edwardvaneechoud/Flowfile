<template>
  <div class="rows-grid">
    <div v-if="fields == null || (pageLoading && !loadedOnce)" class="rows-state rows-loading">
      <el-skeleton :rows="4" animated />
    </div>
    <div v-else-if="error" class="rows-state rows-error" :title="error">
      <el-icon><Warning /></el-icon>
      <span class="rows-error-text">{{ error }}</span>
    </div>
    <div v-else-if="!columns.length" class="rows-state">
      <el-empty description="No columns to show" :image-size="48" />
    </div>
    <template v-else>
      <div class="rows-scroll" :class="{ 'rows-refreshing': pageLoading }">
        <table class="styled-table rows-table">
          <thead>
            <tr>
              <th
                v-for="col in columns"
                :key="col.field"
                class="rows-th"
                :class="{ 'rows-num': numeric.has(col.field) }"
                :aria-sort="ariaSort(col.field)"
                :title="`Sort by ${columnHeader(col)}`"
                @click="onHeaderClick(col.field)"
              >
                <span class="rows-th-label">{{ columnHeader(col) }}</span>
                <el-icon v-if="headerSortDirection(sort, col.field)" class="rows-sort-icon">
                  <component
                    :is="headerSortDirection(sort, col.field) === 'ascending' ? SortUp : SortDown"
                  />
                </el-icon>
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(row, rIdx) in rows" :key="rIdx">
              <td
                v-for="col in columns"
                :key="col.field"
                :class="{ 'rows-num': numeric.has(col.field) }"
                :title="cellTitle(row[col.field])"
              >
                {{ formatTableCell(row[col.field], col) }}
              </td>
            </tr>
          </tbody>
        </table>
        <div v-if="!rows.length && !pageLoading" class="rows-state rows-empty">
          <el-empty description="No rows match the filters" :image-size="48" />
        </div>
      </div>
      <div class="rows-pager">
        <span class="rows-range">{{ rangeText }}</span>
        <div class="rows-pager-buttons">
          <el-button
            size="small"
            text
            :icon="ArrowLeft"
            :disabled="page === 0 || pageLoading"
            aria-label="Previous page"
            @click="page -= 1"
          />
          <span class="rows-page">{{ page + 1 }} / {{ pages }}</span>
          <el-button
            size="small"
            text
            :icon="ArrowRight"
            :disabled="page + 1 >= pages || pageLoading"
            aria-label="Next page"
            @click="page += 1"
          />
        </div>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import { ArrowLeft, ArrowRight, SortDown, SortUp, Warning } from "@element-plus/icons-vue";
import {
  buildFilterStep,
  computeOnSource,
  type TileSource,
} from "../../composables/useDashboardComputation";
import { useGraphicWalkerCompute } from "../../composables/useGraphicWalkerCompute";
import { formatCellValue } from "../../utils/cellFormat";
import { saveCsv } from "../../utils/tableExport";
import type { DashboardFilter, DashboardTableConfig } from "../../types";
import {
  TABLE_CSV_ROW_CAP,
  buildTableCountPayload,
  buildTableCsvPayload,
  buildTablePagePayload,
  capRows,
  columnHeader,
  csvFileName,
  defaultSort,
  formatTableCell,
  headerSortDirection,
  nextHeaderSort,
  pageCount,
  readRowCount,
  tableConfigOf,
  toCsv,
  visibleColumns,
  type ColumnDtypes,
  type TableField,
  type TableSort,
} from "./tableTile";

/**
 * Rows of one source, paged and sorted on the worker.
 *
 * The caller hands it the filters that already reach this source; the grid prepends them as one
 * filter step to every query, so the rows, the total and a download agree. The page and a header
 * sort are this view's own and never saved.
 */
const props = defineProps<{
  source: TileSource | null;
  config?: DashboardTableConfig | null;
  /** The source's columns; null while they load. */
  fields: TableField[] | null;
  /** Column dtypes when the catalog knows them; columns Polars cannot sort by break no ties. */
  dtypes?: ColumnDtypes | null;
  filters: DashboardFilter[];
  /** Moves when the data may have changed; the grid then reads it again. */
  refreshKey?: string | number;
}>();

const emit = defineEmits<{
  (e: "missing"): void;
}>();

const config = computed(() => tableConfigOf(props.config));
const columns = computed(() => visibleColumns(props.config, props.fields ?? []));
const numeric = computed(
  () =>
    new Set(
      (props.fields ?? []).filter((f) => f.semanticType === "quantitative").map((f) => f.fid),
    ),
);

const headerSort = ref<TableSort | null>(null);
const sort = computed(() => headerSort.value ?? defaultSort(props.config, props.fields ?? []));
const page = ref(0);

const rows = ref<Record<string, unknown>[]>([]);
const total = ref<number | null>(null);
const pageLoading = ref(false);
const loadedOnce = ref(false);
const missing = ref(false);

const withFilters = (payload: Record<string, unknown>): Record<string, unknown> => {
  const step = buildFilterStep(props.filters, props.fields);
  const workflow = (payload.workflow as unknown[]) ?? [];
  return step ? { ...payload, workflow: [step, ...workflow] } : payload;
};

const fetcherFor = (maxRows: () => number) => async (payload: Record<string, unknown>) => {
  const source = props.source;
  if (source == null) return { rows: [], error: null };
  try {
    const resp = await computeOnSource(source, withFilters(payload), maxRows());
    return { rows: resp.rows, error: resp.error ?? null };
  } catch (err: any) {
    if (err?.response?.status === 404) {
      missing.value = true;
      emit("missing");
      return { rows: [], error: "The source no longer exists" };
    }
    throw err;
  }
};

const pageQuery = useGraphicWalkerCompute(
  fetcherFor(() => config.value.page_size),
  "dashboard-rows",
);
const countQuery = useGraphicWalkerCompute(
  fetcherFor(() => 2),
  "dashboard-rows-count",
);

const error = computed(() => pageQuery.lastError.value ?? countQuery.lastError.value);

const ready = computed(() => props.source != null && props.fields != null && !missing.value);

const dataKey = computed(() =>
  JSON.stringify([
    props.source,
    props.filters.map((f) => [f.id, f.field_name, f.kind, f.state]),
    props.refreshKey ?? 0,
    props.fields != null,
  ]),
);
const sortKey = computed(() =>
  JSON.stringify([
    sort.value,
    columns.value.map((c) => c.field),
    config.value.page_size,
    props.dtypes ?? null,
  ]),
);

let pageSeq = 0;
let countSeq = 0;

const loadPage = async () => {
  if (!ready.value) return;
  const seq = ++pageSeq;
  pageLoading.value = true;
  const result = await pageQuery.computation(
    buildTablePagePayload({
      columns: columns.value.map((c) => c.field),
      sort: sort.value,
      dtypes: props.dtypes,
      page: page.value,
      pageSize: config.value.page_size,
    }),
  );
  if (seq !== pageSeq) return;
  rows.value = result as Record<string, unknown>[];
  pageLoading.value = false;
  loadedOnce.value = true;
};

const loadCount = async () => {
  if (!ready.value) return;
  const seq = ++countSeq;
  total.value = null;
  const result = await countQuery.computation(buildTableCountPayload());
  if (seq !== countSeq) return;
  total.value = readRowCount(result as Record<string, unknown>[]);
};

// One watcher, so a change that moves several keys at once sends one page request.
let lastData: string | null = null;
let lastSort: string | null = null;
watch(
  [dataKey, sortKey, page],
  () => {
    if (dataKey.value !== lastData) {
      lastData = dataKey.value;
      lastSort = sortKey.value;
      missing.value = false;
      void loadCount();
    } else if (sortKey.value !== lastSort) {
      lastSort = sortKey.value;
    } else {
      void loadPage();
      return;
    }
    // Changed data or order starts on the first page; moving there runs this watcher again.
    if (page.value !== 0) page.value = 0;
    else void loadPage();
  },
  { immediate: true },
);

const pages = computed(() => pageCount(total.value, config.value.page_size));

const rangeText = computed(() => {
  if (total.value == null) return rows.value.length ? "Counting rows…" : "";
  if (total.value === 0) return "0 rows";
  const first = page.value * config.value.page_size + 1;
  const last = first + rows.value.length - 1;
  const fmt = (n: number) => n.toLocaleString();
  return `${fmt(first)}–${fmt(last)} of ${fmt(total.value)} row${total.value === 1 ? "" : "s"}`;
});

const onHeaderClick = (field: string) => {
  headerSort.value = nextHeaderSort(sort.value, field);
};

const ariaSort = (field: string) => {
  const dir = headerSortDirection(sort.value, field);
  return dir ?? "none";
};

const cellTitle = (value: unknown): string | undefined => {
  const text = formatCellValue(value);
  return text.length > 24 ? text : undefined;
};

/** The filtered, sorted rows of the shown columns as a CSV file, at most TABLE_CSV_ROW_CAP. */
const downloadCsv = async (title?: string | null) => {
  const source = props.source;
  if (source == null || props.fields == null || !columns.value.length) {
    ElMessage.warning("There are no rows to download yet.");
    return;
  }
  try {
    const resp = await computeOnSource(
      source,
      withFilters(
        buildTableCsvPayload({
          columns: columns.value.map((c) => c.field),
          sort: sort.value,
          dtypes: props.dtypes,
        }),
      ),
      TABLE_CSV_ROW_CAP + 1,
    );
    if (resp.error) {
      ElMessage.error(`Could not read the rows: ${resp.error}`);
      return;
    }
    const { rows: kept, capped } = capRows(resp.rows);
    await saveCsv(toCsv(columns.value, kept), csvFileName(title));
    if (capped) {
      ElMessage.warning(
        `The download holds the first ${TABLE_CSV_ROW_CAP.toLocaleString()} rows; narrow the filters to get the rest.`,
      );
    }
  } catch (err: any) {
    const detail = err?.response?.data?.detail;
    ElMessage.error(typeof detail === "string" ? detail : "Could not download the rows.");
  }
};

defineExpose({ downloadCsv });
</script>

<style scoped>
.rows-grid {
  display: flex;
  flex-direction: column;
  height: 100%;
  min-height: 0;
}
.rows-state {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  height: 100%;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.rows-loading {
  display: block;
  padding: 8px;
}
.rows-error {
  color: var(--el-color-warning);
  cursor: help;
  padding: 0 12px;
}
.rows-error-text {
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
  overflow: hidden;
}
.rows-scroll {
  flex: 1;
  min-height: 0;
  overflow: auto;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 6px;
  transition: opacity 0.15s;
}
.rows-refreshing {
  opacity: 0.6;
}
.rows-empty {
  height: auto;
  padding: 12px 0;
}
/* rows-table extends .styled-table */
.rows-table {
  table-layout: auto;
  white-space: nowrap;
}
.rows-table th,
.rows-table td {
  width: auto;
  height: 28px;
  padding: 2px 10px;
  max-width: 280px;
  overflow: hidden;
  text-overflow: ellipsis;
  font-size: 12px;
}
.rows-table thead th {
  height: 32px;
  padding: 4px 10px;
  cursor: pointer;
  user-select: none;
}
.rows-table thead th:hover {
  color: var(--el-color-primary);
}
.rows-th-label {
  vertical-align: middle;
}
.rows-sort-icon {
  margin-left: 4px;
  vertical-align: middle;
  color: var(--el-color-primary);
}
.rows-num {
  text-align: right;
  font-variant-numeric: tabular-nums;
}
.rows-pager {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 4px 2px 0;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.rows-pager-buttons {
  display: flex;
  align-items: center;
  gap: 4px;
}
.rows-pager-buttons .el-button + .el-button {
  margin-left: 0;
}
.rows-page {
  font-variant-numeric: tabular-nums;
}
</style>
