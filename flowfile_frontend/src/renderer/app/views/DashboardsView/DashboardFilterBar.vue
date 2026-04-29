<template>
  <div v-if="filters.length || mode === 'edit'" class="filter-bar">
    <span class="filter-bar-label">
      <el-icon><Filter /></el-icon> Filters
    </span>

    <div class="filter-bar-chips">
      <div v-for="f in filters" :key="f.id" class="filter-chip" :title="chipTooltip(f)">
        <span class="filter-chip-name">{{ f.label || f.field_name }}</span>
        <span
          v-if="f.datasource_id == null"
          class="filter-chip-source-untied"
          title="This filter is not bound to a datasource — click the pencil to bind one"
        >
          untied
        </span>
        <span class="filter-chip-colon">:</span>

        <template v-if="f.kind === 'categorical'">
          <el-select
            :model-value="getSelectedStrings(f)"
            multiple
            filterable
            :allow-create="categoricalAllowCreate(f)"
            default-first-option
            collapse-tags
            collapse-tags-tooltip
            size="small"
            :placeholder="categoricalPlaceholder(f)"
            class="filter-chip-input"
            :disabled="mode === 'view' && readonlyView"
            :no-data-text="categoricalNoDataText(f)"
            @update:model-value="(v: string[]) => updateState(f, { selected: v })"
          >
            <el-option
              v-for="opt in categoricalOptions(f)"
              :key="opt.value"
              :value="opt.value"
              :label="opt.label"
            />
          </el-select>
          <span
            v-if="statsFor(f)?.truncated"
            class="filter-chip-hint"
            title="More distinct values exist; type to add custom values."
          >
            +more
          </span>
        </template>

        <template v-else-if="f.kind === 'numeric_range'">
          <el-input-number
            :model-value="(f.state.min as number | null) ?? undefined"
            size="small"
            :placeholder="numericPlaceholder(f, 'min')"
            class="filter-chip-num"
            :disabled="mode === 'view' && readonlyView"
            :controls="false"
            @update:model-value="
              (v: number | undefined) => updateState(f, { ...f.state, min: v ?? null })
            "
          />
          <span class="filter-chip-sep">–</span>
          <el-input-number
            :model-value="(f.state.max as number | null) ?? undefined"
            size="small"
            :placeholder="numericPlaceholder(f, 'max')"
            class="filter-chip-num"
            :disabled="mode === 'view' && readonlyView"
            :controls="false"
            @update:model-value="
              (v: number | undefined) => updateState(f, { ...f.state, max: v ?? null })
            "
          />
        </template>

        <el-date-picker
          v-else-if="f.kind === 'date_range'"
          :model-value="datePickerValue(f)"
          type="daterange"
          size="small"
          range-separator="–"
          start-placeholder="Start"
          end-placeholder="End"
          class="filter-chip-date"
          :disabled="mode === 'view' && readonlyView"
          @update:model-value="onDateRange(f, $event)"
        />

        <button
          v-if="mode === 'edit'"
          type="button"
          class="filter-chip-icon-btn"
          :title="f.datasource_id == null ? 'Bind a datasource' : 'Edit filter'"
          @click="openEdit(f)"
        >
          <el-icon><EditPen /></el-icon>
        </button>
        <button
          v-if="mode === 'edit'"
          type="button"
          class="filter-chip-icon-btn filter-chip-icon-btn-danger"
          title="Remove filter"
          @click="onRemove(f)"
        >
          <el-icon><Close /></el-icon>
        </button>
      </div>

      <el-button
        v-if="mode === 'edit'"
        size="small"
        plain
        :disabled="!datasourcesInUse.length"
        :title="
          datasourcesInUse.length ? '' : 'Add a tile backed by a catalog table to enable filters'
        "
        @click="openAdd"
      >
        <el-icon><Plus /></el-icon> Add filter
      </el-button>
    </div>

    <el-dialog
      v-model="dialogOpen"
      :title="dialogMode === 'add' ? 'Add filter' : 'Edit filter'"
      width="480px"
      append-to-body
      @closed="resetDraft"
    >
      <el-form label-width="100px" size="small">
        <el-form-item label="Datasource">
          <el-select
            v-model="draft.datasource_id"
            placeholder="Select a catalog table"
            :disabled="dialogMode === 'edit' && draft.datasource_id != null"
            @change="onDatasourceChange"
          >
            <el-option
              v-for="ds in datasourcesInUse"
              :key="ds.id"
              :value="ds.id"
              :label="ds.name"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="Field">
          <el-select
            v-model="draft.field_name"
            :disabled="draft.datasource_id == null"
            placeholder="Select a column"
            filterable
            @change="onFieldChange"
          >
            <el-option
              v-for="col in fieldOptions"
              :key="col.name"
              :value="col.name"
              :label="col.name"
            >
              <span class="field-opt">
                <span class="field-opt-name">{{ col.name }}</span>
                <span class="field-opt-dtype">{{ col.dtype }}</span>
              </span>
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Label">
          <el-input v-model="draft.label" placeholder="(optional)" />
        </el-form-item>
        <el-form-item v-if="modeOptionsVisible" label="Mode">
          <el-radio-group v-model="draft.kind">
            <el-radio-button v-if="modeOptions.includes('numeric_range')" value="numeric_range">
              Range
            </el-radio-button>
            <el-radio-button v-if="modeOptions.includes('categorical')" value="categorical">
              Categorical
            </el-radio-button>
            <el-radio-button v-if="modeOptions.includes('date_range')" value="date_range">
              Date range
            </el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="Apply to">
          <el-select
            v-model="draft.target_tile_ids"
            multiple
            collapse-tags
            collapse-tags-tooltip
            :disabled="!eligibleTiles.length"
            :placeholder="
              eligibleTiles.length
                ? 'All tiles for this datasource'
                : 'No tiles use this datasource'
            "
          >
            <el-option v-for="t in eligibleTiles" :key="t.id" :value="t.id" :label="t.label" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogOpen = false">Cancel</el-button>
        <el-button type="primary" :disabled="!canSave" @click="confirmDialog">
          {{ dialogMode === "add" ? "Add" : "Save" }}
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import { Close, EditPen, Filter, Plus } from "@element-plus/icons-vue";
import type {
  ColumnSchema,
  ColumnStatsResponse,
  DashboardFilter,
  DashboardFilterKind,
} from "../../types";
import type { DashboardDatasource } from "../../composables/useDashboardDatasources";
import { dtypeToDefaultFilterKind, isNumericDtype, isTemporalDtype } from "../../utils/dtype";

const props = defineProps<{
  filters: DashboardFilter[];
  mode: "edit" | "view";
  readonlyView?: boolean;
  datasourcesInUse: DashboardDatasource[];
  tilesByDatasource: Record<number, string[]>;
  tileLabel?: (tileId: string) => string;
  getColumnStats?: (
    tableId: number,
    column: string,
    limit?: number,
  ) => Promise<ColumnStatsResponse>;
}>();

const emit = defineEmits<{
  (e: "update:filters", value: DashboardFilter[]): void;
}>();

interface Draft {
  id: string | null;
  datasource_id: number | null;
  field_name: string;
  label: string;
  kind: DashboardFilterKind;
  target_tile_ids: string[];
}

const dialogOpen = ref(false);
const dialogMode = ref<"add" | "edit">("add");
const draft = reactive<Draft>(blankDraft());

function blankDraft(): Draft {
  return {
    id: null,
    datasource_id: null,
    field_name: "",
    label: "",
    kind: "categorical",
    target_tile_ids: [],
  };
}

const generateId = () =>
  typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : `f-${Math.random().toString(36).slice(2, 10)}`;

// Per-filter column stats. Keyed by filter id and populated lazily when
// the filter is rendered or its (datasource, field) changes.
const statsByFilter = ref<Record<string, ColumnStatsResponse>>({});

const fetchStats = async (filterId: string, tableId: number, column: string) => {
  if (!props.getColumnStats) return;
  try {
    const resp = await props.getColumnStats(tableId, column, 100);
    statsByFilter.value = { ...statsByFilter.value, [filterId]: resp };
  } catch (err) {
    console.warn(`[dashboard] column stats failed for filter ${filterId}:`, err);
  }
};

watch(
  () => props.filters.map((f) => `${f.id}:${f.datasource_id ?? ""}:${f.field_name}` as const),
  () => {
    // Refresh stats whenever a filter's binding changes.
    for (const f of props.filters) {
      if (f.datasource_id == null || !f.field_name) continue;
      const cached = statsByFilter.value[f.id];
      // Don't refetch if the binding is unchanged and we already have data.
      if (cached) {
        // Heuristic: stats are bound to (datasource, column). If the
        // user re-pointed the filter, the stats key would be stale, so
        // we always refetch — getColumnStats has its own (table, col)
        // cache so this is cheap.
      }
      fetchStats(f.id, f.datasource_id, f.field_name);
    }
  },
  { immediate: true, deep: false },
);

const statsFor = (f: DashboardFilter): ColumnStatsResponse | null =>
  statsByFilter.value[f.id] ?? null;

const datasourceName = (id: number | null): string => {
  if (id == null) return "Untied";
  const ds = props.datasourcesInUse.find((d) => d.id === id);
  return ds?.name ?? `Table #${id}`;
};

const chipTooltip = (f: DashboardFilter): string => {
  const head = f.label ?? f.field_name;
  if (f.datasource_id == null) return `${head} — untied (no datasource bound)`;
  return `${head} · ${datasourceName(f.datasource_id)}`;
};

const fieldOptions = computed<ColumnSchema[]>(() => {
  if (draft.datasource_id == null) return [];
  const ds = props.datasourcesInUse.find((d) => d.id === draft.datasource_id);
  return ds?.schema_columns ?? [];
});

const selectedFieldDtype = computed<string | null>(() => {
  const col = fieldOptions.value.find((c) => c.name === draft.field_name);
  return col?.dtype ?? null;
});

const modeOptions = computed<DashboardFilterKind[]>(() => {
  const dtype = selectedFieldDtype.value;
  if (!dtype) return [];
  if (isTemporalDtype(dtype)) return ["date_range"];
  if (isNumericDtype(dtype)) return ["numeric_range", "categorical"];
  return ["categorical"];
});

const modeOptionsVisible = computed(() => modeOptions.value.length > 1);

const eligibleTiles = computed<{ id: string; label: string }[]>(() => {
  if (draft.datasource_id == null) return [];
  const ids = props.tilesByDatasource[draft.datasource_id] ?? [];
  return ids.map((id) => ({ id, label: props.tileLabel?.(id) ?? id }));
});

const canSave = computed(() => draft.datasource_id != null && draft.field_name.trim().length > 0);

const updateState = (f: DashboardFilter, state: Record<string, unknown>) => {
  emit(
    "update:filters",
    props.filters.map((x) => (x.id === f.id ? { ...x, state: { ...x.state, ...state } } : x)),
  );
};

const onRemove = (f: DashboardFilter) => {
  emit(
    "update:filters",
    props.filters.filter((x) => x.id !== f.id),
  );
};

const onDatasourceChange = () => {
  draft.field_name = "";
  draft.target_tile_ids = [];
};

const onFieldChange = () => {
  // Default kind from dtype; user may override via the Mode radio if multiple options.
  draft.kind = dtypeToDefaultFilterKind(selectedFieldDtype.value);
};

const initialStateFor = (kind: DashboardFilterKind): Record<string, unknown> => {
  if (kind === "categorical") return { selected: [] };
  if (kind === "numeric_range") return { min: null, max: null };
  return { start: null, end: null };
};

const openAdd = () => {
  dialogMode.value = "add";
  Object.assign(draft, blankDraft());
  dialogOpen.value = true;
};

const openEdit = (f: DashboardFilter) => {
  dialogMode.value = "edit";
  Object.assign(draft, {
    id: f.id,
    datasource_id: f.datasource_id ?? null,
    field_name: f.field_name,
    label: f.label ?? "",
    kind: f.kind,
    target_tile_ids: [...f.target_tile_ids],
  });
  dialogOpen.value = true;
};

const resetDraft = () => {
  Object.assign(draft, blankDraft());
};

const confirmDialog = () => {
  if (!canSave.value || draft.datasource_id == null) return;
  const all = props.tilesByDatasource[draft.datasource_id] ?? [];
  const explicit = draft.target_tile_ids.filter((id) => all.includes(id));
  const useAll = explicit.length === 0 || explicit.length === all.length;

  if (dialogMode.value === "add") {
    const next: DashboardFilter = {
      id: generateId(),
      field_name: draft.field_name.trim(),
      label: draft.label.trim() || null,
      kind: draft.kind,
      state: initialStateFor(draft.kind),
      target: useAll ? "all" : "tiles",
      target_tile_ids: useAll ? [] : explicit,
      datasource_id: draft.datasource_id,
    };
    emit("update:filters", [...props.filters, next]);
  } else {
    const id = draft.id;
    emit(
      "update:filters",
      props.filters.map((x) => {
        if (x.id !== id) return x;
        const kindChanged = x.kind !== draft.kind;
        const fieldChanged = x.field_name !== draft.field_name.trim();
        const dsChanged = x.datasource_id !== draft.datasource_id;
        return {
          ...x,
          datasource_id: draft.datasource_id,
          field_name: draft.field_name.trim(),
          label: draft.label.trim() || null,
          kind: draft.kind,
          // Reset state when the shape changes (kind/field/datasource)
          state: kindChanged || fieldChanged || dsChanged ? initialStateFor(draft.kind) : x.state,
          target: useAll ? "all" : "tiles",
          target_tile_ids: useAll ? [] : explicit,
        };
      }),
    );
  }
  dialogOpen.value = false;
};

const getSelectedStrings = (f: DashboardFilter): string[] =>
  Array.isArray(f.state.selected) ? (f.state.selected as unknown[]).map((v) => String(v)) : [];

const formatStatValue = (v: unknown): string => {
  if (v == null) return "";
  if (v instanceof Date) return v.toISOString();
  return String(v);
};

const categoricalOptions = (f: DashboardFilter): { value: string; label: string }[] => {
  const stats = statsFor(f);
  const fetched = (stats?.values ?? []).map((v) => formatStatValue(v));
  // Always include any user-entered values that aren't in the fetched list,
  // so they remain visible (and selected) in the dropdown.
  const selected = getSelectedStrings(f);
  const seen = new Set<string>();
  const opts: { value: string; label: string }[] = [];
  for (const v of [...fetched, ...selected]) {
    if (seen.has(v)) continue;
    seen.add(v);
    opts.push({ value: v, label: v });
  }
  return opts;
};

const categoricalAllowCreate = (f: DashboardFilter): boolean => {
  // Free-text entry is allowed when stats are unavailable or truncated.
  // When we have a complete distinct list, restrict to those options.
  const stats = statsFor(f);
  if (!stats) return true;
  return stats.truncated;
};

const categoricalPlaceholder = (f: DashboardFilter): string => {
  const stats = statsFor(f);
  if (!stats) return "Loading values…";
  if (!stats.values.length) return "No values found — type to add";
  return "Select values…";
};

const categoricalNoDataText = (f: DashboardFilter): string => {
  if (!props.getColumnStats) return "Type to add a value";
  const stats = statsFor(f);
  if (!stats) return "Loading…";
  return "No matching values";
};

const numericPlaceholder = (f: DashboardFilter, which: "min" | "max"): string => {
  const stats = statsFor(f);
  const v = which === "min" ? stats?.min : stats?.max;
  if (v == null) return which === "min" ? "Min" : "Max";
  return `${which === "min" ? "≥" : "≤"} ${formatStatValue(v)}`;
};

const datePickerValue = (f: DashboardFilter): [Date, Date] | null => {
  const start = f.state.start as string | null | undefined;
  const end = f.state.end as string | null | undefined;
  if (!start || !end) return null;
  return [new Date(start), new Date(end)];
};

const onDateRange = (f: DashboardFilter, range: [Date, Date] | null) => {
  if (!range) {
    updateState(f, { start: null, end: null });
    return;
  }
  updateState(f, {
    start: range[0]?.toISOString() ?? null,
    end: range[1]?.toISOString() ?? null,
  });
};
</script>

<style scoped>
.filter-bar {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 8px 12px;
  border-bottom: 1px solid var(--el-border-color-lighter);
  background: var(--el-fill-color-light);
  flex-wrap: wrap;
}
.filter-bar-label {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
  text-transform: uppercase;
}
.filter-bar-chips {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}
.filter-chip {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 3px 8px;
  background: var(--el-bg-color);
  border: 1px solid var(--el-border-color-light);
  border-radius: 4px;
  font-size: 12px;
}
.filter-chip-name {
  color: var(--el-text-color-primary);
  font-weight: 500;
}
.filter-chip-source-untied {
  color: var(--el-color-warning);
  text-transform: uppercase;
  font-size: 10px;
  letter-spacing: 0.04em;
}
.filter-chip-colon {
  color: var(--el-text-color-secondary);
  margin-right: 2px;
}
.filter-chip-input {
  min-width: 220px;
}
.filter-chip-num {
  width: 96px;
}
.filter-chip-sep {
  color: var(--el-text-color-secondary);
}
.filter-chip-date {
  width: 240px;
}
.filter-chip-icon-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  padding: 0;
  border: none;
  border-radius: 3px;
  background: transparent;
  color: var(--el-text-color-secondary);
  cursor: pointer;
  transition:
    background-color 0.15s,
    color 0.15s;
}
.filter-chip-icon-btn:hover {
  background: var(--el-fill-color-light);
  color: var(--el-color-primary);
}
.filter-chip-icon-btn-danger:hover {
  color: var(--el-color-danger);
}
.filter-chip-hint {
  color: var(--el-text-color-secondary);
  font-size: 10px;
  font-style: italic;
  cursor: help;
}
.field-opt {
  display: flex;
  justify-content: space-between;
  gap: 12px;
}
.field-opt-dtype {
  color: var(--el-text-color-secondary);
  font-family: var(--el-font-family-monospace, monospace);
  font-size: 11px;
}
</style>
