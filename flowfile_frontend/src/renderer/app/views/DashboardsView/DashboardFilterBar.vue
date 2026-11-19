<template>
  <div v-if="stripFilters.length || mode === 'edit'" class="filter-bar">
    <span class="filter-bar-label">
      <el-icon><Filter /></el-icon> Filters
    </span>

    <div class="filter-bar-chips">
      <div
        v-for="f in stripFilters"
        :key="f.id"
        class="filter-chip"
        :class="{
          'filter-chip-drop-before': dropTarget?.id === f.id && dropTarget.position === 'before',
          'filter-chip-drop-after': dropTarget?.id === f.id && dropTarget.position === 'after',
        }"
        :title="chipTooltip(f)"
        v-on="mode === 'edit' ? reorderHandlers(f) : {}"
      >
        <span
          v-if="mode === 'edit'"
          class="filter-chip-handle"
          draggable="true"
          :title="`${chipTooltip(f)}. Drag to reorder, or onto the canvas to place it there`"
          @dragstart="onGripDragStart($event, f)"
          @dragend="dropTarget = null"
        >
          <span class="filter-chip-grip"
            ><el-icon><Rank /></el-icon
          ></span>
          <span class="filter-chip-name">{{ f.label || f.field_name }}</span>
        </span>
        <span v-else class="filter-chip-name">{{ f.label || f.field_name }}</span>
        <span
          v-if="filterSourceKey(f) == null"
          class="filter-chip-source-untied"
          title="This filter is not bound to a datasource — click the pencil to bind one"
        >
          untied
        </span>
        <span
          v-else-if="mode === 'edit' && targetsNoTile(f)"
          class="filter-chip-source-untied"
          title="The tiles this filter applied to were removed, so it filters nothing — click the pencil to choose tiles"
        >
          no tiles
        </span>
        <span
          v-else-if="mode === 'edit' && noTileHints?.[f.id]"
          class="filter-chip-source-untied"
          data-testid="filter-no-source-tile"
          :title="noTileHints[f.id]"
        >
          no tiles yet
        </span>
        <span class="filter-chip-colon">:</span>

        <DashboardFilterControl
          :filter="f"
          :disabled="mode === 'view' && readonlyView"
          :get-column-stats="getColumnStats"
          :search-values="searchValues"
          :stats-refresh-nonce="statsRefreshNonce"
          @update:filter="replaceFilter"
        />

        <button
          v-if="mode === 'edit'"
          type="button"
          class="filter-chip-icon-btn"
          :title="filterSourceKey(f) == null ? 'Bind a datasource' : 'Edit filter'"
          @click="openEdit(f)"
        >
          <el-icon><EditPen /></el-icon>
        </button>
        <button
          v-if="mode === 'edit'"
          type="button"
          class="filter-chip-icon-btn"
          title="Place on canvas"
          aria-label="Place on canvas"
          @click="emit('place', f.id)"
        >
          <el-icon><Grid /></el-icon>
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

      <el-button v-if="mode === 'edit'" size="small" plain @click="openAdd">
        <el-icon><Plus /></el-icon> Add filter
      </el-button>
      <el-button
        v-if="mode === 'view' && canReset"
        size="small"
        text
        title="Back to the filters this dashboard was saved with"
        @click="emit('reset')"
      >
        Reset
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
            v-model="draft.source_key"
            placeholder="Select a table or a SQL chart"
            filterable
            :loading="tablesLoading"
            :disabled="dialogMode === 'edit' && boundAtOpen"
            data-testid="filter-source-select"
            @change="onDatasourceChange"
          >
            <el-option-group v-for="group in sourceGroups" :key="group.label" :label="group.label">
              <el-option
                v-for="opt in group.options"
                :key="opt.key"
                :value="opt.key"
                :label="opt.label"
              />
            </el-option-group>
          </el-select>
        </el-form-item>
        <el-form-item label="Field">
          <el-select
            v-model="draft.field_name"
            :disabled="draft.source_key == null"
            placeholder="Select a column"
            filterable
            :loading="sourceLoading"
            data-testid="filter-field-select"
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
                <span class="field-opt-meta">
                  <span v-if="coverageByField[col.name]" class="field-opt-cover">
                    {{ coverageByField[col.name] }}
                  </span>
                  <span class="field-opt-dtype">{{ col.dtype }}</span>
                </span>
              </span>
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Label">
          <el-input v-model="draft.label" placeholder="(optional)" />
        </el-form-item>
        <el-form-item label="Scope">
          <el-radio-group v-model="draft.scope" :disabled="draft.source_key == null">
            <el-radio-button value="datasource">This datasource</el-radio-button>
            <el-radio-button value="all">All datasources</el-radio-button>
          </el-radio-group>
          <div class="scope-hint">
            {{
              draft.scope === "all"
                ? "Applies to every targeted tile with a same-named column of a compatible type, whatever table or query it reads from."
                : draftIsChart
                  ? "Applies only to tiles reading this chart's query."
                  : "Applies only to tiles reading from this table."
            }}
          </div>
        </el-form-item>
        <el-form-item v-if="reach.length" label="Reaches">
          <div class="reach">
            <div class="reach-summary">
              {{
                fieldsLoading ? "Checking tile columns…" : `${reachCount} of ${reach.length} tiles`
              }}
            </div>
            <ul class="reach-list">
              <li v-for="r in reach" :key="r.id" :class="`is-${r.fit.status}`">
                <el-icon>
                  <Check v-if="r.fit.status === 'fits'" />
                  <Loading v-else-if="r.fit.status === 'unknown'" />
                  <Close v-else />
                </el-icon>
                <span class="reach-label">{{ r.label }}</span>
                <span v-if="r.fit.status !== 'fits'" class="reach-reason">{{
                  reachReason(r)
                }}</span>
              </li>
            </ul>
          </div>
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
        <el-form-item v-if="draft.kind === 'categorical'" label="Values">
          <el-checkbox v-model="draft.multiple" data-testid="filter-allow-several">
            Allow several values
          </el-checkbox>
        </el-form-item>
        <el-form-item label="Apply to">
          <el-select
            v-model="draft.target_tile_ids"
            multiple
            collapse-tags
            collapse-tags-tooltip
            :disabled="!eligibleTiles.length"
            :placeholder="
              draft.targets_none
                ? 'No tiles'
                : eligibleTiles.length
                  ? draft.scope === 'all'
                    ? 'All tiles'
                    : 'All tiles for this datasource'
                  : 'No tiles use this datasource'
            "
            @change="draft.targets_none = false"
          >
            <el-option
              v-for="t in eligibleTiles"
              :key="t.id"
              :value="t.id"
              :label="t.label"
              :disabled="t.disabled"
            />
          </el-select>
          <div
            v-if="draft.source_key != null && !draft.targets_none && !eligibleTiles.length"
            class="scope-hint"
            data-testid="filter-no-source-tile-hint"
          >
            {{
              draft.scope === "all"
                ? "No tile on this dashboard reads data yet, so this filter filters nothing until you add one."
                : "No tile on this dashboard reads this source yet, so this filter filters nothing until you add a tile on it."
            }}
          </div>
          <div v-if="draft.targets_none" class="scope-hint" data-testid="filter-targets-none">
            The tiles this filter applied to were removed, so it filters nothing. Choose tiles, or
            <el-button link type="primary" size="small" @click="draft.targets_none = false">
              apply it to all tiles
            </el-button>
          </div>
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
import { Check, Close, EditPen, Filter, Grid, Loading, Plus, Rank } from "@element-plus/icons-vue";
import type {
  ColumnSchema,
  ColumnStatsResponse,
  DashboardFilter,
  DashboardFilterKind,
  DashboardFilterScope,
} from "../../types";
import type { DashboardDatasource } from "../../composables/useDashboardDatasources";
import {
  describeFieldFit,
  filterSourceKey,
  type DatasourceKey,
  type FieldFit,
  type TileField,
} from "../../composables/useDashboardComputation";
import { dtypeToDefaultFilterKind, isNumericDtype, isTemporalDtype } from "../../utils/dtype";
import { useDashboardDragAndDrop } from "../../composables/useDashboardDragAndDrop";
import { moveFilter, type DropPosition } from "./filterOrder";
import DashboardFilterControl from "./DashboardFilterControl.vue";
import { targetsNoTile } from "./filterPlacement";
import { withMultiple, type ValueSearchResult } from "./filterValues";
import {
  draftTargets,
  filterSourceGroups,
  sourceFieldsOfKey,
  sourceIdsOfKey,
  type FilterSourceOption,
} from "./filterDialog";
import { useCatalogStore } from "../../stores/catalog-store";

const props = defineProps<{
  /** Every filter of the dashboard; the dialog edits any of them. */
  filters: DashboardFilter[];
  /** Filters shown by a tile on the canvas; the strip leaves them out. */
  placedFilterIds?: Set<string>;
  mode: "edit" | "view";
  readonlyView?: boolean;
  /** View mode: the viewer changed a filter, so Reset is offered. */
  canReset?: boolean;
  datasourcesInUse: DashboardDatasource[];
  /** Tile ids per source key (`table:<id>` or `viz:<id>`). */
  tilesByDatasource: Record<DatasourceKey, string[]>;
  /** Every viz tile, the candidate set for scope "all". */
  vizTileIds?: string[];
  tileLabel?: (tileId: string) => string;
  /** Columns of a viz tile's data, null while unknown; drives the reach preview. */
  tileFields?: (tileId: string) => TileField[] | null;
  /** Fetch the columns of every viz tile; awaited when the dialog opens. */
  loadTileFields?: () => Promise<void>;
  getColumnStats?: (
    tableId: number,
    column: string,
    limit?: number,
  ) => Promise<ColumnStatsResponse>;
  /** Distinct values of a filter's field on its own source that contain the text. */
  searchValues?: (filter: DashboardFilter, text: string) => Promise<ValueSearchResult>;
  /** Bump to load every shown filter's values again (manual dashboard refresh). */
  statsRefreshNonce?: number;
  /** A source this page has read (any catalog table, not only one a tile reads); null if not. */
  datasourceOf?: (key: DatasourceKey) => DashboardDatasource | null;
  /** Read a source's name and columns, for a table no tile reads. */
  loadDatasource?: (key: DatasourceKey) => Promise<void>;
  /** Edit mode: why a filter filters nothing because no tile reads its source, by filter id. */
  noTileHints?: Record<string, string>;
}>();

const emit = defineEmits<{
  (e: "update:filters", value: DashboardFilter[]): void;
  (e: "reset"): void;
  /** Edit mode: put this filter on the canvas. */
  (e: "place", filterId: string): void;
}>();

const stripFilters = computed(() =>
  props.placedFilterIds?.size
    ? props.filters.filter((f) => !props.placedFilterIds!.has(f.id))
    : props.filters,
);

interface Draft {
  id: string | null;
  /** The source the filter is bound to, as a datasource key. */
  source_key: DatasourceKey | null;
  field_name: string;
  label: string;
  kind: DashboardFilterKind;
  scope: DashboardFilterScope;
  multiple: boolean;
  target_tile_ids: string[];
  /** The filter targets tiles but none is left; kept until the author chooses again. */
  targets_none: boolean;
}

const dialogOpen = ref(false);
const dialogMode = ref<"add" | "edit">("add");
const draft = reactive<Draft>(blankDraft());

function blankDraft(): Draft {
  return {
    id: null,
    source_key: null,
    field_name: "",
    label: "",
    kind: "categorical",
    scope: "datasource",
    multiple: true,
    target_tile_ids: [],
    targets_none: false,
  };
}

/** Whether the filter being edited was bound to a source when the dialog opened. */
const boundAtOpen = ref(false);
const draftIsChart = computed(() => draft.source_key?.startsWith("viz:") ?? false);

const generateId = () =>
  typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : `f-${Math.random().toString(36).slice(2, 10)}`;

const chipTooltip = (f: DashboardFilter): string => {
  const head = f.label ?? f.field_name;
  const key = filterSourceKey(f);
  if (key == null) return `${head} — untied (no datasource bound)`;
  const ds = props.datasourcesInUse.find((d) => d.key === key) ?? props.datasourceOf?.(key);
  const fallback =
    f.datasource_id != null ? `Table #${f.datasource_id}` : `Chart #${f.datasource_viz_id}`;
  return `${head} · ${ds?.name ?? fallback}`;
};

const catalogStore = useCatalogStore();
const tablesLoading = ref(false);

const ensureCatalogTables = async () => {
  if (catalogStore.allTables.length || tablesLoading.value) return;
  tablesLoading.value = true;
  try {
    await catalogStore.loadAllTables();
  } catch {
    // The dashboard's own sources stay offered.
  } finally {
    tablesLoading.value = false;
  }
};

const sourceLabelOf = (key: DatasourceKey): string => {
  const ds = props.datasourcesInUse.find((d) => d.key === key) ?? props.datasourceOf?.(key);
  if (ds) return ds.kind === "viz" ? `${ds.name} (SQL chart)` : ds.name;
  const [kind, id] = key.split(":");
  return kind === "viz" ? `Chart #${id}` : `Table #${id}`;
};

const sourceGroups = computed(() => {
  const current: FilterSourceOption | null =
    draft.source_key != null
      ? { key: draft.source_key, label: sourceLabelOf(draft.source_key) }
      : null;
  return filterSourceGroups(
    props.datasourcesInUse,
    catalogStore.allTables.map((t) => ({ id: t.id, name: t.full_table_name ?? t.name })),
    current,
  );
});

const draftSource = computed<DashboardDatasource | null>(() => {
  const key = draft.source_key;
  if (key == null) return null;
  return props.datasourcesInUse.find((d) => d.key === key) ?? props.datasourceOf?.(key) ?? null;
});

const fieldOptions = computed<ColumnSchema[]>(() =>
  draftSource.value ? sourceFieldsOfKey([draftSource.value], draft.source_key) : [],
);

const sourceLoading = ref(false);
let sourceSeq = 0;

const ensureDraftSource = async () => {
  const key = draft.source_key;
  if (key == null || draftSource.value || !props.loadDatasource) return;
  const mine = ++sourceSeq;
  sourceLoading.value = true;
  try {
    await props.loadDatasource(key);
  } finally {
    if (mine === sourceSeq) sourceLoading.value = false;
  }
};

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

const candidateTileIds = (sourceKey: DatasourceKey, scope: DashboardFilterScope): string[] =>
  scope === "all" ? (props.vizTileIds ?? []) : (props.tilesByDatasource[sourceKey] ?? []);

const fieldsLoading = ref(false);

const ensureTileFields = async () => {
  if (!props.loadTileFields) return;
  fieldsLoading.value = true;
  try {
    await props.loadTileFields();
  } finally {
    fieldsLoading.value = false;
  }
};

const fitFor = (tileId: string, kind: DashboardFilterKind, fieldName: string): FieldFit =>
  draft.scope === "all"
    ? describeFieldFit(kind, fieldName, props.tileFields?.(tileId) ?? null)
    : { status: "fits" };

const eligibleTiles = computed<{ id: string; label: string; disabled: boolean }[]>(() => {
  if (draft.source_key == null) return [];
  return candidateTileIds(draft.source_key, draft.scope).map((id) => {
    const status = draft.field_name ? fitFor(id, draft.kind, draft.field_name).status : "fits";
    return {
      id,
      label: props.tileLabel?.(id) ?? id,
      disabled: status === "missing" || status === "type",
    };
  });
});

/** Per-tile verdict for the draft, listed under Scope so the reach is visible before saving. */
const reach = computed<{ id: string; label: string; fit: FieldFit }[]>(() => {
  if (draft.source_key == null || !draft.field_name) return [];
  return candidateTileIds(draft.source_key, draft.scope).map((id) => ({
    id,
    label: props.tileLabel?.(id) ?? id,
    fit: fitFor(id, draft.kind, draft.field_name),
  }));
});

const reachCount = computed(() => reach.value.filter((r) => r.fit.status === "fits").length);

const reachReason = (r: { fit: FieldFit }): string => {
  if (r.fit.status === "missing") return `no ${draft.field_name} column`;
  if (r.fit.status === "type") return r.fit.reason;
  if (r.fit.status === "unknown") return "columns not loaded";
  return "";
};

/** "n/m tiles" per column under scope "all", judged by the kind its dtype defaults to. */
const coverageByField = computed<Record<string, string>>(() => {
  const out: Record<string, string> = {};
  if (draft.scope !== "all" || draft.source_key == null) return out;
  const ids = candidateTileIds(draft.source_key, "all");
  for (const col of fieldOptions.value) {
    if (fieldsLoading.value) {
      out[col.name] = "…";
      continue;
    }
    const kind = dtypeToDefaultFilterKind(col.dtype);
    const n = ids.filter(
      (id) => describeFieldFit(kind, col.name, props.tileFields?.(id) ?? null).status === "fits",
    ).length;
    out[col.name] = `${n}/${ids.length} tiles`;
  }
  return out;
});

// Explicit targets from the other scope may no longer be candidates.
watch(
  () => draft.scope,
  () => {
    const ids = new Set(eligibleTiles.value.map((t) => t.id));
    draft.target_tile_ids = draft.target_tile_ids.filter((id) => ids.has(id));
  },
);

const canSave = computed(() => draft.source_key != null && draft.field_name.trim().length > 0);

const replaceFilter = (next: DashboardFilter) => {
  emit(
    "update:filters",
    props.filters.map((x) => (x.id === next.id ? next : x)),
  );
};

const onRemove = (f: DashboardFilter) => {
  emit(
    "update:filters",
    props.filters.filter((x) => x.id !== f.id),
  );
};

// Reordering keeps its own MIME; the canvas takes the chip by the FILTER_MIME set beside it.
const FILTER_REORDER_MIME = "application/flowfile-filter-reorder";
const { onFilterDragStart } = useDashboardDragAndDrop();

const dropTarget = ref<{ id: string; position: DropPosition } | null>(null);

const isReorderDrag = (e: DragEvent): boolean =>
  !!e.dataTransfer?.types.includes(FILTER_REORDER_MIME);

const dropPositionFor = (e: DragEvent): DropPosition => {
  const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
  return e.clientX < rect.left + rect.width / 2 ? "before" : "after";
};

const onGripDragStart = (e: DragEvent, f: DashboardFilter) => {
  const dt = e.dataTransfer;
  if (!dt) return;
  dt.setData(FILTER_REORDER_MIME, f.id);
  onFilterDragStart(e, f.id);
  const chip = (e.target as HTMLElement).closest(".filter-chip");
  if (chip) {
    const rect = chip.getBoundingClientRect();
    dt.setDragImage(chip, e.clientX - rect.left, e.clientY - rect.top);
  }
};

const reorderHandlers = (f: DashboardFilter) => ({
  dragover: (e: DragEvent) => {
    if (!isReorderDrag(e)) return;
    e.preventDefault();
    if (e.dataTransfer) e.dataTransfer.dropEffect = "move";
    const position = dropPositionFor(e);
    if (dropTarget.value?.id !== f.id || dropTarget.value.position !== position) {
      dropTarget.value = { id: f.id, position };
    }
  },
  dragleave: (e: DragEvent) => {
    const chip = e.currentTarget as HTMLElement;
    if (e.relatedTarget instanceof Node && chip.contains(e.relatedTarget)) return;
    if (dropTarget.value?.id === f.id) dropTarget.value = null;
  },
  drop: (e: DragEvent) => {
    dropTarget.value = null;
    const draggedId = e.dataTransfer?.getData(FILTER_REORDER_MIME);
    if (!draggedId) return;
    e.preventDefault();
    const next = moveFilter(props.filters, draggedId, f.id, dropPositionFor(e));
    if (next !== props.filters) emit("update:filters", next);
  },
});

const onDatasourceChange = () => {
  draft.field_name = "";
  draft.target_tile_ids = [];
  draft.targets_none = false;
  void ensureDraftSource();
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
  boundAtOpen.value = false;
  void ensureTileFields();
  void ensureCatalogTables();
  dialogOpen.value = true;
};

// The Insert list's Filter: the dialog's result comes back to the editor, which places its tile.
let pendingAdd: ((filter: DashboardFilter | null) => void) | null = null;

const settlePendingAdd = (filter: DashboardFilter | null) => {
  const resolve = pendingAdd;
  pendingAdd = null;
  resolve?.(filter);
};

/** Open the add dialog for a filter the caller places; resolves with it, or null on cancel. */
const openAddForCanvas = (): Promise<DashboardFilter | null> => {
  settlePendingAdd(null);
  const result = new Promise<DashboardFilter | null>((resolve) => {
    pendingAdd = resolve;
  });
  openAdd();
  return result;
};

const openEdit = (f: DashboardFilter) => {
  dialogMode.value = "edit";
  const sourceKey = filterSourceKey(f);
  boundAtOpen.value = sourceKey != null;
  Object.assign(draft, {
    id: f.id,
    source_key: sourceKey,
    field_name: f.field_name,
    label: f.label ?? "",
    kind: f.kind,
    scope: f.scope ?? "datasource",
    multiple: f.multiple !== false,
    target_tile_ids: [...f.target_tile_ids],
    targets_none: targetsNoTile(f),
  });
  void ensureTileFields();
  void ensureCatalogTables();
  void ensureDraftSource();
  dialogOpen.value = true;
};

const resetDraft = () => {
  settlePendingAdd(null);
  Object.assign(draft, blankDraft());
  boundAtOpen.value = false;
};

const confirmDialog = () => {
  if (!canSave.value || draft.source_key == null) return;
  const { target, target_tile_ids } = draftTargets(
    candidateTileIds(draft.source_key, draft.scope),
    draft.target_tile_ids,
    draft.targets_none,
  );
  const ids = sourceIdsOfKey(draft.source_key);

  if (dialogMode.value === "add") {
    const next: DashboardFilter = {
      id: generateId(),
      field_name: draft.field_name.trim(),
      label: draft.label.trim() || null,
      kind: draft.kind,
      state: initialStateFor(draft.kind),
      target,
      target_tile_ids,
      ...ids,
      scope: draft.scope,
      multiple: draft.multiple,
    };
    if (pendingAdd) settlePendingAdd(next);
    else emit("update:filters", [...props.filters, next]);
  } else {
    const id = draft.id;
    emit(
      "update:filters",
      props.filters.map((x) => {
        if (x.id !== id) return x;
        const kindChanged = x.kind !== draft.kind;
        const fieldChanged = x.field_name !== draft.field_name.trim();
        const dsChanged = filterSourceKey(x) !== draft.source_key;
        const edited: DashboardFilter = {
          ...x,
          ...ids,
          field_name: draft.field_name.trim(),
          label: draft.label.trim() || null,
          kind: draft.kind,
          scope: draft.scope,
          // Reset state when the shape changes (kind/field/datasource)
          state: kindChanged || fieldChanged || dsChanged ? initialStateFor(draft.kind) : x.state,
          target,
          target_tile_ids,
        };
        return withMultiple(edited, draft.multiple);
      }),
    );
  }
  dialogOpen.value = false;
};

const openEditById = (filterId: string) => {
  const f = props.filters.find((x) => x.id === filterId);
  if (f) openEdit(f);
};

defineExpose({ openEdit: openEditById, openAddForCanvas });
</script>

<style scoped>
.filter-bar {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 6px;
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
.filter-chip-drop-before {
  box-shadow: inset 2px 0 0 var(--el-color-primary);
}
.filter-chip-drop-after {
  box-shadow: inset -2px 0 0 var(--el-color-primary);
}
.filter-chip-handle {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  cursor: grab;
}
.filter-chip-handle:active {
  cursor: grabbing;
}
.filter-chip-grip {
  display: inline-flex;
  align-items: center;
  margin-left: -2px;
  color: var(--el-text-color-secondary);
}
.filter-chip-handle:hover .filter-chip-grip {
  color: var(--el-color-primary);
}
.filter-chip-name {
  color: var(--el-text-color-primary);
  font-weight: 500;
}
.filter-chip-source-untied {
  white-space: nowrap;
  color: var(--el-color-warning);
  text-transform: uppercase;
  font-size: 10px;
  letter-spacing: 0.04em;
}
.scope-hint {
  width: 100%;
  margin-top: 4px;
  font-size: 11px;
  line-height: 1.4;
  color: var(--el-text-color-secondary);
}
.filter-chip-colon {
  color: var(--el-text-color-secondary);
  margin-right: 2px;
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
.field-opt-meta {
  display: flex;
  align-items: baseline;
  gap: 8px;
}
.field-opt-cover {
  color: var(--el-color-primary);
  font-size: 11px;
}
.reach {
  width: 100%;
  font-size: 12px;
  line-height: 1.5;
}
.reach-summary {
  color: var(--el-text-color-secondary);
}
.reach-list {
  list-style: none;
  margin: 2px 0 0;
  padding: 0;
}
.reach-list li {
  display: flex;
  align-items: center;
  gap: 6px;
}
.reach-list li.is-fits .el-icon {
  color: var(--el-color-success);
}
.reach-list li.is-missing .el-icon,
.reach-list li.is-type .el-icon {
  color: var(--el-color-danger);
}
.reach-list li.is-unknown .el-icon {
  color: var(--el-text-color-secondary);
}
.reach-reason {
  color: var(--el-text-color-secondary);
}
</style>
