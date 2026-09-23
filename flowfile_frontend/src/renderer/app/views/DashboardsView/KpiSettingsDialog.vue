<template>
  <el-dialog
    :model-value="modelValue"
    title="KPI settings"
    width="480px"
    append-to-body
    @update:model-value="emit('update:modelValue', $event)"
  >
    <el-form label-width="100px" size="small" @submit.prevent>
      <el-form-item label="Source">
        <el-select
          v-model="draft.viz_id"
          placeholder="Select a visualization"
          filterable
          :loading="catalogStore.loadingVisualizationLibrary"
          @change="onVizChange"
        >
          <el-option-group v-for="group in sourceGroups" :key="group.label" :label="group.label">
            <el-option v-for="v in group.items" :key="v.id" :value="v.id" :label="v.name" />
          </el-option-group>
        </el-select>
        <div class="kpi-hint">
          Aggregates this visualization's data source with the dashboard filters applied.
        </div>
      </el-form-item>
      <el-form-item label="Aggregation">
        <el-select v-model="draft.kpi.agg" @change="onAggChange">
          <el-option
            v-for="opt in KPI_AGG_OPTIONS"
            :key="opt.value"
            :value="opt.value"
            :label="opt.label"
          />
        </el-select>
      </el-form-item>
      <el-form-item label="Field">
        <el-select
          v-model="fieldModel"
          :disabled="draft.viz_id == null"
          :loading="fieldsLoading"
          :placeholder="fieldsError ?? 'Select a column'"
          filterable
        >
          <el-option v-if="draft.kpi.agg === 'count'" :value="ALL_ROWS" label="All rows" />
          <el-option v-for="f in fieldOptions" :key="f.fid" :value="f.fid" :label="f.fid">
            <span class="field-opt">
              <span>{{ f.fid }}</span>
              <span class="field-opt-dtype">{{ semanticLabel(f.semanticType) }}</span>
            </span>
          </el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="Label">
        <el-input
          v-model="draft.kpi.label"
          :placeholder="autoKpiLabel(draft.kpi)"
          maxlength="120"
          clearable
        />
      </el-form-item>
      <el-form-item label="Format">
        <div class="kpi-format">
          <el-switch v-model="draft.kpi.compact" active-text="Compact" />
          <el-select v-model="decimalsModel" class="kpi-decimals" title="Decimals">
            <el-option :value="AUTO_DECIMALS" label="Auto decimals" />
            <el-option
              v-for="d in DECIMAL_CHOICES"
              :key="d"
              :value="d"
              :label="`${d} decimal${d === 1 ? '' : 's'}`"
            />
          </el-select>
        </div>
      </el-form-item>
      <el-form-item label="Number size">
        <el-select v-model="draft.kpi.value_size">
          <el-option
            v-for="opt in KPI_VALUE_SIZE_OPTIONS"
            :key="opt.value"
            :value="opt.value"
            :label="opt.label"
          />
        </el-select>
      </el-form-item>
      <el-form-item label="Prefix / suffix">
        <div class="kpi-affixes">
          <el-input v-model="draft.kpi.prefix" placeholder="e.g. $" maxlength="8" />
          <el-input v-model="draft.kpi.suffix" placeholder="e.g. %" maxlength="16" />
        </div>
      </el-form-item>
      <el-form-item label="Compare to">
        <el-radio-group v-model="draft.kpi.comparison">
          <el-radio-button value="none">None</el-radio-button>
          <el-radio-button value="target">Target</el-radio-button>
          <el-radio-button value="previous_period">Previous period</el-radio-button>
        </el-radio-group>
        <div v-if="draft.kpi.comparison === 'previous_period'" class="kpi-hint">
          {{ previousPeriodStatus }}
        </div>
      </el-form-item>
      <el-form-item v-if="draft.kpi.comparison === 'target'" label="Target">
        <el-input-number v-model="targetModel" :controls="false" placeholder="Target value" />
      </el-form-item>
      <el-form-item v-if="draft.kpi.comparison !== 'none'" label="Better when">
        <el-radio-group v-model="draft.kpi.higher_is_better">
          <el-radio-button :value="true">Higher</el-radio-button>
          <el-radio-button :value="false">Lower</el-radio-button>
        </el-radio-group>
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button @click="emit('update:modelValue', false)">Cancel</el-button>
      <el-button type="primary" :disabled="!canSave" @click="onSave">Save</el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";
import type { TileField } from "../../composables/useDashboardComputation";
import type { DashboardFilter, DashboardKpi, DashboardTile } from "../../types";
import {
  DEFAULT_KPI,
  KPI_AGG_OPTIONS,
  KPI_VALUE_SIZE_OPTIONS,
  autoKpiLabel,
  fieldEligible,
  kpiNeedsField,
} from "./kpi";

const props = defineProps<{
  modelValue: boolean;
  tile: DashboardTile;
  /** The date range filter reaching this tile, bounds set or not. */
  dateFilter?: DashboardFilter | null;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (e: "save", patch: { viz_id: number; kpi: DashboardKpi }): void;
}>();

// el-select treats null as "no selection", so null-valued choices use sentinels.
const ALL_ROWS = "__kpi_all_rows__";
const AUTO_DECIMALS = -1;
const DECIMAL_CHOICES = [0, 1, 2, 3, 4, 5, 6];

const catalogStore = useCatalogStore();

const draft = reactive<{ viz_id: number | null; kpi: DashboardKpi }>({
  viz_id: null,
  kpi: { ...DEFAULT_KPI },
});

const fields = ref<TileField[]>([]);
const fieldsLoading = ref(false);
const fieldsError = ref<string | null>(null);
let fieldsSeq = 0;

const loadFields = async (vizId: number | null) => {
  const seq = ++fieldsSeq;
  fields.value = [];
  fieldsError.value = null;
  if (vizId == null) return;
  fieldsLoading.value = true;
  try {
    const resp = await CatalogApi.getSavedVisualizationFields(vizId);
    if (seq !== fieldsSeq) return;
    fields.value = (resp.fields ?? []).map((f) => ({
      fid: String(f.fid),
      semanticType: f.semanticType,
    }));
  } catch {
    if (seq === fieldsSeq) fieldsError.value = "Could not load columns";
  } finally {
    if (seq === fieldsSeq) fieldsLoading.value = false;
  }
};

const onOpen = () => {
  draft.viz_id = props.tile.viz_id;
  draft.kpi = { ...DEFAULT_KPI, ...(props.tile.kpi ?? {}) };
  if (!catalogStore.visualizationLibrary.length) {
    catalogStore.loadVisualizationLibrary().catch(() => undefined);
  }
  void loadFields(draft.viz_id);
};

// Mounted already open (v-if + v-model in one tick), so el-dialog's @open never fires.
watch(
  () => props.modelValue,
  (open) => {
    if (open) onOpen();
  },
  { immediate: true },
);

const sourceGroups = computed(() => {
  const groups = new Map<string, { label: string; items: { id: number; name: string }[] }>();
  for (const v of catalogStore.visualizationLibrary) {
    const label =
      v.source_type === "sql"
        ? "SQL"
        : (v.table_full_name ?? v.table_name ?? `Table #${v.catalog_table_id}`);
    if (!groups.has(label)) groups.set(label, { label, items: [] });
    groups.get(label)!.items.push({ id: v.id, name: v.name });
  }
  return [...groups.values()].sort((a, b) =>
    a.label === "SQL" ? 1 : b.label === "SQL" ? -1 : a.label.localeCompare(b.label),
  );
});

const fieldOptions = computed(() =>
  fields.value.filter((f) => fieldEligible(draft.kpi.agg, f.semanticType)),
);

const onAggChange = () => {
  const field = draft.kpi.field;
  if (field && !fieldOptions.value.some((f) => f.fid === field)) draft.kpi.field = null;
};

const onVizChange = async (vizId: number | null) => {
  await loadFields(vizId);
  onAggChange();
};

const fieldModel = computed<string | undefined>({
  get: () =>
    draft.kpi.field ?? (draft.kpi.agg === "count" && draft.viz_id != null ? ALL_ROWS : undefined),
  set: (v) => {
    draft.kpi.field = !v || v === ALL_ROWS ? null : v;
  },
});

const decimalsModel = computed<number>({
  get: () => draft.kpi.decimals ?? AUTO_DECIMALS,
  set: (v) => {
    draft.kpi.decimals = v === AUTO_DECIMALS ? null : v;
  },
});

const targetModel = computed<number | undefined>({
  get: () => draft.kpi.target ?? undefined,
  set: (v) => {
    draft.kpi.target = v ?? null;
  },
});

const SEMANTIC_LABEL: Record<string, string> = {
  quantitative: "number",
  nominal: "text",
  ordinal: "text",
  temporal: "date",
};
const semanticLabel = (s?: string) => (s ? (SEMANTIC_LABEL[s] ?? s) : "");

const previousPeriodStatus = computed(() => {
  const f = props.dateFilter;
  if (!f) {
    return (
      "No date range filter reaches this tile yet. In the filter strip click Add filter, " +
      "choose this KPI's datasource and a date column, then pick a start and end date. " +
      "The KPI then compares that window with the same-length window right before it."
    );
  }
  const name = f.label || f.field_name;
  const start = f.state?.start;
  const end = f.state?.end;
  if (typeof start !== "string" || !start || typeof end !== "string" || !end) {
    return `Uses the ‘${name}’ date range filter. Pick a start and end date in the filter strip to see the comparison.`;
  }
  const fmt = (d: string) => new Date(d).toLocaleDateString();
  return `Uses the ‘${name}’ date range filter (${fmt(start)} – ${fmt(end)}) and compares against the same-length window right before it.`;
});

const canSave = computed(
  () =>
    draft.viz_id != null &&
    (!kpiNeedsField(draft.kpi.agg) || !!draft.kpi.field) &&
    (draft.kpi.comparison !== "target" || draft.kpi.target != null),
);

const blankToNull = (s: string | null | undefined): string | null => (s && s.trim() ? s : null);

const onSave = () => {
  if (!canSave.value || draft.viz_id == null) return;
  emit("save", {
    viz_id: draft.viz_id,
    kpi: {
      ...draft.kpi,
      label: blankToNull(draft.kpi.label?.trim()),
      prefix: blankToNull(draft.kpi.prefix),
      suffix: blankToNull(draft.kpi.suffix),
      target: draft.kpi.comparison === "target" ? draft.kpi.target : null,
    },
  });
  emit("update:modelValue", false);
};
</script>

<style scoped>
.kpi-hint {
  width: 100%;
  margin-top: 4px;
  font-size: 11px;
  line-height: 1.4;
  color: var(--el-text-color-secondary);
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
.kpi-format,
.kpi-affixes {
  display: flex;
  align-items: center;
  gap: 12px;
  width: 100%;
}
.kpi-decimals {
  flex: 1;
}
.kpi-affixes .el-input {
  flex: 1;
}
</style>
