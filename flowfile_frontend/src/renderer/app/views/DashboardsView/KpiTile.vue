<template>
  <div
    class="kpi"
    :class="{ 'kpi-colored': !!tile.text_color }"
    :style="tile.text_color ? { color: tile.text_color } : undefined"
    @dblclick="onDblclick"
  >
    <div v-if="unconfigured" class="kpi-state">
      <el-button v-if="mode === 'edit'" size="small" @mousedown.stop @click="openSettings">
        <el-icon><Odometer /></el-icon>
        <span>Configure KPI</span>
      </el-button>
      <span v-else class="kpi-muted">KPI not configured</span>
    </div>
    <div v-else-if="missing" class="kpi-state tile-missing">
      <el-icon><WarningFilled /></el-icon>
      <span>Visualization #{{ tile.viz_id }} no longer exists.</span>
    </div>
    <div v-else-if="error" class="kpi-state tile-error" :title="error">
      <el-icon><Warning /></el-icon>
      <span class="kpi-error-text">{{ error }}</span>
    </div>
    <div v-else-if="!hasResult" class="kpi-state kpi-loading">
      <el-skeleton :rows="2" animated />
    </div>
    <template v-else>
      <div class="kpi-label" :title="sourceTitle">{{ label }}</div>
      <div class="kpi-value-box">
        <div class="kpi-value" :class="{ 'kpi-refreshing': loading }" :style="valueStyle">
          {{ formattedValue }}
        </div>
      </div>
      <div v-if="delta" class="kpi-delta" :title="deltaTitle">
        <span class="kpi-delta-chip" :class="`is-${delta.tone}`">
          {{ DELTA_ARROW[delta.direction] }} {{ deltaText }}
        </span>
        <span class="kpi-delta-caption">{{ comparisonCaption }}</span>
      </div>
      <div v-else-if="showPeriodHint" class="kpi-delta kpi-muted">
        Add a date range filter to compare periods
      </div>
    </template>

    <KpiSettingsDialog
      v-if="showSettings"
      v-model="showSettings"
      :tile="tile"
      :date-filter="dateFilter"
      @save="(patch) => emit('update:tile', { ...tile, ...patch })"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, toRef, watch } from "vue";
import { Odometer, Warning, WarningFilled } from "@element-plus/icons-vue";
import { CatalogApi } from "../../api/catalog.api";
import {
  filtersTargetingTile,
  useDashboardComputation,
  type TileField,
} from "../../composables/useDashboardComputation";
import { useCatalogStore } from "../../stores/catalog-store";
import type { DashboardFilter, DashboardTile } from "../../types";
import KpiSettingsDialog from "./KpiSettingsDialog.vue";
import {
  autoKpiLabel,
  buildKpiPayload,
  computeDelta,
  findDateRangeFilter,
  formatDeltaAbs,
  formatDeltaPct,
  formatKpiValue,
  isKpiComplete,
  pickComparisonFilter,
  previousPeriodFilters,
  previousPeriodLabel,
  readKpiValue,
  valueSizePx,
  type KpiDelta,
} from "./kpi";

const props = defineProps<{
  tile: DashboardTile;
  mode: "edit" | "view";
  filters: DashboardFilter[];
  tileDatasource?: (tileId: string) => number | null;
  vizRefreshNonce?: number;
}>();

const emit = defineEmits<{
  (e: "update:tile", value: DashboardTile): void;
}>();

const DELTA_ARROW: Record<KpiDelta["direction"], string> = { up: "▲", down: "▼", flat: "■" };

const catalogStore = useCatalogStore();
const showSettings = ref(false);

const openSettings = () => {
  if (props.mode === "edit") showSettings.value = true;
};

const onDblclick = (e: MouseEvent) => {
  if (props.mode !== "edit") return;
  e.stopPropagation();
  openSettings();
};

defineExpose({ openSettings });

const unconfigured = computed(() => props.tile.viz_id == null || !isKpiComplete(props.tile.kpi));

// null while unknown: compute waits for the columns so cross-source filters gate correctly.
const fields = ref<TileField[] | null>(null);
const missing = ref(false);
const fieldsError = ref<string | null>(null);
let fieldsSeq = 0;

const loadFields = async () => {
  const seq = ++fieldsSeq;
  const vizId = props.tile.viz_id;
  fields.value = null;
  missing.value = false;
  fieldsError.value = null;
  if (vizId == null) return;
  try {
    const resp = await CatalogApi.getSavedVisualizationFields(vizId);
    if (seq !== fieldsSeq) return;
    fields.value = (resp.fields ?? []).map((f) => ({
      fid: String(f.fid),
      semanticType: f.semanticType,
    }));
  } catch (err: any) {
    if (seq !== fieldsSeq) return;
    if (err?.response?.status === 404) {
      missing.value = true;
    } else {
      console.error("KPI tile field load failed:", err);
      fieldsError.value = "Could not load the visualization's columns";
    }
  }
};

onMounted(loadFields);
watch([() => props.tile.viz_id, () => props.vizRefreshNonce], loadFields);

const tileRef = toRef(props, "tile");
const filtersRef = toRef(props, "filters");
const resolveDatasource = (id: string) => props.tileDatasource?.(id) ?? null;
const onMissing = () => {
  missing.value = true;
};

const current = useDashboardComputation({
  tile: tileRef,
  filters: filtersRef,
  tileDatasource: resolveDatasource,
  tileFields: () => fields.value,
  onMissing,
});

const comparisonFilter = computed(() =>
  props.tile.kpi?.comparison === "previous_period"
    ? pickComparisonFilter(props.filters, props.tile.id, resolveDatasource, fields.value)
    : null,
);

const dateFilter = computed(() =>
  findDateRangeFilter(props.filters, props.tile.id, resolveDatasource, fields.value),
);

const previousFilters = computed(() =>
  previousPeriodFilters(props.filters, comparisonFilter.value?.id),
);

// Its own instance so the two requests keep separate lastError state.
const previous = useDashboardComputation({
  tile: tileRef,
  filters: previousFilters,
  tileDatasource: resolveDatasource,
  tileFields: () => fields.value,
  onMissing,
});

const value = ref<number | null>(null);
const reference = ref<number | null>(null);
const computeError = ref<string | null>(null);
const hasResult = ref(false);
const loading = ref(false);
let requestSeq = 0;

const error = computed(() => fieldsError.value ?? computeError.value);

const filterKey = computed(() =>
  JSON.stringify(
    filtersTargetingTile(props.filters, props.tile.id, resolveDatasource, fields.value).map((f) => [
      f.id,
      f.state,
    ]),
  ),
);

const requestKey = computed(() =>
  JSON.stringify([
    props.tile.viz_id,
    props.tile.kpi ?? null,
    filterKey.value,
    props.vizRefreshNonce ?? 0,
  ]),
);

const run = async () => {
  const kpi = props.tile.kpi;
  const payload = kpi ? buildKpiPayload(kpi) : null;
  if (!kpi || !payload || props.tile.viz_id == null || !fields.value || missing.value) return;
  const seq = ++requestSeq;
  const withPrevious = kpi.comparison === "previous_period" && comparisonFilter.value != null;
  loading.value = true;
  const [rows, prevRows] = await Promise.all([
    current.computation(payload),
    withPrevious ? previous.computation(payload) : Promise.resolve(null),
  ]);
  if (seq !== requestSeq) return;
  loading.value = false;
  if (missing.value) return;
  const fieldLabel = kpi.field ?? "*";
  const read = current.lastError.value
    ? { error: current.lastError.value }
    : readKpiValue(rows as Record<string, unknown>[], fieldLabel);
  if ("error" in read) {
    computeError.value = read.error;
    return;
  }
  computeError.value = null;
  value.value = read.value;
  const prev =
    prevRows && !previous.lastError.value
      ? readKpiValue(prevRows as Record<string, unknown>[], fieldLabel)
      : null;
  reference.value = prev && "value" in prev ? prev.value : null;
  hasResult.value = true;
};

watch([requestKey, fields], run, { immediate: true });

const label = computed(() => {
  const kpi = props.tile.kpi;
  return kpi ? kpi.label || autoKpiLabel(kpi) : "";
});

const sourceTitle = computed(() => {
  const name = catalogStore.visualizationLibrary.find((v) => v.id === props.tile.viz_id)?.name;
  return name ? `Source: ${name}` : undefined;
});

const formattedValue = computed(() => formatKpiValue(value.value, props.tile.kpi ?? {}));

// A fixed size overrides the container-query fit.
const valueStyle = computed(() => {
  const px = valueSizePx(props.tile.kpi?.value_size);
  return px == null ? undefined : { fontSize: `${px}px` };
});

const referenceValue = computed<number | null>(() => {
  const kpi = props.tile.kpi;
  if (kpi?.comparison === "target") return kpi.target ?? null;
  if (kpi?.comparison === "previous_period" && comparisonFilter.value) return reference.value;
  return null;
});

const delta = computed(() =>
  computeDelta(value.value, referenceValue.value, props.tile.kpi?.higher_is_better ?? true),
);

const deltaText = computed(() => {
  const d = delta.value;
  if (!d) return "";
  return d.pct != null ? formatDeltaPct(d.pct) : formatDeltaAbs(d.abs, props.tile.kpi ?? {});
});

const comparisonCaption = computed(() => {
  const kpi = props.tile.kpi;
  if (kpi?.comparison === "target") return `vs target ${formatKpiValue(kpi.target, kpi)}`;
  return comparisonFilter.value ? (previousPeriodLabel(comparisonFilter.value.state) ?? "") : "";
});

const deltaTitle = computed(() =>
  delta.value
    ? `${formatDeltaAbs(delta.value.abs, props.tile.kpi ?? {})} ${comparisonCaption.value}`
    : undefined,
);

const showPeriodHint = computed(
  () =>
    props.mode === "edit" &&
    props.tile.kpi?.comparison === "previous_period" &&
    fields.value != null &&
    !comparisonFilter.value,
);
</script>

<style scoped>
.kpi {
  display: flex;
  flex-direction: column;
  height: 100%;
  padding: 6px 10px;
  box-sizing: border-box;
  overflow: hidden;
}
.kpi-state {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  height: 100%;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.kpi-state .el-button {
  cursor: pointer;
}
.kpi-loading {
  display: block;
}
.kpi-muted {
  color: var(--el-text-color-secondary);
}
.tile-missing,
.tile-error {
  color: var(--el-color-warning);
}
.tile-error {
  cursor: help;
}
.kpi-error-text {
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
  overflow: hidden;
}
.kpi-label {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.kpi-value-box {
  flex: 1;
  min-height: 0;
  display: flex;
  align-items: center;
  container-type: size;
}
.kpi-value {
  font-size: clamp(16px, min(70cqh, 22cqw), 56px);
  font-weight: 600;
  line-height: 1.1;
  color: var(--el-text-color-primary);
  font-variant-numeric: tabular-nums;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  transition: opacity 0.15s;
}
.kpi-refreshing {
  opacity: 0.55;
}
.kpi-colored .kpi-value {
  color: inherit;
}
.kpi-colored .kpi-label {
  color: inherit;
  opacity: 0.75;
}
.kpi-delta {
  display: flex;
  align-items: baseline;
  gap: 6px;
  font-size: 12px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.kpi-delta-chip {
  font-weight: 600;
  font-variant-numeric: tabular-nums;
}
.kpi-delta-chip.is-good {
  color: var(--color-success-dark);
}
.kpi-delta-chip.is-bad {
  color: var(--color-danger-dark);
}
.kpi-delta-chip.is-neutral,
.kpi-delta-caption {
  color: var(--el-text-color-secondary);
}
</style>
