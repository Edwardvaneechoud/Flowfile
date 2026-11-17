<template>
  <span class="filter-control" :class="`filter-control-${variant}`">
    <template v-if="filter.kind === 'categorical'">
      <el-select
        :model-value="selectedStrings"
        multiple
        filterable
        :allow-create="categoricalAllowCreate"
        default-first-option
        collapse-tags
        collapse-tags-tooltip
        size="small"
        :placeholder="categoricalPlaceholder"
        class="filter-chip-input"
        :disabled="disabled"
        :no-data-text="categoricalNoDataText"
        @update:model-value="(v: string[]) => updateState({ selected: v })"
      >
        <el-option
          v-for="opt in categoricalOptions"
          :key="opt.value"
          :value="opt.value"
          :label="opt.label"
        />
      </el-select>
      <span
        v-if="stats?.truncated"
        class="filter-chip-hint"
        title="More distinct values exist; type to add custom values."
      >
        +more
      </span>
    </template>

    <template v-else-if="filter.kind === 'numeric_range'">
      <el-input-number
        :model-value="(filter.state.min as number | null) ?? undefined"
        size="small"
        :placeholder="numericPlaceholder('min')"
        class="filter-chip-num"
        :disabled="disabled"
        :controls="false"
        @update:model-value="
          (v: number | undefined) => updateState({ ...filter.state, min: v ?? null })
        "
      />
      <span class="filter-chip-sep">–</span>
      <el-input-number
        :model-value="(filter.state.max as number | null) ?? undefined"
        size="small"
        :placeholder="numericPlaceholder('max')"
        class="filter-chip-num"
        :disabled="disabled"
        :controls="false"
        @update:model-value="
          (v: number | undefined) => updateState({ ...filter.state, max: v ?? null })
        "
      />
    </template>

    <template v-else-if="filter.kind === 'date_range'">
      <el-select
        :model-value="presetOf(filter.state) ?? CUSTOM_RANGE"
        size="small"
        class="filter-chip-preset"
        :disabled="disabled"
        @update:model-value="onDatePreset"
      >
        <el-option :value="CUSTOM_RANGE" label="Custom range" />
        <el-option v-for="p in DATE_PRESETS" :key="p.value" :value="p.value" :label="p.label" />
      </el-select>
      <el-date-picker
        v-if="!presetOf(filter.state)"
        :model-value="datePickerValue"
        type="daterange"
        size="small"
        range-separator="–"
        start-placeholder="Start"
        end-placeholder="End"
        class="filter-chip-date"
        :disabled="disabled"
        @update:model-value="onDateRange"
      />
    </template>
  </span>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import type { ColumnStatsResponse, DashboardFilter } from "../../types";
import { DATE_PRESETS, presetOf, resolveDateRange } from "./dateRange";

/**
 * The input of one dashboard filter, shared by the filter strip and a filter tile on the canvas,
 * so both drive the same filter object. It loads the column's values itself.
 */
const props = withDefaults(
  defineProps<{
    filter: DashboardFilter;
    disabled?: boolean;
    /** "chip" lays the inputs out inline in a strip chip; "tile" fills the tile's width. */
    variant?: "chip" | "tile";
    getColumnStats?: (
      tableId: number,
      column: string,
      limit?: number,
    ) => Promise<ColumnStatsResponse>;
    /** Bump to load the values again (manual dashboard refresh). */
    statsRefreshNonce?: number;
  }>(),
  { disabled: false, variant: "chip", getColumnStats: undefined, statsRefreshNonce: 0 },
);

const emit = defineEmits<{
  (e: "update:filter", value: DashboardFilter): void;
}>();

const stats = ref<ColumnStatsResponse | null>(null);

watch(
  () => [
    props.statsRefreshNonce,
    `${props.filter.id}:${props.filter.datasource_id ?? ""}:${props.filter.field_name}`,
  ],
  async () => {
    const { datasource_id: tableId, field_name: column, id } = props.filter;
    if (!props.getColumnStats || tableId == null || !column) return;
    try {
      const resp = await props.getColumnStats(tableId, column, 100);
      // getColumnStats caches per (table, column), so a refetch on rebinding is cheap.
      if (props.filter.datasource_id === tableId && props.filter.field_name === column) {
        stats.value = resp;
      }
    } catch (err) {
      console.warn(`[dashboard] column stats failed for filter ${id}:`, err);
    }
  },
  { immediate: true },
);

const updateState = (state: Record<string, unknown>) => {
  emit("update:filter", { ...props.filter, state: { ...props.filter.state, ...state } });
};

const selectedStrings = computed<string[]>(() =>
  Array.isArray(props.filter.state.selected)
    ? (props.filter.state.selected as unknown[]).map((v) => String(v))
    : [],
);

const formatStatValue = (v: unknown): string => {
  if (v == null) return "";
  if (v instanceof Date) return v.toISOString();
  return String(v);
};

// Values typed in that are not in the fetched list stay visible (and selected).
const categoricalOptions = computed<{ value: string; label: string }[]>(() => {
  const fetched = (stats.value?.values ?? []).map((v) => formatStatValue(v));
  const seen = new Set<string>();
  const opts: { value: string; label: string }[] = [];
  for (const v of [...fetched, ...selectedStrings.value]) {
    if (seen.has(v)) continue;
    seen.add(v);
    opts.push({ value: v, label: v });
  }
  return opts;
});

// Free text only while the distinct list is unknown or truncated.
const categoricalAllowCreate = computed(() => !stats.value || stats.value.truncated);

const categoricalPlaceholder = computed(() => {
  if (!stats.value) return "Loading values…";
  if (!stats.value.values.length) return "No values found — type to add";
  return "Select values…";
});

const categoricalNoDataText = computed(() => {
  if (!props.getColumnStats) return "Type to add a value";
  if (!stats.value) return "Loading…";
  return "No matching values";
});

const numericPlaceholder = (which: "min" | "max"): string => {
  const v = which === "min" ? stats.value?.min : stats.value?.max;
  if (v == null) return which === "min" ? "Min" : "Max";
  return `${which === "min" ? "≥" : "≤"} ${formatStatValue(v)}`;
};

const datePickerValue = computed<[Date, Date] | null>(() => {
  const start = props.filter.state.start as string | null | undefined;
  const end = props.filter.state.end as string | null | undefined;
  if (!start || !end) return null;
  return [new Date(start), new Date(end)];
});

const onDateRange = (range: [Date, Date] | null) => {
  if (!range) {
    updateState({ preset: null, start: null, end: null });
    return;
  }
  updateState({
    preset: null,
    start: range[0]?.toISOString() ?? null,
    end: range[1]?.toISOString() ?? null,
  });
};

const CUSTOM_RANGE = "custom";

// Switching to a custom range starts from the window the preset was showing.
const onDatePreset = (value: string) => {
  if (value !== CUSTOM_RANGE) {
    updateState({ preset: value, start: null, end: null });
    return;
  }
  const shown = resolveDateRange(props.filter.state);
  updateState({ preset: null, start: shown?.start ?? null, end: shown?.end ?? null });
};
</script>

<style scoped>
/* In a chip the inputs sit in the chip's own row, as they always did. */
.filter-control-chip {
  display: contents;
}
.filter-control-tile {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
  width: 100%;
  min-width: 0;
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
.filter-chip-preset {
  width: 140px;
}
.filter-chip-hint {
  color: var(--el-text-color-secondary);
  font-size: 10px;
  font-style: italic;
  cursor: help;
}
.filter-control-tile .filter-chip-input {
  flex: 1 1 160px;
  min-width: 0;
}
.filter-control-tile .filter-chip-num {
  flex: 1 1 72px;
  width: auto;
  min-width: 0;
}
.filter-control-tile .filter-chip-preset {
  flex: 0 1 140px;
  min-width: 0;
}
.filter-control-tile .filter-chip-date {
  flex: 1 1 200px;
  min-width: 0;
}
</style>
