<template>
  <span class="filter-control" :class="`filter-control-${variant}`">
    <template v-if="filter.kind === 'categorical'">
      <el-select
        :key="several ? 'several' : 'one'"
        :model-value="several ? chosen : (chosen[0] ?? undefined)"
        :multiple="several"
        :clearable="!several"
        filterable
        remote
        :remote-method="onQuery"
        :loading="searching"
        :allow-create="categoricalAllowCreate"
        default-first-option
        collapse-tags
        collapse-tags-tooltip
        size="small"
        :placeholder="categoricalPlaceholder"
        class="filter-chip-input"
        :disabled="disabled"
        :no-data-text="categoricalNoDataText"
        @update:model-value="(v: unknown) => updateState(selectionState(filter, v))"
        @visible-change="onDropdownVisible"
      >
        <el-option v-for="opt in categoricalOptions" :key="opt" :value="opt" :label="opt" />
        <template v-if="cappedHint" #footer>
          <span class="filter-values-capped">{{ cappedHint }}</span>
        </template>
      </el-select>
      <span
        v-if="baseList?.truncated"
        class="filter-chip-hint"
        :title="`Only the first ${VALUE_SEARCH_LIMIT} values are listed; type to search them all.`"
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
import {
  formatFilterValue,
  matchesQuery,
  allowsSeveral,
  selectedValues,
  selectionState,
  valueOptions,
  VALUE_SEARCH_LIMIT,
  type ValueSearchResult,
} from "./filterValues";

/**
 * The input of one dashboard filter, shared by the filter strip and a filter tile on the canvas,
 * so both drive the same filter object. It loads the column's values itself: a table's through
 * its column stats, a SQL chart's through a value search, and either source's through a value
 * search while the user types into a list that holds only the first values.
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
    /** Distinct values of the filter's field on its own source that contain the text. */
    searchValues?: (filter: DashboardFilter, text: string) => Promise<ValueSearchResult>;
    /** Bump to load the values again (manual dashboard refresh). */
    statsRefreshNonce?: number;
  }>(),
  {
    disabled: false,
    variant: "chip",
    getColumnStats: undefined,
    searchValues: undefined,
    statsRefreshNonce: 0,
  },
);

const emit = defineEmits<{
  (e: "update:filter", value: DashboardFilter): void;
}>();

/** The values the input lists before anything is typed, plus a numeric column's bounds. */
interface BaseList {
  values: string[];
  truncated: boolean;
  min: unknown;
  max: unknown;
}

const baseList = ref<BaseList | null>(null);

const sourceSignature = () =>
  `${props.filter.id}:${props.filter.datasource_id ?? ""}:${props.filter.datasource_viz_id ?? ""}:${props.filter.field_name}`;

watch(
  () => [props.statsRefreshNonce, sourceSignature()],
  async () => {
    const {
      datasource_id: tableId,
      datasource_viz_id: vizId,
      field_name: column,
      id,
    } = props.filter;
    const signature = sourceSignature();
    if (!column) return;
    try {
      let next: BaseList | null = null;
      if (tableId != null && props.getColumnStats) {
        // getColumnStats caches per (table, column), so a refetch on rebinding is cheap.
        const resp = await props.getColumnStats(tableId, column, VALUE_SEARCH_LIMIT);
        next = {
          values: (resp.values ?? []).map(formatFilterValue),
          truncated: resp.truncated,
          min: resp.min,
          max: resp.max,
        };
      } else if (vizId != null && props.searchValues && props.filter.kind === "categorical") {
        const resp = await props.searchValues(props.filter, "");
        next = { values: resp.values, truncated: resp.capped, min: null, max: null };
      }
      if (next && sourceSignature() === signature) baseList.value = next;
    } catch (err) {
      console.warn(`[dashboard] values failed for filter ${id}:`, err);
    }
  },
  { immediate: true },
);

const updateState = (state: Record<string, unknown>) => {
  emit("update:filter", { ...props.filter, state: { ...props.filter.state, ...state } });
};

const several = computed(() => allowsSeveral(props.filter));
const chosen = computed<string[]>(() => selectedValues(props.filter));

const query = ref("");
const searchResult = ref<(ValueSearchResult & { text: string }) | null>(null);
const searching = ref(false);
const searchFailed = ref(false);
let searchSeq = 0;

// A complete list is searched in the browser; only a list cut at the limit asks the source.
const onQuery = async (text: string) => {
  query.value = text;
  const seq = ++searchSeq;
  if (text === "" || !baseList.value?.truncated || !props.searchValues) {
    searching.value = false;
    return;
  }
  searching.value = true;
  try {
    const result = await props.searchValues(props.filter, text);
    if (seq === searchSeq) {
      searchResult.value = { ...result, text };
      searchFailed.value = false;
    }
  } catch (err) {
    if (seq === searchSeq) searchFailed.value = true;
    console.warn(`[dashboard] value search failed for filter ${props.filter.id}:`, err);
  } finally {
    if (seq === searchSeq) searching.value = false;
  }
};

const onDropdownVisible = (open: boolean) => {
  if (!open) void onQuery("");
};

const searched = computed(() =>
  query.value !== "" && searchResult.value?.text === query.value ? searchResult.value : null,
);

// The chosen values always show as chosen, whatever the search returned.
const categoricalOptions = computed<string[]>(() => {
  const listed = searched.value
    ? searched.value.values
    : (baseList.value?.values ?? []).filter((v) => matchesQuery(v, query.value));
  return valueOptions(listed, chosen.value);
});

const cappedHint = computed<string | null>(() => {
  if (searched.value?.capped) {
    return `Showing the first ${VALUE_SEARCH_LIMIT} matches; type more to narrow them.`;
  }
  if (query.value === "" && baseList.value?.truncated) {
    return `Showing the first ${VALUE_SEARCH_LIMIT} values; type to search them all.`;
  }
  return null;
});

// Free text only while the values are unknown, or cut short with no working search to find the rest.
const categoricalAllowCreate = computed(
  () =>
    !baseList.value || (baseList.value.truncated && (!props.searchValues || searchFailed.value)),
);

const categoricalPlaceholder = computed(() => {
  if (!baseList.value) return "Loading values…";
  if (!baseList.value.values.length) return "No values found — type to add";
  return several.value ? "Select values…" : "Select a value…";
});

const categoricalNoDataText = computed(() => {
  if (!props.getColumnStats && !props.searchValues) return "Type to add a value";
  if (!baseList.value) return "Loading…";
  if (searching.value) return "Searching…";
  return "No matching values";
});

const numericPlaceholder = (which: "min" | "max"): string => {
  const v = which === "min" ? baseList.value?.min : baseList.value?.max;
  if (v == null) return which === "min" ? "Min" : "Max";
  return `${which === "min" ? "≥" : "≤"} ${formatFilterValue(v)}`;
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
.filter-values-capped {
  display: block;
  color: var(--el-text-color-secondary);
  font-size: 11px;
  line-height: 1.4;
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
