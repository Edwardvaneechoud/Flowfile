<template>
  <span v-if="hasValue" class="dp-geometry-cell" :title="title">
    <span class="dp-geometry-cell__icon material-icons" aria-hidden="true">{{ icon }}</span>
    <span class="dp-geometry-cell__text">{{ text }}</span>
  </span>
</template>

<script setup lang="ts">
// AG Grid cell renderer for geometry columns: a shape icon plus the formatter's
// summary. Display-only — the raw value stays in the tooltip, Cmd+C and the editor.
import { computed } from "vue";
import type { ICellRendererParams } from "@ag-grid-community/core";
import { formatCellValue } from "../../../utils/cellFormat";
import { geometryIcon } from "../../../utils/geometry";

const props = defineProps<{ params: ICellRendererParams }>();

const hasValue = computed(
  () =>
    props.params.value !== null && props.params.value !== undefined && props.params.value !== "",
);
const raw = computed(() => formatCellValue(props.params.value));
// A tooltip past this is unreadable anyway; keeps huge WKT out of the DOM on every row.
const TITLE_MAX = 2000;
const title = computed(() =>
  raw.value.length > TITLE_MAX ? `${raw.value.slice(0, TITLE_MAX)}\u2026` : raw.value,
);
const text = computed(() => props.params.valueFormatted ?? raw.value);
const icon = computed(() => geometryIcon(props.params.value));
</script>

<!-- Unscoped like ColumnStatsHeader: AG Grid mounts renderers via `extends`, dropping the scope id. -->
<style>
.dp-geometry-cell {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  max-width: 100%;
}

.dp-geometry-cell__icon {
  font-size: 14px;
  color: var(--color-accent-dark);
  flex-shrink: 0;
}

.dp-geometry-cell__text {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
</style>
