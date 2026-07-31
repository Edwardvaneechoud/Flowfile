<template>
  <div class="dp-col-header" @click="onHeaderClicked">
    <span class="dp-col-header__label" :title="params.displayName">{{ params.displayName }}</span>
    <span v-if="params.dataType" class="dp-col-header__dtype" :title="params.dataType">{{
      params.dataType
    }}</span>
    <span v-if="sortDirection" class="dp-col-header__icon material-icons" aria-hidden="true">
      {{ sortDirection === "asc" ? "arrow_upward" : "arrow_downward" }}
    </span>
    <span class="dp-col-header__spacer"></span>
    <button
      class="dp-col-header__info"
      type="button"
      :title="`Statistics for ${params.displayName}`"
      :aria-label="`Statistics for ${params.displayName}`"
      @click.stop="onInfoClicked"
    >
      <span class="material-icons" aria-hidden="true">info_outline</span>
    </button>
  </div>
</template>

<script setup lang="ts">
// Custom AG Grid header: label + data-type pill + click-to-sort + a dedicated
// ⓘ button that requests column statistics via grid context. The ⓘ is a
// separate, deliberate click — stats compute never rides along on a sort click.
// dataType arrives via headerComponentParams from the preview's table_schema.
import { onBeforeUnmount, ref } from "vue";
import type { IHeaderParams } from "@ag-grid-community/core";

const props = defineProps<{ params: IHeaderParams & { dataType?: string } }>();

const sortDirection = ref<"asc" | "desc" | null>(props.params.column.getSort() ?? null);

const onSortChanged = () => {
  sortDirection.value = props.params.column.getSort() ?? null;
};
props.params.column.addEventListener("sortChanged", onSortChanged);
onBeforeUnmount(() => {
  props.params.column.removeEventListener("sortChanged", onSortChanged);
});

const onHeaderClicked = (event: MouseEvent) => {
  if (!props.params.enableSorting) return;
  props.params.progressSort(event.shiftKey);
};

const onInfoClicked = (event: MouseEvent) => {
  const anchor = (event.currentTarget as HTMLElement).getBoundingClientRect();
  props.params.context?.onRequestColumnStats?.(props.params.column.getColId(), anchor);
};
</script>

<!-- Unscoped on purpose: AG Grid's Vue wrapper mounts this component via
     `extends`, which drops the SFC scope id, so scoped rules would never match.
     Class names are namespaced (dp-col-header__*) instead. -->
<style>
.dp-col-header {
  display: flex;
  align-items: center;
  gap: 4px;
  width: 100%;
  height: 100%;
  overflow: hidden;
  cursor: pointer;
}

.dp-col-header__label {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.dp-col-header__dtype {
  flex-shrink: 1;
  min-width: 0;
  max-width: 90px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  padding: 0 5px;
  border-radius: 7px;
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-primary);
  color: var(--color-text-secondary);
  font-size: 9px;
  font-weight: 500;
  line-height: 13px;
  text-transform: none;
  letter-spacing: 0;
}

.dp-col-header__icon {
  font-size: 13px;
  color: var(--color-text-secondary);
  flex-shrink: 0;
}

.dp-col-header__spacer {
  flex: 1;
}

.dp-col-header__info {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border: none;
  background: transparent;
  padding: 0 1px;
  cursor: pointer;
  color: var(--color-text-secondary);
  opacity: 0.55;
  flex-shrink: 0;
}

.dp-col-header__info .material-icons {
  font-size: 14px;
}

.dp-col-header__info:hover {
  opacity: 1;
  color: var(--color-accent, #6366f1);
}
</style>
