<template>
  <div class="canvas" :class="{ 'canvas-edit': mode === 'edit' }">
    <div v-if="!layout.tiles.length && mode === 'view'" class="canvas-empty">
      <el-empty description="This dashboard has no tiles." />
    </div>

    <GridLayout
      v-else
      v-model:layout="gridItems"
      :col-num="layout.grid.cols"
      :row-height="layout.grid.row_height"
      :is-draggable="mode === 'edit'"
      :is-resizable="mode === 'edit'"
      :margin="[8, 8]"
      :use-css-transforms="true"
      :vertical-compact="true"
      @layout-updated="onLayoutUpdated"
    >
      <GridItem
        v-for="item in gridItems"
        :key="item.i"
        :i="item.i"
        :x="item.x"
        :y="item.y"
        :w="item.w"
        :h="item.h"
        :min-w="2"
        :min-h="2"
        drag-allow-from=".tile-handle"
      >
        <DashboardTile
          :tile="tileById[item.i]"
          :mode="mode"
          :appearance="appearance"
          :filters="layout.filters"
          :viz-refresh-nonce="vizRefreshNonceFor(tileById[item.i].viz_id ?? -1)"
          :tile-datasource="tileDatasource"
          @remove="onRemoveTile(item.i)"
          @edit-viz="emit('edit-viz', tileById[item.i].viz_id ?? null)"
          @update:tile="onUpdateTile"
        />
      </GridItem>
    </GridLayout>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { GridItem, GridLayout } from "grid-layout-plus";
import DashboardTile from "./DashboardTile.vue";
import type { DashboardLayout, DashboardTile as DashboardTileType } from "../../types";

const props = defineProps<{
  layout: DashboardLayout;
  mode: "edit" | "view";
  appearance: "light" | "dark" | "media";
  /** Map of viz_id → bump counter; tiles whose viz_id matches receive this
   * counter and remount/re-fetch when it changes. */
  vizRefreshNonces?: Record<number, number>;
  /** Resolves a tile id to its underlying CatalogTable id (if any). */
  tileDatasource?: (tileId: string) => number | null;
}>();

const emit = defineEmits<{
  (e: "update:layout", value: DashboardLayout): void;
  (e: "edit-viz", vizId: number | null): void;
}>();

const vizRefreshNonceFor = (vizId: number): number => props.vizRefreshNonces?.[vizId] ?? 0;

const tileDatasource = (tileId: string): number | null => props.tileDatasource?.(tileId) ?? null;

interface GridItemModel {
  i: string;
  x: number;
  y: number;
  w: number;
  h: number;
}

const tileById = computed<Record<string, DashboardTileType>>(() =>
  Object.fromEntries(props.layout.tiles.map((t) => [t.id, t])),
);

const buildGridItems = (): GridItemModel[] =>
  props.layout.tiles.map((t) => ({ i: t.id, x: t.x, y: t.y, w: t.w, h: t.h }));

const sameGrid = (a: GridItemModel[], b: GridItemModel[]): boolean => {
  if (a.length !== b.length) return false;
  const map = new Map(a.map((it) => [it.i, it]));
  for (const it of b) {
    const cur = map.get(it.i);
    if (!cur) return false;
    if (cur.x !== it.x || cur.y !== it.y || cur.w !== it.w || cur.h !== it.h) return false;
  }
  return true;
};

const gridItems = ref<GridItemModel[]>(buildGridItems());

// Only rebuild gridItems when props.layout.tiles actually differ in shape.
// Without this guard, parent → canvas → grid → layout-updated → parent
// becomes an infinite loop (grid-layout-plus re-fires layout-updated when
// its `layout` prop is reassigned to an equivalent value, especially with
// vertical-compact on).
watch(
  () => props.layout.tiles,
  () => {
    const next = buildGridItems();
    if (sameGrid(gridItems.value, next)) return;
    gridItems.value = next;
  },
  { deep: true },
);

const onLayoutUpdated = (next: GridItemModel[]) => {
  if (props.mode !== "edit") return;
  const byId = Object.fromEntries(next.map((it) => [it.i, it]));
  let changed = false;
  const tiles = props.layout.tiles.map((t) => {
    const g = byId[t.id];
    if (!g) return t;
    if (g.x === t.x && g.y === t.y && g.w === t.w && g.h === t.h) return t;
    changed = true;
    return { ...t, x: g.x, y: g.y, w: g.w, h: g.h };
  });
  if (!changed) return;
  emit("update:layout", { ...props.layout, tiles });
};

const onRemoveTile = (id: string) => {
  emit("update:layout", {
    ...props.layout,
    tiles: props.layout.tiles.filter((t) => t.id !== id),
  });
};

const onUpdateTile = (next: DashboardTileType) => {
  emit("update:layout", {
    ...props.layout,
    tiles: props.layout.tiles.map((t) => (t.id === next.id ? next : t)),
  });
};
</script>

<style scoped>
.canvas {
  position: relative;
  width: 100%;
  height: 100%;
  overflow: auto;
  padding: 8px 8px 48px;
  background: var(--el-fill-color-blank);
}
/* grid-layout-plus's drag/drop placeholder leaks past the end of a
 * gesture (z-index 2 > items at z-index 0), so it tinted the dropped
 * tile. Keep it visible only while an item is actively dragging or
 * resizing; otherwise hide it. */
.canvas :deep(.vgl-layout) {
  --vgl-placeholder-bg: var(--el-color-primary);
  --vgl-placeholder-opacity: 12%;
}
.canvas
  :deep(.vgl-layout:not(:has(.vgl-item--dragging, .vgl-item--resizing)))
  .vgl-item--placeholder {
  display: none;
}
.canvas-edit {
  background: repeating-linear-gradient(
    0deg,
    var(--el-fill-color-blank),
    var(--el-fill-color-blank) 39px,
    var(--el-border-color-lighter) 39px,
    var(--el-border-color-lighter) 40px
  );
}
.canvas-empty {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
}
</style>
