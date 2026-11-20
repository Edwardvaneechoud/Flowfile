<template>
  <div
    ref="canvasRef"
    class="canvas"
    :class="{
      'canvas-edit': mode === 'edit',
      'canvas-view': mode === 'view',
      'canvas-stacked': stacked,
      'canvas-drop-active': dragOver,
    }"
    @dragover="onCanvasDragOver"
    @dragleave="onCanvasDragLeave"
    @drop="onCanvasDrop"
    @load.capture="onImageLoad"
  >
    <div v-if="!layout.tiles.length && mode === 'view'" class="canvas-empty">
      <el-empty
        :description="hasTabs(layout) ? 'This tab has no tiles.' : 'This dashboard has no tiles.'"
      />
    </div>

    <GridLayout
      v-else
      ref="gridRef"
      v-model:layout="gridItems"
      :col-num="layout.grid.cols"
      :row-height="layout.grid.row_height"
      :is-draggable="mode === 'edit' && !stacked"
      :is-resizable="mode === 'edit' && !stacked"
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
        :min-w="minWidth(tileById[item.i]?.type)"
        :min-h="['viz', 'kpi', 'table', 'filter'].includes(tileById[item.i]?.type) ? 2 : 1"
        drag-allow-from=".tile-handle"
      >
        <DashboardTile
          :tile="tileById[item.i]"
          :mode="mode"
          :appearance="appearance"
          :filters="layout.filters"
          :viz-refresh-nonce="vizRefreshNonceFor(tileById[item.i].viz_id ?? -1)"
          :data-refresh-nonce="dataRefreshNonce"
          :tile-datasource="tileDatasource"
          :grid-cols="layout.grid.cols"
          :can-edit-chart="canEditCharts"
          :get-column-stats="getColumnStats"
          :search-values="searchValues"
          :move-targets="moveTargets"
          :no-tile-hints="noTileHints"
          @remove="onRemoveTile(item.i)"
          @duplicate="onDuplicateTile(item.i)"
          @edit-viz="emit('edit-viz', tileById[item.i].viz_id ?? null)"
          @keep-chart="emit('keep-chart', tileById[item.i].viz_id ?? null)"
          @update:tile="onUpdateTile"
          @update:filter="emit('update:filter', $event)"
          @edit-filter="emit('edit-filter', $event)"
          @move-filter-to-bar="onMoveFilterToBar"
          @delete-filter="emit('delete-filter', $event)"
          @move-to-tab="emit('move-tile-to-tab', { tileId: item.i, tabId: $event })"
        />
      </GridItem>
    </GridLayout>
  </div>
</template>

<script setup lang="ts">
import {
  computed,
  onBeforeUnmount,
  onMounted,
  ref,
  watch,
  type ComponentPublicInstance,
} from "vue";
import { GridItem, GridLayout } from "grid-layout-plus";
import { ElMessage } from "element-plus";
import DashboardTile from "./DashboardTile.vue";
import {
  useDashboardDragAndDrop,
  FILTER_MIME,
  IMAGE_MIME,
  KPI_MIME,
  NEW_FILTER_MIME,
  SEPARATOR_MIME,
  TABLE_MIME,
  TEXT_MIME,
  VIZ_MIME,
} from "../../composables/useDashboardDragAndDrop";
import { computeDropCell } from "./gridDrop";
import { FILTER_TILE_DEFAULT_SIZE, moveFilterToBar } from "./filterPlacement";
import type { ValueSearchResult } from "./filterValues";
import { STACK_BREAKPOINT_PX, stackTiles, type ImageSize } from "./stackedLayout";
import { hasTabs } from "./dashboardTabs";
import {
  IMAGE_DEFAULT_SIZE,
  NOT_AN_IMAGE_MESSAGE,
  draggedFiles,
  imageBudgetError,
  imageBytesOf,
  imageMimeOf,
} from "./imageTile";
import { ImageFileError, readImageFile } from "./readImageFile";
import { useDashboardImageBudget } from "../../composables/useDashboardImageBudget";
import type {
  ColumnStatsResponse,
  DashboardFilter,
  DashboardLayout,
  DashboardTab,
  DashboardTile as DashboardTileType,
  SeparatorOrientation,
} from "../../types";

const props = defineProps<{
  layout: DashboardLayout;
  mode: "edit" | "view";
  appearance: "light" | "dark" | "media";
  /** Map of viz_id → bump counter; tiles whose viz_id matches receive this
   * counter and remount/re-fetch when it changes. */
  vizRefreshNonces?: Record<number, number>;
  /** Bumped on every dashboard refresh, for tiles that read a table without a chart. */
  dataRefreshNonce?: number;
  /** Resolves a tile id to the source key it reads (`table:<id>` or `viz:<id>`), if known. */
  tileDatasource?: (tileId: string) => string | null;
  /** The value list of a filter shown on the canvas. */
  getColumnStats?: (
    tableId: number,
    column: string,
    limit?: number,
  ) => Promise<ColumnStatsResponse>;
  /** The value search of a filter shown on the canvas. */
  searchValues?: (filter: DashboardFilter, text: string) => Promise<ValueSearchResult>;
  /** False hides a chart tile's edit badge and double-click from viewers; default shown. */
  canEditCharts?: boolean;
  /** The other tabs a tile may move to (editor only). */
  moveTargets?: DashboardTab[];
  /** Editor only: why a filter filters nothing because no tile reads its source, by filter id. */
  noTileHints?: Record<string, string>;
}>();

const emit = defineEmits<{
  (e: "update:layout", value: DashboardLayout): void;
  (e: "edit-viz", vizId: number | null): void;
  /** A chart the dashboard owns is to become a chart of its own. */
  (e: "keep-chart", vizId: number | null): void;
  (e: "add-viz-at", payload: { vizId: number; x: number; y: number }): void;
  (e: "add-text-at", payload: { x: number; y: number }): void;
  (e: "add-kpi-at", payload: { x: number; y: number }): void;
  (e: "add-table-at", payload: { x: number; y: number }): void;
  (
    e: "add-separator-at",
    payload: { x: number; y: number; orientation: SeparatorOrientation },
  ): void;
  /** The Insert list's Filter was dropped on the grid: a filter still to be made goes here. */
  (e: "add-filter-at", payload: { x: number; y: number }): void;
  /** The Insert list's Image (no data URL), or an image file read and checked, dropped on the grid. */
  (e: "add-image-at", payload: { x: number; y: number; dataUrl: string | null }): void;
  /** A filter chip was dropped on the grid. */
  (e: "place-filter-at", payload: { filterId: string; x: number; y: number }): void;
  /** A filter tile's control changed its filter; the view owns where filters live. */
  (e: "update:filter", value: DashboardFilter): void;
  (e: "edit-filter", filterId: string): void;
  (e: "delete-filter", filterId: string): void;
  (e: "move-tile-to-tab", payload: { tileId: string; tabId: string }): void;
}>();

const {
  isDraggingViz,
  isDraggingText,
  isDraggingSeparator,
  isDraggingKpi,
  isDraggingTable,
  isDraggingImage,
  isDraggingFilter,
  isDraggingNewFilter,
} = useDashboardDragAndDrop();
const canvasRef = ref<HTMLElement | null>(null);
const gridRef = ref<ComponentPublicInstance | null>(null);
const dragOver = ref(false);

const dragHasOurPayload = (e: DragEvent): boolean => {
  if (
    isDraggingViz.value ||
    isDraggingText.value ||
    isDraggingSeparator.value ||
    isDraggingKpi.value ||
    isDraggingTable.value ||
    isDraggingImage.value ||
    isDraggingFilter.value ||
    isDraggingNewFilter.value
  ) {
    return true;
  }
  const types = e.dataTransfer?.types;
  return (
    !!types &&
    (types.includes(VIZ_MIME) ||
      types.includes(TEXT_MIME) ||
      types.includes(SEPARATOR_MIME) ||
      types.includes(KPI_MIME) ||
      types.includes(TABLE_MIME) ||
      types.includes(IMAGE_MIME) ||
      types.includes(FILTER_MIME) ||
      types.includes(NEW_FILTER_MIME))
  );
};

// Files from the computer: an image becomes a tile, anything else is refused on drop with a message.
const dragHasFiles = (e: DragEvent): boolean =>
  !!e.dataTransfer &&
  draggedFiles(Array.from(e.dataTransfer.types), Array.from(e.dataTransfer.items)) !== "none";

const onCanvasDragOver = (e: DragEvent) => {
  if (props.mode !== "edit") return;
  // Payload is unreadable during dragover, so gate on our shared flags (and the
  // advertised MIME types) to ignore unrelated text drags.
  if (!dragHasOurPayload(e) && !dragHasFiles(e)) return;
  e.preventDefault(); // required so the subsequent `drop` fires
  if (e.dataTransfer) e.dataTransfer.dropEffect = "copy";
  dragOver.value = true;
};

const onCanvasDragLeave = (e: DragEvent) => {
  const target = e.currentTarget;
  if (
    target instanceof HTMLElement &&
    e.relatedTarget instanceof Node &&
    target.contains(e.relatedTarget)
  ) {
    return; // moving between children, still inside the canvas
  }
  dragOver.value = false;
};

const onCanvasDrop = (e: DragEvent) => {
  dragOver.value = false;
  if (props.mode !== "edit") return;
  const dt = e.dataTransfer;
  if (!dt) return;
  const isText = dt.types.includes(TEXT_MIME);
  const isSeparator = dt.types.includes(SEPARATOR_MIME);
  const isKpi = dt.types.includes(KPI_MIME);
  const isTable = dt.types.includes(TABLE_MIME);
  const isImage = dt.types.includes(IMAGE_MIME);
  const filterId = dt.getData(FILTER_MIME);
  const isNewFilter = dt.types.includes(NEW_FILTER_MIME);
  const orientation: SeparatorOrientation =
    dt.getData(SEPARATOR_MIME) === "vertical" ? "vertical" : "horizontal";
  const rawViz = dt.getData(VIZ_MIME);
  const vizId = rawViz ? Number(rawViz) : NaN;
  const isViz = Number.isFinite(vizId);
  const files = dragHasOurPayload(e) ? [] : Array.from(dt.files);
  if (
    !isText &&
    !isSeparator &&
    !isKpi &&
    !isTable &&
    !isImage &&
    !isViz &&
    !filterId &&
    !isNewFilter &&
    !files.length
  ) {
    return;
  }
  e.preventDefault();
  const imageFile = files.find((f) => imageMimeOf(f) !== null);
  if (files.length && !imageFile) {
    ElMessage.info(NOT_AN_IMAGE_MESSAGE);
    return;
  }

  // Text and horizontal separators span the full width, viz and table tiles half,
  // KPIs and filters a quarter, images a sixth, vertical separators two columns; the width drives the x-clamp
  // so a tile never overflows the grid.
  const cols = props.layout.grid.cols;
  const w =
    imageFile || isImage
      ? IMAGE_DEFAULT_SIZE.w
      : isViz || isTable
        ? cols / 2
        : filterId || isNewFilter
          ? FILTER_TILE_DEFAULT_SIZE.w
          : isKpi
            ? cols / 4
            : isSeparator && orientation === "vertical"
              ? 2
              : cols;
  const layoutEl =
    (gridRef.value?.$el as HTMLElement | undefined) ??
    canvasRef.value?.querySelector<HTMLElement>(".vgl-layout") ??
    null;
  // Grid not mounted — let the parent append at the next free row (y < 0).
  let cell = { x: 0, y: -1 };
  if (layoutEl) {
    const rect = layoutEl.getBoundingClientRect();
    cell = computeDropCell({
      rect: { left: rect.left, top: rect.top, width: rect.width },
      clientX: e.clientX,
      clientY: e.clientY,
      cols: props.layout.grid.cols,
      rowHeight: props.layout.grid.row_height,
      margin: 8,
      w,
    });
  }
  if (imageFile) void addImageAt(imageFile, cell);
  else if (isImage) emit("add-image-at", { ...cell, dataUrl: null });
  else if (filterId) emit("place-filter-at", { filterId, x: cell.x, y: cell.y });
  else if (isNewFilter) emit("add-filter-at", { x: cell.x, y: cell.y });
  else if (isText) emit("add-text-at", { x: cell.x, y: cell.y });
  else if (isSeparator) emit("add-separator-at", { x: cell.x, y: cell.y, orientation });
  else if (isKpi) emit("add-kpi-at", { x: cell.x, y: cell.y });
  else if (isTable) emit("add-table-at", { x: cell.x, y: cell.y });
  else emit("add-viz-at", { vizId, x: cell.x, y: cell.y });
};

const imageBudget = useDashboardImageBudget();

const addImageAt = async (file: File, cell: { x: number; y: number }) => {
  try {
    const dataUrl = await readImageFile(file, imageBudget(null));
    emit("add-image-at", { ...cell, dataUrl });
  } catch (err) {
    ElMessage.warning(err instanceof ImageFileError ? err.message : "The image could not be read.");
  }
};

const minWidth = (type: DashboardTileType["type"] | undefined): number =>
  type === "separator" ? 1 : type === "image" ? 2 : Math.max(2, props.layout.grid.cols / 6);

const vizRefreshNonceFor = (vizId: number): number => props.vizRefreshNonces?.[vizId] ?? 0;

const tileDatasource = (tileId: string): string | null => props.tileDatasource?.(tileId) ?? null;

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

// Measured, not the window: the canvas shares the window with the catalog tree and paddings.
const canvasWidth = ref(0);
let resizeObserver: ResizeObserver | null = null;

onMounted(() => {
  if (!canvasRef.value || typeof ResizeObserver === "undefined") return;
  resizeObserver = new ResizeObserver((entries) => {
    canvasWidth.value = entries[0]?.contentRect.width ?? 0;
  });
  resizeObserver.observe(canvasRef.value);
});

onBeforeUnmount(() => resizeObserver?.disconnect());

/** A narrow viewer reads the tiles in one column; the editor keeps the grid at every width. */
const stacked = computed(
  () => props.mode === "view" && canvasWidth.value > 0 && canvasWidth.value < STACK_BREAKPOINT_PX,
);

// Image tiles' pictures as drawn; `load` does not bubble, so the canvas listens in capture.
const imageSizes = ref<Record<string, ImageSize>>({});

const onImageLoad = (e: Event) => {
  const img = e.target;
  if (!(img instanceof HTMLImageElement) || !img.dataset.tileId) return;
  const size = { width: img.naturalWidth, height: img.naturalHeight };
  const known = imageSizes.value[img.dataset.tileId];
  if (known?.width === size.width && known?.height === size.height) return;
  imageSizes.value = { ...imageSizes.value, [img.dataset.tileId]: size };
};

const buildGridItems = (): GridItemModel[] =>
  stacked.value
    ? stackTiles(props.layout.tiles, props.layout.grid.cols, {
        filters: props.layout.filters,
        widthPx: canvasWidth.value,
        rowHeight: props.layout.grid.row_height,
        margin: 8,
        imageSizes: imageSizes.value,
      })
    : props.layout.tiles.map((t) => ({ i: t.id, x: t.x, y: t.y, w: t.w, h: t.h }));

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
  [() => props.layout.tiles, () => props.layout.filters, stacked, imageSizes],
  () => {
    const next = buildGridItems();
    if (sameGrid(gridItems.value, next)) return;
    gridItems.value = next;
  },
  { deep: true },
);

const onLayoutUpdated = (next: GridItemModel[]) => {
  if (props.mode !== "edit" || stacked.value) return;
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

const newTileId = () =>
  typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : `tile-${Math.random().toString(36).slice(2, 10)}`;

// The copy lands on the first free row; the grid's compaction pulls it up where there is room.
const onDuplicateTile = (id: string) => {
  const source = props.layout.tiles.find((t) => t.id === id);
  // A filter shows in one place only, so its tile is never copied.
  if (!source || source.type === "filter") return;
  if (source.image) {
    const error = imageBudgetError(imageBudget(null), imageBytesOf([source]));
    if (error) {
      ElMessage.warning(error);
      return;
    }
  }
  const freeRow = Math.max(...props.layout.tiles.map((t) => t.y + t.h));
  const copy: DashboardTileType = {
    ...source,
    kpi: source.kpi ? { ...source.kpi } : source.kpi,
    table: source.table
      ? {
          ...source.table,
          columns: source.table.columns.map((c) => ({ ...c })),
          sort_by: [...source.table.sort_by],
        }
      : source.table,
    image: source.image ? { ...source.image } : source.image,
    id: newTileId(),
    y: freeRow,
  };
  emit("update:layout", { ...props.layout, tiles: [...props.layout.tiles, copy] });
};

const onMoveFilterToBar = (filterId: string) => {
  emit("update:layout", moveFilterToBar(props.layout, filterId));
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
  padding: var(--spacing-2) 0 48px;
  background: var(--el-fill-color-blank);
}
/* The grid insets tiles by its own 8px margin; pull that back so the first
 * column lines up with the page header. */
.canvas-view {
  margin: 0 calc(-1 * var(--spacing-2));
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
.canvas-drop-active {
  outline: 2px dashed var(--el-color-primary);
  outline-offset: -4px;
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
