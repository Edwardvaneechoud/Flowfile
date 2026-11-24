<template>
  <div
    class="dash-card"
    :class="{ 'is-dark': themeStore.isDark }"
    role="button"
    tabindex="0"
    :title="`Open ${dashboard.name}`"
    @click="emit('view')"
    @keydown.enter="emit('view')"
  >
    <div class="dash-card-header">
      <i class="fa-solid fa-table-cells-large dash-card-icon"></i>
      <span class="dash-name">{{ dashboard.name }}</span>
      <SharedBadge :access="dashboard.access" />
      <span
        v-if="isHubRow(dashboard)"
        class="dash-hub-tag"
        data-testid="dashboard-on-hub"
        title="Saved in your organisation's catalog on the hub"
      >
        Hub
      </span>
      <el-dropdown trigger="click" @click.stop>
        <el-icon class="dash-card-menu" @click.stop><MoreFilled /></el-icon>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item @click.stop="emit('view')">
              <el-icon><View /></el-icon> View
            </el-dropdown-item>
            <el-dropdown-item v-if="canManage(dashboard)" @click.stop="emit('edit')">
              <el-icon><Edit /></el-icon> Edit
            </el-dropdown-item>
            <el-dropdown-item v-if="canShare(dashboard)" @click.stop="emit('share')">
              <el-icon><Share /></el-icon> Share
            </el-dropdown-item>
            <el-dropdown-item v-if="canManage(dashboard)" divided @click.stop="emit('delete')">
              <el-icon><Delete /></el-icon> Delete
            </el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
    </div>

    <div class="dash-card-body">
      <div
        v-if="tileCount"
        class="dash-preview"
        :class="{ 'is-clipped': preview.clipped }"
        aria-hidden="true"
      >
        <div
          v-for="rect in preview.rects"
          :key="rect.id"
          class="pv-slot"
          :style="{
            left: `${rect.left}%`,
            top: `${rect.top}%`,
            width: `${rect.width}%`,
            height: `${rect.height}%`,
          }"
        >
          <div
            class="pv-tile"
            :class="[`pv-${rect.type}`, `pv-${rect.orientation}`]"
            :style="tileStyle(rect)"
          >
            <template v-if="rect.type === 'viz'">
              <span class="pv-title"></span>
              <div class="pv-plot">
                <img
                  v-if="chartOf(rect)?.thumbnail"
                  :src="chartOf(rect)!.thumbnail!"
                  class="pv-img"
                  alt=""
                  loading="lazy"
                />
                <PreviewChartSketch
                  v-else-if="charts"
                  :kind="sketchKind(chartOf(rect)?.chartType)"
                  :seed="rect.vizId ?? hashSeed(rect.id)"
                  :palette="palette"
                />
              </div>
            </template>

            <template v-else-if="rect.type === 'kpi'">
              <span class="pv-kpi-label"></span>
              <span class="pv-kpi-value"></span>
              <svg
                v-if="rect.hasTrend"
                class="pv-kpi-trend"
                viewBox="0 0 100 50"
                preserveAspectRatio="none"
              >
                <polyline
                  :points="trendPoints(rect)"
                  fill="none"
                  :stroke="rect.textColor ?? palette[0]"
                  stroke-width="1.5"
                  vector-effect="non-scaling-stroke"
                />
              </svg>
            </template>

            <template v-else-if="rect.type === 'text'">
              <span class="pv-heading"></span>
              <span class="pv-text-line"></span>
              <span class="pv-text-line pv-short"></span>
            </template>

            <template v-else-if="rect.type === 'table'">
              <span class="pv-table-head"></span>
              <span v-for="n in 6" :key="n" class="pv-table-row"></span>
            </template>

            <template v-else-if="rect.type === 'filter'">
              <span class="pv-filter-box">
                <span class="pv-filter-label"></span>
                <i class="fa-solid fa-chevron-down pv-filter-chevron"></i>
              </span>
            </template>

            <template v-else-if="rect.type === 'image'">
              <i v-if="rect.hasImage" class="fa-regular fa-image pv-image-glyph"></i>
            </template>

            <span v-else-if="rect.type === 'separator'" class="pv-rule"></span>
          </div>
        </div>
      </div>
      <p v-else class="dash-card-desc dash-card-desc-empty">No tiles yet.</p>
      <p v-if="dashboard.description" class="dash-card-desc" :title="dashboard.description">
        {{ dashboard.description }}
      </p>
    </div>

    <div class="dash-card-footer">
      <span>{{ tileCount }} tile{{ tileCount === 1 ? "" : "s" }}</span>
      <template v-if="tabCount > 1">
        <span class="dot">·</span>
        <span>{{ tabCount }} tabs</span>
      </template>
      <template v-if="dashboard.namespace_name">
        <span class="dot">·</span>
        <span class="dash-footer-ns">{{ dashboard.namespace_name }}</span>
      </template>
      <span class="dash-footer-updated">Updated {{ formatDate(dashboard.updated_at) }}</span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { Delete, Edit, MoreFilled, Share, View } from "@element-plus/icons-vue";
import { formatDate } from "../CatalogView/catalog-formatters";
import type { Dashboard } from "../../types";
import SharedBadge from "../../components/sharing/SharedBadge.vue";
import { useResourceSharing } from "../../composables/useResourceSharing";
import { useThemeStore } from "../../stores/theme-store";
import {
  CATEGORY_DARK,
  CATEGORY_LIGHT,
} from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/vizTheme";
import { isHubRow } from "../CatalogView/hubRows";
import { buildLayoutPreview, type PreviewChart, type PreviewRect } from "./layoutPreview";
import { hashSeed, linePoints, lineToPoints, sketchKind } from "./previewSketch";
import PreviewChartSketch from "./PreviewChartSketch.vue";

const props = defineProps<{
  dashboard: Dashboard;
  /** viz id → its picture and drawn mark; a chart missing here draws as a bar sketch, and no map leaves plots empty. */
  charts?: Record<number, PreviewChart>;
}>();

const emit = defineEmits<{
  (e: "view"): void;
  (e: "edit"): void;
  (e: "delete"): void;
  (e: "share"): void;
}>();

const { canShare, canManage } = useResourceSharing();
const themeStore = useThemeStore();

const tileCount = computed(() => props.dashboard.layout.tiles.length);
const tabCount = computed(() => props.dashboard.layout.tabs?.length ?? 0);
const preview = computed(() => buildLayoutPreview(props.dashboard.layout));
const palette = computed(() => (themeStore.isDark ? CATEGORY_DARK : CATEGORY_LIGHT));

const chartOf = (rect: PreviewRect): PreviewChart | undefined =>
  rect.vizId != null ? props.charts?.[rect.vizId] : undefined;

const tileStyle = (rect: PreviewRect) => ({
  ...(rect.bgColor ? { background: rect.bgColor } : {}),
  ...(rect.textColor ? { color: rect.textColor } : {}),
});

const trendPoints = (rect: PreviewRect) => lineToPoints(linePoints(hashSeed(rect.id), 8));
</script>

<style scoped>
.dash-card {
  --preview-canvas: color-mix(in srgb, var(--el-text-color-primary) 5%, var(--el-bg-color));
  --preview-tile: var(--el-bg-color);
  --preview-ink: color-mix(in srgb, var(--el-text-color-primary) 70%, transparent);
  --preview-muted: color-mix(in srgb, var(--el-text-color-primary) 14%, transparent);
  --preview-rule: var(--el-border-color);
  --preview-water: color-mix(in srgb, var(--color-accent) 7%, var(--preview-tile));
  --preview-grid: color-mix(in srgb, var(--color-accent) 12%, transparent);
  --preview-land: color-mix(in srgb, var(--el-text-color-primary) 6%, var(--preview-tile));

  display: flex;
  flex-direction: column;
  border: 1px solid var(--el-border-color-light);
  border-radius: 8px;
  background: var(--el-bg-color);
  overflow: hidden;
  cursor: pointer;
  transition:
    border-color 0.15s,
    box-shadow 0.15s,
    transform 0.15s;
}
.dash-card:hover,
.dash-card:focus-visible {
  border-color: var(--el-color-primary-light-5);
  box-shadow: 0 4px 14px rgba(0, 0, 0, 0.08);
  outline: none;
}
.dash-card.is-dark .pv-img {
  filter: invert(0.9) hue-rotate(180deg);
}
.dash-card-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 12px;
  min-width: 0;
}
.dash-card-icon {
  flex-shrink: 0;
  color: var(--el-color-primary);
  font-size: 14px;
}
.dash-name {
  flex: 1;
  min-width: 0;
  font-weight: 600;
  font-size: 14px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.dash-hub-tag {
  flex-shrink: 0;
  font-size: 11px;
  padding: 0 6px;
  border-radius: 8px;
  color: var(--el-color-primary);
  background: var(--el-color-primary-light-9);
}
.dash-card-menu {
  cursor: pointer;
  color: var(--el-text-color-secondary);
}
.dash-card-body {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 0 12px 10px;
}
.dash-preview {
  position: relative;
  width: 100%;
  aspect-ratio: 16 / 10;
  border-radius: 6px;
  background: var(--preview-canvas);
  border: 1px solid var(--el-border-color-lighter);
  overflow: hidden;
}
.dash-preview.is-clipped::after {
  content: "";
  position: absolute;
  inset: auto 0 0 0;
  height: 12%;
  background: linear-gradient(to bottom, transparent, var(--preview-canvas) 90%);
  pointer-events: none;
}
.pv-slot {
  position: absolute;
  box-sizing: border-box;
  container-type: size;
}
.pv-tile {
  position: absolute;
  inset: 2px;
  display: flex;
  flex-direction: column;
  gap: clamp(2px, 6cqh, 4px);
  padding: clamp(2px, 8cqh, 5px) clamp(3px, 4cqw, 6px);
  border-radius: 3px;
  background: var(--preview-tile);
  box-shadow: 0 0 0 1px var(--el-border-color-lighter);
  overflow: hidden;
  color: var(--el-text-color-primary);
}
.pv-title,
.pv-kpi-label,
.pv-heading,
.pv-text-line,
.pv-table-row,
.pv-filter-label {
  display: block;
  flex-shrink: 0;
  border-radius: 2px;
  background: var(--preview-muted);
}
.pv-title {
  width: 38%;
  max-width: 70px;
  height: 3px;
}
.pv-plot {
  flex: 1;
  min-height: 0;
  display: flex;
  align-items: stretch;
  justify-content: center;
}
.pv-img {
  display: block;
  width: 100%;
  height: 100%;
  object-fit: contain;
  object-position: center;
  border-radius: 2px;
  background: #ffffff;
}
@container (max-height: 34px) {
  .pv-title {
    display: none;
  }
}
.pv-kpi {
  justify-content: center;
}
.pv-kpi-label {
  width: 40%;
  max-width: 54px;
  height: 3px;
}
.pv-kpi-value {
  display: block;
  flex-shrink: 0;
  width: 52%;
  max-width: 80px;
  height: clamp(4px, 22cqh, 11px);
  border-radius: 2px;
  background: currentColor;
  opacity: 0.7;
}
.pv-kpi-trend {
  display: block;
  width: 100%;
  flex: 1;
  min-height: 0;
  max-height: 40%;
}
.pv-text {
  justify-content: center;
}
.pv-heading {
  width: 46%;
  height: clamp(3px, 14cqh, 6px);
  background: currentColor;
  opacity: 0.55;
}
.pv-text-line {
  width: 88%;
  height: 2px;
  background: currentColor;
  opacity: 0.16;
}
.pv-short {
  width: 64%;
}
.pv-table {
  gap: 0;
}
.pv-table-head {
  display: block;
  flex-shrink: 0;
  height: clamp(4px, 10cqh, 7px);
  margin-bottom: 2px;
  border-radius: 2px;
  background: color-mix(in srgb, var(--color-accent) 22%, transparent);
}
.pv-table-row {
  height: 2px;
  margin: clamp(2px, 5cqh, 4px) 0;
  background: var(--preview-muted);
  opacity: 0.8;
}
.pv-filter {
  justify-content: center;
}
.pv-filter-box {
  display: flex;
  align-items: center;
  gap: 4px;
  height: clamp(6px, 60cqh, 14px);
  padding: 0 4px;
  border: 1px solid var(--preview-rule);
  border-radius: 3px;
}
.pv-filter-label {
  flex: 1;
  max-width: 50%;
  height: 3px;
}
.pv-filter-chevron {
  margin-left: auto;
  font-size: clamp(4px, 40cqh, 7px);
  color: var(--el-text-color-secondary);
}
.pv-image {
  align-items: center;
  justify-content: center;
  background: var(--preview-land);
}
.pv-image-glyph {
  font-size: clamp(8px, 35cqh, 20px);
  color: var(--el-text-color-secondary);
  opacity: 0.7;
}
.pv-separator {
  justify-content: center;
  align-items: center;
  padding: 0;
  background: transparent;
  box-shadow: none;
}
.pv-rule {
  display: block;
  width: 100%;
  height: 1px;
  background: var(--preview-rule);
}
.pv-separator.pv-vertical .pv-rule {
  width: 1px;
  height: 100%;
}
.dash-card-desc {
  margin: 0;
  font-size: 12px;
  color: var(--el-text-color-secondary);
  line-height: 1.4;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.dash-card-desc-empty {
  display: flex;
  align-items: center;
  justify-content: center;
  aspect-ratio: 16 / 10;
  border: 1px dashed var(--el-border-color);
  border-radius: 6px;
  color: var(--el-text-color-placeholder);
  font-style: italic;
}
.dash-card-footer {
  display: flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
  padding: 8px 12px;
  border-top: 1px solid var(--el-border-color-lighter);
  color: var(--el-text-color-secondary);
  font-size: 12px;
  white-space: nowrap;
}
.dash-footer-ns {
  overflow: hidden;
  text-overflow: ellipsis;
}
.dash-footer-updated {
  margin-left: auto;
  padding-left: 8px;
}
.dash-card-footer .dot {
  color: var(--el-text-color-disabled);
}
</style>
