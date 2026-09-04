<template>
  <div
    class="dash-card"
    role="button"
    tabindex="0"
    :title="`Open ${dashboard.name}`"
    @click="emit('view')"
    @keydown.enter="emit('view')"
  >
    <div class="dash-card-header">
      <i class="fa-solid fa-table-cells-large dash-card-icon"></i>
      <div class="dash-card-title">
        <span class="dash-name">{{ dashboard.name }}</span>
        <SharedBadge :access="dashboard.access" />
        <span class="dash-tile-count">{{ tileCount }} tile{{ tileCount === 1 ? "" : "s" }}</span>
      </div>
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
      <div v-if="preview.length" class="dash-preview" aria-hidden="true">
        <div
          v-for="rect in preview"
          :key="rect.id"
          class="dash-preview-tile"
          :class="[`dash-preview-${rect.type}`, `dash-preview-${rect.orientation}`]"
          :style="{
            left: `${rect.left}%`,
            top: `${rect.top}%`,
            width: `${rect.width}%`,
            height: `${rect.height}%`,
          }"
        >
          <img
            v-if="thumbnailFor(rect)"
            :src="thumbnailFor(rect)"
            class="dash-preview-img"
            alt=""
            loading="lazy"
          />
          <i
            v-else-if="rect.type === 'viz'"
            class="fa-solid fa-chart-column dash-preview-glyph"
          ></i>
        </div>
      </div>
      <p v-else-if="!dashboard.description" class="dash-card-desc dash-card-desc-empty">
        No tiles yet.
      </p>
      <p v-if="dashboard.description" class="dash-card-desc">{{ dashboard.description }}</p>
    </div>

    <div class="dash-card-footer">
      <span v-if="dashboard.namespace_name">{{ dashboard.namespace_name }}</span>
      <span v-if="dashboard.namespace_name" class="dot">·</span>
      <span>Updated {{ formatDate(dashboard.updated_at) }}</span>
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
import { buildLayoutPreview, type PreviewRect } from "./layoutPreview";

const props = defineProps<{
  dashboard: Dashboard;
  /** viz id → saved chart PNG data URL; tiles without one fall back to a tint. */
  thumbnails?: Record<number, string>;
}>();

const emit = defineEmits<{
  (e: "view"): void;
  (e: "edit"): void;
  (e: "delete"): void;
  (e: "share"): void;
}>();

const { canShare, canManage } = useResourceSharing();

const tileCount = computed(() => props.dashboard.layout.tiles.length);
const preview = computed(() => buildLayoutPreview(props.dashboard.layout));
const thumbnailFor = (rect: PreviewRect): string | undefined =>
  rect.type === "viz" && rect.vizId != null ? props.thumbnails?.[rect.vizId] : undefined;
</script>

<style scoped>
.dash-card {
  display: flex;
  flex-direction: column;
  border: 1px solid var(--el-border-color-light);
  border-radius: 8px;
  background: var(--el-bg-color);
  overflow: hidden;
  cursor: pointer;
  transition:
    background 0.15s,
    border-color 0.15s,
    box-shadow 0.15s;
  min-height: 160px;
}
.dash-card:hover,
.dash-card:focus-visible {
  background: var(--el-fill-color-lighter);
  border-color: var(--el-color-primary-light-5);
  box-shadow: 0 1px 6px rgba(0, 0, 0, 0.06);
  outline: none;
}
.dash-card-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 12px;
  border-bottom: 1px solid var(--el-border-color-lighter);
}
.dash-card-icon {
  color: var(--el-color-primary);
  font-size: 14px;
}
.dash-card-title {
  display: flex;
  align-items: baseline;
  gap: 8px;
  min-width: 0;
  flex: 1;
}
.dash-name {
  font-weight: 600;
  font-size: 14px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.dash-tile-count {
  font-size: 11px;
  color: var(--el-text-color-secondary);
  text-transform: uppercase;
}
.dash-card-menu {
  cursor: pointer;
  color: var(--el-text-color-secondary);
}
.dash-card-body {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 10px;
  padding: 14px 16px;
  background: var(--el-fill-color-blank);
}
.dash-preview {
  position: relative;
  width: 100%;
  aspect-ratio: 16 / 7;
  border-radius: 4px;
  background: var(--el-fill-color-light);
  overflow: hidden;
}
.dash-preview-tile {
  position: absolute;
  box-sizing: border-box;
  border-radius: 2px;
  border: 1px solid var(--el-bg-color);
  overflow: hidden;
  display: flex;
  align-items: center;
  justify-content: center;
  container-type: size;
}
.dash-preview-glyph {
  font-size: clamp(8px, 45cqh, 26px);
  color: color-mix(in srgb, var(--color-accent) 70%, transparent);
}
.dash-preview-img {
  display: block;
  width: 100%;
  height: 100%;
  object-fit: cover;
  object-position: top left;
  background: var(--el-bg-color);
}
.dash-preview-viz {
  background: color-mix(in srgb, var(--color-accent) 35%, transparent);
}
.dash-preview-text {
  background: color-mix(in srgb, var(--color-warning) 35%, transparent);
}
.dash-preview-separator {
  border: none;
  border-radius: 0;
  background: var(--color-text-muted);
}
.dash-preview-separator.dash-preview-horizontal {
  height: 2px !important;
  margin-top: 1px;
}
.dash-preview-separator.dash-preview-vertical {
  width: 2px !important;
  margin-left: 1px;
}
.dash-card:hover .dash-preview-viz,
.dash-card:focus-visible .dash-preview-viz {
  background: color-mix(in srgb, var(--color-accent) 55%, transparent);
}
.dash-card-desc {
  margin: 0;
  font-size: 12px;
  color: var(--el-text-color-secondary);
  text-align: center;
  max-width: 100%;
  line-height: 1.4;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.dash-card-desc-empty {
  color: var(--el-text-color-disabled);
  font-style: italic;
}
.dash-card-footer {
  display: flex;
  gap: 6px;
  padding: 8px 12px;
  border-top: 1px solid var(--el-border-color-lighter);
  color: var(--el-text-color-secondary);
  font-size: 12px;
}
.dash-card-footer .dot {
  color: var(--el-text-color-disabled);
}
</style>
