<template>
  <div
    class="viz-card"
    role="button"
    tabindex="0"
    :title="`Open ${viz.name}`"
    @click="emit('edit')"
    @keydown.enter="emit('edit')"
  >
    <div class="viz-card-header">
      <i :class="iconClass" class="viz-card-icon"></i>
      <div class="viz-card-title">
        <span class="viz-name">{{ viz.name }}</span>
        <span v-if="viz.chart_type" class="viz-chart-type">{{ viz.chart_type }}</span>
      </div>
      <el-dropdown trigger="click" @click.stop>
        <el-icon class="viz-card-menu" @click.stop><MoreFilled /></el-icon>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item @click.stop="emit('edit')">
              <el-icon><Edit /></el-icon>
              Open
            </el-dropdown-item>
            <el-dropdown-item divided @click.stop="emit('delete')">
              <el-icon><Delete /></el-icon>
              Delete
            </el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
    </div>

    <div class="viz-card-body" :class="{ 'viz-card-body-image': !!viz.thumbnail_data_url }">
      <img
        v-if="viz.thumbnail_data_url"
        :src="viz.thumbnail_data_url"
        :alt="`Thumbnail of ${viz.name}`"
        class="viz-card-thumb-img"
        loading="lazy"
        decoding="async"
      />
      <template v-else>
        <i :class="iconClass" class="viz-card-thumb-icon"></i>
        <p v-if="viz.description" class="viz-card-desc">{{ viz.description }}</p>
        <p v-else class="viz-card-desc viz-card-desc-empty">
          Click to open and view this visualization.
        </p>
      </template>
    </div>

    <div class="viz-card-footer">
      <span v-if="viz.chart_type">{{ viz.chart_type }}</span>
      <span v-if="viz.chart_type" class="dot">·</span>
      <span>Updated {{ formatDate(viz.updated_at) }}</span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { Delete, Edit, MoreFilled } from "@element-plus/icons-vue";
import type { CatalogVisualization } from "../../types";
import { formatDate } from "./catalog-formatters";

const props = defineProps<{
  viz: CatalogVisualization;
}>();

const emit = defineEmits<{ (e: "edit"): void; (e: "delete"): void }>();

const iconClass = computed(() =>
  props.viz.source_type === "sql"
    ? "fa-solid fa-code viz-source-sql"
    : "fa-solid fa-chart-column viz-source-table",
);
</script>

<style scoped>
.viz-card {
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
}
.viz-card:hover,
.viz-card:focus-visible {
  background: var(--el-fill-color-lighter);
  border-color: var(--el-color-primary-light-5);
  box-shadow: 0 1px 6px rgba(0, 0, 0, 0.06);
  outline: none;
}
.viz-card-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 12px;
  border-bottom: 1px solid var(--el-border-color-lighter);
}
.viz-card-icon {
  color: var(--el-color-primary);
  font-size: 14px;
  flex-shrink: 0;
}
.viz-card-title {
  display: flex;
  align-items: baseline;
  gap: 8px;
  min-width: 0;
  flex: 1;
}
.viz-name {
  font-weight: 600;
  font-size: 14px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.viz-chart-type {
  font-size: 11px;
  color: var(--el-text-color-secondary);
  text-transform: uppercase;
}
.viz-card-menu {
  cursor: pointer;
  color: var(--el-text-color-secondary);
}
.viz-card-body {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 12px;
  padding: 24px 16px;
  min-height: 140px;
  background: var(--el-fill-color-blank);
}
.viz-card-body-image {
  padding: 0;
}
.viz-card-thumb-img {
  display: block;
  width: 100%;
  height: 180px;
  object-fit: contain;
  background: var(--el-fill-color-blank);
}
.viz-card-thumb-icon {
  font-size: 36px;
  color: var(--el-color-primary-light-3);
}
.viz-card-desc {
  margin: 0;
  font-size: 12px;
  color: var(--el-text-color-secondary);
  text-align: center;
  max-width: 32ch;
  line-height: 1.4;
}
.viz-card-desc-empty {
  color: var(--el-text-color-disabled);
  font-style: italic;
}
.viz-card-footer {
  display: flex;
  gap: 6px;
  padding: 8px 12px;
  border-top: 1px solid var(--el-border-color-lighter);
  color: var(--el-text-color-secondary);
  font-size: 12px;
}
.viz-card-footer .dot {
  color: var(--el-text-color-disabled);
}
.viz-source-sql {
  color: var(--el-color-warning);
}
</style>
