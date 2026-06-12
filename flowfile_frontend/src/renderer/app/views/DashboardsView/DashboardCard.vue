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
      <p v-if="dashboard.description" class="dash-card-desc">{{ dashboard.description }}</p>
      <p v-else class="dash-card-desc dash-card-desc-empty">
        {{ tileCount > 0 ? "Click to view this dashboard." : "No tiles yet." }}
      </p>
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

const props = defineProps<{ dashboard: Dashboard }>();

const emit = defineEmits<{
  (e: "view"): void;
  (e: "edit"): void;
  (e: "delete"): void;
  (e: "share"): void;
}>();

const { canShare, canManage } = useResourceSharing();

const tileCount = computed(() => props.dashboard.layout.tiles.length);
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
  align-items: center;
  justify-content: center;
  padding: 20px 16px;
  background: var(--el-fill-color-blank);
}
.dash-card-desc {
  margin: 0;
  font-size: 12px;
  color: var(--el-text-color-secondary);
  text-align: center;
  max-width: 32ch;
  line-height: 1.4;
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
