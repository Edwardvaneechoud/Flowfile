<template>
  <el-dropdown trigger="click">
    <button
      type="button"
      class="tile-actions-trigger"
      aria-label="Tile actions"
      title="More"
      data-testid="tile-actions"
      @mousedown.stop
      @click.stop
    >
      <el-icon><MoreFilled /></el-icon>
    </button>
    <template #dropdown>
      <el-dropdown-menu>
        <TileActionItems :actions="actions" @select="emit('select', $event)" />
      </el-dropdown-menu>
    </template>
  </el-dropdown>
</template>

<script setup lang="ts">
import { MoreFilled } from "@element-plus/icons-vue";
import TileActionItems from "./TileActionItems.vue";
import type { TileAction } from "./tileActions";

/** The viewer's "⋯" on a tile: a real button, so it takes focus and opens from the keyboard. */
defineProps<{ actions: TileAction[] }>();

const emit = defineEmits<{
  (e: "select", action: TileAction): void;
}>();
</script>

<style scoped>
.tile-actions-trigger {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 16px;
  padding: 0;
  line-height: 1;
  border: 1px solid transparent;
  border-radius: 4px;
  background: transparent;
  color: var(--el-text-color-secondary);
  cursor: pointer;
}
.tile-actions-trigger:hover,
.tile-actions-trigger:focus-visible {
  color: var(--el-color-primary);
  border-color: var(--el-color-primary-light-5);
  outline: none;
}
</style>
