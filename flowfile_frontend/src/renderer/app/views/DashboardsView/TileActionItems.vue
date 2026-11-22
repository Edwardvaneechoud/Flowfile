<template>
  <el-dropdown-item
    v-for="(action, i) in actions"
    :key="action"
    :divided="divided && i === 0"
    :data-testid="`tile-action-${action}`"
    @click="emit('select', action)"
  >
    <el-icon><component :is="ICONS[action]" /></el-icon> {{ TILE_ACTION_LABELS[action] }}
  </el-dropdown-item>
</template>

<script setup lang="ts">
import { Download, FullScreen, Picture, Tickets } from "@element-plus/icons-vue";
import { TILE_ACTION_LABELS, type TileAction } from "./tileActions";

/** A tile's viewer entries as dropdown items, for the viewer's menu and the end of the editor's. */
defineProps<{
  actions: TileAction[];
  /** A line above the first entry, where they follow the editor's own. */
  divided?: boolean;
}>();

const emit = defineEmits<{
  (e: "select", action: TileAction): void;
}>();

const ICONS = {
  expand: FullScreen,
  "see-rows": Tickets,
  "download-csv": Download,
  "download-image": Picture,
} as const;
</script>
