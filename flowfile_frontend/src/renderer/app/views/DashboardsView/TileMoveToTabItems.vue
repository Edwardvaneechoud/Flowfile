<template>
  <el-dropdown-item
    v-for="(tab, i) in tabs"
    :key="tab.id"
    :divided="i === 0"
    data-testid="tile-move-to-tab"
    @click="emit('select', tab.id)"
  >
    <el-icon><Right /></el-icon> Move to {{ tab.title }}
  </el-dropdown-item>
</template>

<script setup lang="ts">
import { Right } from "@element-plus/icons-vue";
import type { DashboardTab } from "../../types";

/** The editor menu's "Move to <tab>" entries, one per other tab of the dashboard. */
defineProps<{ tabs: DashboardTab[] }>();

const emit = defineEmits<{
  (e: "select", tabId: string): void;
}>();
</script>
