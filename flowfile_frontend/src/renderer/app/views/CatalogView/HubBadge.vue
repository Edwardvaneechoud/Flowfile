<template>
  <el-tooltip :content="title" placement="top" :show-after="300">
    <span class="resource-badge" data-testid="hub-badge">
      <i class="fa-solid fa-building"></i>
      Hub
    </span>
  </el-tooltip>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { HUB_NAMESPACE_TITLE, HUB_TABLE_TITLE } from "./hubRows";

const props = defineProps<{ kind: "namespace" | "table" }>();

const title = computed(() => (props.kind === "table" ? HUB_TABLE_TITLE : HUB_NAMESPACE_TITLE));
</script>
