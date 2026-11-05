<template>
  <el-tooltip :content="HUB_NAMESPACE_TITLE" placement="top" :show-after="300">
    <span class="resource-badge" data-testid="hub-badge">
      <i class="fa-solid fa-building"></i>
      Hub
    </span>
  </el-tooltip>
</template>

<script setup lang="ts">
import { HUB_NAMESPACE_TITLE } from "./hubRows";
</script>
