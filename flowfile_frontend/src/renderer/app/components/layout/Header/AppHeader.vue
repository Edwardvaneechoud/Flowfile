<template>
  <div class="app-header">
    <div class="logo"></div>
    <div class="app-header__spacer"></div>
    <ProjectStatusIndicator />
  </div>
</template>

<script setup lang="ts">
import ProjectStatusIndicator from "../../project/ProjectStatusIndicator.vue";
</script>

<style scoped>
.app-header {
  display: flex;
  align-items: center;
  width: 100%;
  padding-right: 12px;
}

.app-header__spacer {
  flex: 1;
}
</style>
