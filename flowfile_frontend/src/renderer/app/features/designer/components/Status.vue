<template>
  <div class="status">
    <Status />
  </div>
</template>

<script setup lang="ts">
import Status from "../editor/status.vue";
</script>

<style scoped>
.status {
  display: flex;
  align-items: center;
  gap: 10px;
  padding-right: 20px;
  width: 100px;
}
</style>
