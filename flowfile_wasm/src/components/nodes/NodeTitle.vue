<template>
  <div class="node-title-wrapper">
    <div class="node-title-header">
      <h3 class="node-title">{{ title }}</h3>
    </div>
    <p v-if="intro" class="node-intro">{{ intro }}</p>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  title: string
  intro?: string
}>()
</script>

<style scoped>
.node-title-wrapper {
  padding: 12px 16px;
  border-bottom: 1px solid var(--border-color);
  background: var(--bg-tertiary);
}

.node-title-header {
  display: flex;
  align-items: center;
  gap: 8px;
}

.node-title {
  margin: 0;
  font-size: 14px;
  font-weight: 600;
  color: var(--text-primary);
}

.node-intro {
  margin: 8px 0 0 0;
  font-size: 12px;
  color: var(--text-secondary);
  line-height: 1.4;
}
</style>
