<template>
  <div class="empty-state">
    <i v-if="icon" :class="['empty-state-icon', icon]" />
    <h3 v-if="title" class="empty-state-title">{{ title }}</h3>
    <p v-if="description" class="empty-state-description">{{ description }}</p>
    <div v-if="$slots.actions" class="empty-state-actions">
      <slot name="actions" />
    </div>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  icon?: string;
  title?: string;
  description?: string;
}>();
</script>

<style scoped>
.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--spacing-3, 0.75rem);
  padding: var(--spacing-8, 2rem) var(--spacing-4, 1rem);
  color: var(--color-text-secondary, #718096);
  text-align: center;
}

.empty-state-icon {
  font-size: 48px;
  color: var(--color-primary, #4299e1);
  opacity: 0.5;
  margin-bottom: var(--spacing-2, 0.5rem);
}

.empty-state-title {
  margin: 0;
  font-size: var(--font-size-lg, 1rem);
  color: var(--color-text-primary, #2d3748);
  font-weight: var(--font-weight-semibold, 600);
}

.empty-state-description {
  margin: 0;
  max-width: 400px;
  font-size: var(--font-size-sm, 0.875rem);
  line-height: 1.5;
}

.empty-state-actions {
  margin-top: var(--spacing-3, 0.75rem);
  display: flex;
  gap: var(--spacing-2, 0.5rem);
}
</style>
