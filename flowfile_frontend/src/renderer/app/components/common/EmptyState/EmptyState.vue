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
  gap: var(--spacing-2);
  padding: var(--spacing-6) var(--spacing-4);
  color: var(--color-text-tertiary);
  text-align: center;
}

.empty-state-icon {
  font-size: var(--font-size-4xl);
  color: var(--color-text-muted);
  opacity: 0.6;
  margin-bottom: var(--spacing-1);
}

.empty-state-title {
  margin: 0;
  font-size: var(--font-size-base);
  color: var(--color-text-secondary);
  font-weight: var(--font-weight-medium);
}

.empty-state-description {
  margin: 0;
  max-width: 320px;
  font-size: var(--font-size-sm);
  color: var(--color-text-muted);
  line-height: var(--line-height-normal);
}

.empty-state-actions {
  margin-top: var(--spacing-3);
  display: flex;
  gap: var(--spacing-2);
}

/* Global stylesheets (_cards.css/_modals.css) style bare `.empty-state i` for
   legacy hero icons; neutralize that leak inside the actions slot so buttons
   keep their own icon sizing. */
.empty-state-actions :deep(i) {
  font-size: inherit;
  margin: 0;
  opacity: 1;
}
</style>
