<template>
  <div class="template-card">
    <div class="template-card__header">
      <span class="material-icons template-card__icon">{{ template.icon }}</span>
      <span class="template-card__badge" :class="badgeClass">{{ template.category }}</span>
    </div>
    <h3 class="template-card__title">{{ template.name }}</h3>
    <p class="template-card__description">{{ template.description }}</p>
    <div class="template-card__footer">
      <div class="template-card__meta">
        <span class="material-icons template-card__meta-icon">account_tree</span>
        <span>{{ template.node_count }} nodes</span>
      </div>
      <div class="template-card__tags">
        <span v-for="tag in template.tags" :key="tag" class="template-card__tag">{{ tag }}</span>
      </div>
    </div>
    <el-button
      type="primary"
      size="small"
      class="template-card__button"
      :loading="loading"
      @click="$emit('use-template', template.template_id)"
    >
      <span class="material-icons" style="font-size: 16px; margin-right: 4px">play_arrow</span>
      Use Template
    </el-button>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { FlowTemplateMeta } from "../../types/template.types";

const props = defineProps<{
  template: FlowTemplateMeta;
  loading?: boolean;
}>();

defineEmits<{
  (e: "use-template", templateId: string): void;
}>();

const badgeClass = computed(() => {
  const category = props.template.category.toLowerCase();
  return `template-card__badge--${category}`;
});
</script>

<style scoped>
.template-card {
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  padding: 20px;
  cursor: pointer;
  transition: all var(--transition-base) var(--transition-timing);
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.template-card:hover {
  border-color: var(--color-primary);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
  transform: translateY(-2px);
}

.template-card__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.template-card__icon {
  font-size: 32px;
  color: var(--color-primary);
}

.template-card__badge {
  font-size: 11px;
  font-weight: 600;
  padding: 2px 8px;
  border-radius: 10px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.template-card__badge--beginner {
  background: var(--color-success-light);
  color: var(--color-success);
}

.template-card__badge--intermediate {
  background: var(--color-warning-light);
  color: var(--color-warning);
}

.template-card__badge--advanced {
  background: var(--color-danger-light);
  color: var(--color-danger);
}

/* Dark mode: use transparent backgrounds so they work on dark surfaces */
[data-theme="dark"] .template-card__badge--beginner {
  background: rgba(16, 185, 129, 0.2);
  color: var(--color-success);
}

[data-theme="dark"] .template-card__badge--intermediate {
  background: rgba(245, 158, 11, 0.2);
  color: var(--color-warning);
}

[data-theme="dark"] .template-card__badge--advanced {
  background: rgba(239, 68, 68, 0.2);
  color: var(--color-danger);
}

.template-card__title {
  font-size: 16px;
  font-weight: 600;
  color: var(--color-text-primary);
  margin: 0;
}

.template-card__description {
  font-size: 13px;
  color: var(--color-text-secondary);
  line-height: 1.5;
  margin: 0;
  flex: 1;
}

.template-card__footer {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.template-card__meta {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 12px;
  color: var(--color-text-secondary);
}

.template-card__meta-icon {
  font-size: 14px;
}

.template-card__tags {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

.template-card__tag {
  font-size: 11px;
  padding: 1px 6px;
  border-radius: 4px;
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  border: 1px solid var(--color-border-primary);
}

.template-card__button {
  margin-top: 4px;
  width: 100%;
}
</style>
