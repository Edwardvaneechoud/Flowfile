<template>
  <div v-if="show" class="modal-overlay" @click="emit('close')">
    <div class="modal-container help-modal" @click.stop>
      <div class="modal-header">
        <h3 class="modal-title">
          <i :class="icon"></i>
          {{ title }}
        </h3>
        <button class="modal-close" @click="emit('close')">
          <i class="fa-solid fa-times"></i>
        </button>
      </div>
      <div class="modal-content">
        <div v-for="(section, idx) in sections" :key="idx" class="help-section">
          <h4 class="section-title">
            <i v-if="section.icon" :class="section.icon"></i>
            {{ section.title }}
          </h4>
          <p v-if="section.description" class="section-description">
            {{ section.description }}
          </p>

          <!-- Feature cards -->
          <div v-if="section.features" class="feature-grid">
            <div v-for="(feature, fIdx) in section.features" :key="fIdx" class="feature-card">
              <div class="feature-icon">
                <i :class="feature.icon"></i>
              </div>
              <h5>{{ feature.title }}</h5>
              <p>{{ feature.description }}</p>
            </div>
          </div>

          <!-- Tip cards -->
          <div v-if="section.tips" class="tips-list">
            <div v-for="(tip, tIdx) in section.tips" :key="tIdx" class="tip-card">
              <div :class="['tip-icon', tip.type]">
                <i
                  :class="tip.type === 'success' ? 'fa-solid fa-check' : 'fa-solid fa-exclamation'"
                ></i>
              </div>
              <div class="tip-content">
                <h5>{{ tip.title }}</h5>
                <p>{{ tip.description }}</p>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div class="modal-actions">
        <button class="btn btn-primary" @click="emit('close')">Got it</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { HelpSection } from "./types";

defineProps<{
  show: boolean;
  title: string;
  icon: string;
  sections: HelpSection[];
}>();

const emit = defineEmits<{
  (e: "close"): void;
}>();
</script>

<style scoped>
.help-modal {
  max-width: 600px;
}

/* Sections */
.help-section {
  margin-bottom: 1.5rem;
}

.help-section:last-child {
  margin-bottom: 0;
}

.section-title {
  margin: 0 0 0.5rem 0;
  font-size: 0.9375rem;
  font-weight: 600;
  color: var(--color-text-primary, #1a1a2e);
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.section-title i {
  color: var(--color-accent, #0891b2);
  font-size: 0.875rem;
}

.section-description {
  margin: 0 0 0.75rem 0;
  color: var(--color-text-secondary, #4a5568);
  line-height: 1.6;
  font-size: 0.875rem;
}

/* Feature grid */
.feature-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 0.75rem;
}

.feature-card {
  padding: 0.75rem;
  background: var(--color-background-secondary, #f8f9fa);
  border: 1px solid var(--color-border-primary, #e0e0e0);
  border-radius: 8px;
}

.feature-icon {
  width: 2.25rem;
  height: 2.25rem;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--color-accent-subtle, #ecfeff);
  border-radius: 50%;
  color: var(--color-accent, #0891b2);
  font-size: 0.875rem;
  margin-bottom: 0.5rem;
}

.feature-card h5 {
  margin: 0 0 0.25rem 0;
  font-size: 0.8125rem;
  font-weight: 600;
  color: var(--color-text-primary, #1a1a2e);
}

.feature-card p {
  margin: 0;
  font-size: 0.75rem;
  color: var(--color-text-secondary, #4a5568);
  line-height: 1.5;
}

/* Tips */
.tips-list {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.tip-card {
  display: flex;
  gap: 0.75rem;
  padding: 0.625rem 0.75rem;
  background: var(--color-background-secondary, #f8f9fa);
  border-radius: 6px;
}

.tip-icon {
  width: 1.5rem;
  height: 1.5rem;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 50%;
  flex-shrink: 0;
  font-size: 0.625rem;
}

.tip-icon.success {
  background: var(--color-success-light, #d1fae5);
  color: var(--color-success, #10b981);
}

.tip-icon.warning {
  background: var(--color-warning-light, #fef3c7);
  color: var(--color-warning, #f59e0b);
}

.tip-content h5 {
  margin: 0 0 0.125rem 0;
  font-size: 0.8125rem;
  font-weight: 600;
  color: var(--color-text-primary, #1a1a2e);
}

.tip-content p {
  margin: 0;
  font-size: 0.75rem;
  color: var(--color-text-secondary, #4a5568);
  line-height: 1.5;
}
</style>
