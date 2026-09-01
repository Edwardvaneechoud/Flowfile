<template>
  <div class="share-notice" :class="variant" role="status">
    <div class="share-notice__body">
      <span class="share-notice__icon" aria-hidden="true">
        <svg v-if="variant === 'warning'" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
        <svg v-else width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>
      </span>
      <div class="share-notice__text">
        <p class="share-notice__title">{{ title }}</p>
        <p v-if="detail" class="share-notice__detail">{{ detail }}</p>
        <ul v-if="placeholders.length" class="share-notice__list">
          <li v-for="p in placeholders" :key="p.nodeId">
            <button class="share-notice__node" @click="$emit('focus-node', p.nodeId)">
              {{ p.label }} (#{{ p.nodeId }})
            </button>
            <span class="share-notice__reason"> — {{ p.reason }}</span>
          </li>
        </ul>
        <p v-if="variant === 'warning'" class="share-notice__footer">
          A shared flow runs the transformations its sender built.
          <a href="https://flowfile.io/install/" target="_blank" rel="noopener noreferrer">Install the full version</a>
          to run everything.
        </p>
      </div>
    </div>
    <button class="share-notice__close" title="Dismiss" @click="$emit('close')">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
    </button>
  </div>
</template>

<script setup lang="ts">
import type { SharePlaceholderInfo } from '../composables/useShareLink'

withDefaults(
  defineProps<{
    variant: 'warning' | 'error'
    title: string
    detail?: string
    placeholders?: SharePlaceholderInfo[]
  }>(),
  { detail: '', placeholders: () => [] }
)

defineEmits<{
  (e: 'close'): void
  (e: 'focus-node', nodeId: number): void
}>()
</script>

<style scoped>
.share-notice {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 8px;
  margin: 8px 12px 0;
  padding: 10px 12px;
  border-radius: 6px;
  font-size: 13px;
}

.share-notice.warning {
  background-color: color-mix(in srgb, #f59e0b 12%, transparent);
  border: 1px solid color-mix(in srgb, #f59e0b 40%, transparent);
  color: var(--text-primary);
}

.share-notice.error {
  background-color: color-mix(in srgb, #ef4444 10%, transparent);
  border: 1px solid color-mix(in srgb, #ef4444 40%, transparent);
  color: var(--text-primary);
}

.share-notice__body {
  display: flex;
  gap: 8px;
  min-width: 0;
}

.share-notice__icon {
  display: flex;
  margin-top: 1px;
  flex-shrink: 0;
}

.share-notice.warning .share-notice__icon { color: #d97706; }
.share-notice.error .share-notice__icon { color: #dc2626; }

.share-notice__text { min-width: 0; }

.share-notice__title {
  margin: 0;
  font-weight: 600;
}

.share-notice__detail {
  margin: 4px 0 0;
  color: var(--text-secondary);
}

.share-notice__list {
  margin: 6px 0 0;
  padding-left: 18px;
}

.share-notice__list li { margin: 2px 0; }

.share-notice__node {
  background: none;
  border: none;
  padding: 0;
  font: inherit;
  font-weight: 600;
  color: var(--accent-color, #3b82f6);
  cursor: pointer;
  text-decoration: underline;
}

.share-notice__reason { color: var(--text-secondary); }

.share-notice__footer {
  margin: 6px 0 0;
  color: var(--text-secondary);
}

.share-notice__footer a { color: var(--accent-color, #3b82f6); }

.share-notice__close {
  background: none;
  border: none;
  padding: 2px;
  cursor: pointer;
  color: var(--text-secondary);
  flex-shrink: 0;
}
</style>
