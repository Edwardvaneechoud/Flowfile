<script setup lang="ts">
import { watch, onBeforeUnmount } from 'vue'

const props = defineProps<{ open: boolean; title?: string }>()
const emit = defineEmits<{ close: [] }>()

function onKeyDown(e: KeyboardEvent) {
  if (e.key === 'Escape') emit('close')
}

watch(
  () => props.open,
  (open) => {
    if (open) window.addEventListener('keydown', onKeyDown)
    else window.removeEventListener('keydown', onKeyDown)
  },
)

onBeforeUnmount(() => window.removeEventListener('keydown', onKeyDown))
</script>

<template>
  <Teleport to="body">
    <Transition name="ov">
      <div v-if="open" class="ov-overlay">
        <div class="ov-header">
          <span class="ov-title">{{ title }}</span>
          <button class="ov-close" title="Close (Esc)" @click="emit('close')">
            <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" /></svg>
            <span>Close</span>
          </button>
        </div>
        <div class="ov-body">
          <slot />
        </div>
      </div>
    </Transition>
  </Teleport>
</template>

<style scoped>
.ov-overlay {
  position: fixed;
  inset: 0;
  z-index: 9990;
  display: flex;
  flex-direction: column;
  background: var(--color-background-primary);
  color: var(--color-text-primary);
}
.ov-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0.75rem 1rem;
  border-bottom: 1px solid var(--color-border-primary);
  background: var(--color-background-primary);
}
.ov-title {
  font-size: 14px;
  font-weight: 600;
  color: var(--color-text-primary);
}
.ov-close {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px 12px;
  border: 1px solid var(--color-border-light);
  border-radius: 6px;
  background: var(--color-background-secondary);
  color: var(--color-text-primary);
  font-size: 13px;
  cursor: pointer;
}
.ov-close:hover {
  background: var(--color-background-hover);
}
.ov-body {
  flex: 1;
  min-height: 0;
  overflow: hidden;
  padding: 16px;
  background: var(--color-background-primary);
}
.ov-enter-active,
.ov-leave-active {
  transition: opacity 0.2s ease;
}
.ov-enter-from,
.ov-leave-to {
  opacity: 0;
}
</style>
