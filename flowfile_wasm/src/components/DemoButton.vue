<template>
  <!-- Prominent floating button for first-time visitors -->
  <div v-if="prominent" class="demo-button-prominent">
    <button
      class="demo-dismiss-btn"
      @click.stop="handleDismiss"
      title="Dismiss"
      aria-label="Dismiss demo button"
    >
      <span class="material-icons">close</span>
    </button>
    <div class="demo-button-content" @click="handleClick">
      <span class="material-icons demo-icon">play_circle</span>
      <span class="demo-text">Try Demo</span>
    </div>
    <div class="demo-tooltip">See it in action!</div>
    <div class="pulse-ring"></div>
    <div class="pulse-ring delay"></div>
  </div>

  <!-- Subtle toolbar button for returning visitors -->
  <button
    v-else
    class="action-btn demo-btn-subtle"
    @click="handleClick"
    :disabled="isLoading"
    title="Load demo flow"
  >
    <span class="material-icons btn-icon">play_circle</span>
    <span class="btn-text">{{ isLoading ? 'Loading...' : 'Demo' }}</span>
  </button>
</template>

<script setup lang="ts">
import { useDemo } from '../composables/useDemo'

defineProps<{
  prominent?: boolean
}>()

const emit = defineEmits<{
  (e: 'loaded'): void
  (e: 'dismissed'): void
}>()

const { isLoading, loadDemo, dismissDemo } = useDemo()

async function handleClick() {
  const success = await loadDemo(true)
  if (success) {
    emit('loaded')
  }
}

function handleDismiss() {
  dismissDemo()
  emit('dismissed')
}
</script>

<style scoped>
/* Prominent floating button styles */
.demo-button-prominent {
  position: fixed;
  bottom: var(--spacing-6);
  right: var(--spacing-6);
  z-index: var(--z-index-dropdown);
}

.demo-button-content {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-3) var(--spacing-6);
  background: linear-gradient(135deg, var(--color-button-primary) 0%, var(--color-button-primary-hover) 100%);
  border-radius: var(--border-radius-full);
  color: var(--color-text-inverse);
  font-weight: var(--font-weight-semibold);
  font-size: var(--font-size-lg);
  box-shadow: var(--shadow-lg);
  transition: all var(--transition-normal) var(--transition-timing);
  position: relative;
  z-index: 2;
  cursor: pointer;
}

.demo-button-content:hover {
  transform: translateY(-2px);
  box-shadow: var(--shadow-xl);
}

.demo-button-content:active {
  transform: translateY(0);
}

.demo-icon {
  font-size: var(--font-size-4xl);
}

.demo-text {
  white-space: nowrap;
}

/* Dismiss button */
.demo-dismiss-btn {
  position: absolute;
  top: -8px;
  right: -8px;
  width: 24px;
  height: 24px;
  padding: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-full);
  color: var(--color-text-secondary);
  cursor: pointer;
  z-index: 10;
  transition: all var(--transition-fast) var(--transition-timing);
  box-shadow: var(--shadow-sm);
}

.demo-dismiss-btn:hover {
  background: var(--color-danger-light);
  border-color: var(--color-danger);
  color: var(--color-danger);
}

.demo-dismiss-btn .material-icons {
  font-size: 16px;
}

/* Tooltip that appears on hover */
.demo-tooltip {
  position: absolute;
  bottom: calc(100% + var(--spacing-3));
  left: 50%;
  transform: translateX(-50%) translateY(10px);
  padding: var(--spacing-2) var(--spacing-4);
  background: var(--color-gray-800);
  color: var(--color-text-inverse);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  border-radius: var(--border-radius-md);
  white-space: nowrap;
  opacity: 0;
  pointer-events: none;
  transition: all var(--transition-normal) var(--transition-timing);
  box-shadow: var(--shadow-lg);
}

.demo-tooltip::after {
  content: '';
  position: absolute;
  top: 100%;
  left: 50%;
  transform: translateX(-50%);
  border: 6px solid transparent;
  border-top-color: var(--color-gray-800);
}

.demo-button-prominent:hover .demo-tooltip {
  opacity: 1;
  transform: translateX(-50%) translateY(0);
}

/* Pulsing ring animation */
.pulse-ring {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  width: 100%;
  height: 100%;
  border-radius: var(--border-radius-full);
  background: transparent;
  border: 3px solid var(--color-button-primary);
  animation: pulse 2s ease-out infinite;
  pointer-events: none;
  z-index: 1;
}

.pulse-ring.delay {
  animation-delay: 0.5s;
}

@keyframes pulse {
  0% {
    transform: translate(-50%, -50%) scale(1);
    opacity: 0.8;
  }
  100% {
    transform: translate(-50%, -50%) scale(1.5);
    opacity: 0;
  }
}

/* Subtle toolbar button styles */
.demo-btn-subtle {
  background-color: var(--color-background-tertiary);
  border-color: var(--color-border-primary);
  color: var(--color-text-primary);
}

.demo-btn-subtle .btn-icon {
  color: var(--color-text-secondary);
}

.demo-btn-subtle:hover:not(:disabled) {
  background-color: var(--color-background-hover);
  border-color: var(--color-border-secondary);
}

.demo-btn-subtle:hover:not(:disabled) .btn-icon {
  color: var(--color-text-primary);
}

/* Dark theme adjustments */
[data-theme="dark"] .demo-tooltip {
  background: var(--color-gray-700);
}

[data-theme="dark"] .demo-tooltip::after {
  border-top-color: var(--color-gray-700);
}

/* Responsive adjustments */
@media (max-width: 768px) {
  .demo-button-prominent {
    bottom: var(--spacing-4);
    right: var(--spacing-4);
  }

  .demo-button-content {
    padding: var(--spacing-3) var(--spacing-5);
    font-size: var(--font-size-md);
  }

  .demo-icon {
    font-size: var(--font-size-3xl);
  }

  .demo-btn-subtle .btn-text {
    display: none;
  }
}
</style>
