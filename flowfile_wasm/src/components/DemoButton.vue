<template>
  <!-- Prominent floating button for first-time visitors -->
  <div v-if="prominent" class="demo-button-prominent" @click="handleClick">
    <div class="demo-button-content">
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
}>()

const { isLoading, loadDemo } = useDemo()

async function handleClick() {
  const success = await loadDemo(true)
  if (success) {
    emit('loaded')
  }
}
</script>

<style scoped>
/* Prominent floating button styles */
.demo-button-prominent {
  position: fixed;
  bottom: 24px;
  right: 24px;
  z-index: 1000;
  cursor: pointer;
}

.demo-button-content {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 14px 24px;
  background: linear-gradient(135deg, var(--color-accent) 0%, var(--color-accent-hover) 100%);
  border-radius: var(--border-radius-full);
  color: white;
  font-weight: var(--font-weight-semibold);
  font-size: var(--font-size-lg);
  box-shadow: 0 8px 32px rgba(8, 145, 178, 0.4), 0 4px 12px rgba(0, 0, 0, 0.15);
  transition: all var(--transition-normal);
  position: relative;
  z-index: 2;
}

.demo-button-prominent:hover .demo-button-content {
  transform: translateY(-2px);
  box-shadow: 0 12px 40px rgba(8, 145, 178, 0.5), 0 6px 16px rgba(0, 0, 0, 0.2);
}

.demo-button-prominent:active .demo-button-content {
  transform: translateY(0);
}

.demo-icon {
  font-size: 24px;
}

.demo-text {
  white-space: nowrap;
}

/* Tooltip that appears on hover */
.demo-tooltip {
  position: absolute;
  bottom: calc(100% + 12px);
  left: 50%;
  transform: translateX(-50%) translateY(10px);
  padding: 8px 16px;
  background: var(--color-gray-800);
  color: white;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  border-radius: var(--border-radius-md);
  white-space: nowrap;
  opacity: 0;
  pointer-events: none;
  transition: all var(--transition-normal);
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
  border: 3px solid var(--color-accent);
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
  background-color: var(--color-accent-subtle);
  border-color: var(--color-accent);
  color: var(--color-accent);
}

.demo-btn-subtle .btn-icon {
  color: var(--color-accent);
}

.demo-btn-subtle:hover:not(:disabled) {
  background-color: var(--color-accent);
  color: white;
}

.demo-btn-subtle:hover:not(:disabled) .btn-icon {
  color: white;
}

/* Dark theme adjustments */
[data-theme="dark"] .demo-tooltip {
  background: var(--color-gray-700);
}

[data-theme="dark"] .demo-tooltip::after {
  border-top-color: var(--color-gray-700);
}

[data-theme="dark"] .demo-button-content {
  box-shadow: 0 8px 32px rgba(8, 145, 178, 0.3), 0 4px 12px rgba(0, 0, 0, 0.3);
}

[data-theme="dark"] .demo-button-prominent:hover .demo-button-content {
  box-shadow: 0 12px 40px rgba(8, 145, 178, 0.4), 0 6px 16px rgba(0, 0, 0, 0.4);
}

/* Responsive adjustments */
@media (max-width: 768px) {
  .demo-button-prominent {
    bottom: 16px;
    right: 16px;
  }

  .demo-button-content {
    padding: 12px 20px;
    font-size: var(--font-size-md);
  }

  .demo-icon {
    font-size: 20px;
  }

  .demo-btn-subtle .btn-text {
    display: none;
  }
}
</style>
