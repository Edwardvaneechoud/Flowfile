<template>
  <div class="popout-window" :data-testid="testId">
    <header class="popout-window__bar">
      <span class="popout-window__title" :title="title">{{ title }}</span>
      <button
        type="button"
        class="popout-window__return"
        :data-testid="`${testId}-return`"
        @click="emit('return')"
      >
        <i class="fa-solid fa-arrow-right-to-bracket" aria-hidden="true"></i>
        Return to designer
      </button>
    </header>
    <div v-if="state === 'ready'" class="popout-window__body">
      <slot></slot>
    </div>
    <div v-else-if="state === 'missing'" class="popout-window__empty">
      <p>This flow is not open in Flowfile.</p>
      <button type="button" class="popout-window__return" @click="emit('close')">
        Close window
      </button>
    </div>
    <div v-else class="popout-window__empty"><p>Loading the flow…</p></div>
  </div>
</template>

<script lang="ts" setup>
// The chrome every pop-out window shares: a title bar with "Return to designer" over the panel.
import type { PopoutWindowState } from "./usePopoutWindowHost";

defineProps<{ title: string; state: PopoutWindowState; testId: string }>();
const emit = defineEmits<{ return: []; close: [] }>();
</script>

<style scoped>
.popout-window {
  display: flex;
  flex-direction: column;
  height: 100vh;
  background: var(--color-background-primary);
  color: var(--color-text-primary);
}

.popout-window__bar {
  display: flex;
  flex: none;
  align-items: center;
  gap: var(--spacing-sm);
  height: 40px;
  padding: 0 var(--spacing-md);
  border-bottom: 1px solid var(--color-border-light);
  background: var(--color-background-secondary);
}

.popout-window__title {
  flex: 1;
  min-width: 0;
  overflow: hidden;
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.popout-window__return {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 10px;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  cursor: pointer;
}

.popout-window__return:hover {
  background: var(--color-background-tertiary);
}

.popout-window__body {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
}

.popout-window__body > :slotted(*) {
  flex: 1;
  min-height: 0;
}

.popout-window__empty {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-sm);
  color: var(--color-text-secondary);
}
</style>
