<template>
  <div class="popped-out" data-testid="notebook-popout-stub">
    <span class="material-icons popped-out__icon" aria-hidden="true">open_in_new</span>
    <p class="popped-out__text">The notebook is open in its own window.</p>
    <div class="popped-out__actions">
      <button type="button" class="popped-out__btn" @click="focus(flowId)">Show window</button>
      <button
        type="button"
        class="popped-out__btn popped-out__btn--primary"
        data-testid="nb-bring-back"
        @click="bringBack(flowId)"
      >
        Bring back
      </button>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { usePopout } from "../../../composables/usePopout";

defineProps<{ flowId: number }>();

const { focus, bringBack } = usePopout("notebook");
</script>

<style scoped>
.popped-out {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-sm);
  padding: var(--spacing-lg);
  color: var(--color-text-secondary);
  text-align: center;
}

.popped-out__icon {
  font-size: 28px;
  color: var(--color-text-tertiary);
}

.popped-out__text {
  margin: 0;
  font-size: var(--font-size-sm);
}

.popped-out__actions {
  display: flex;
  gap: var(--spacing-sm);
}

.popped-out__btn {
  padding: 6px 12px;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  cursor: pointer;
}

.popped-out__btn:hover {
  background: var(--color-background-tertiary);
}

.popped-out__btn--primary {
  border-color: var(--color-primary);
  background: var(--color-primary);
  color: #fff;
}

.popped-out__btn--primary:hover {
  filter: brightness(0.95);
}
</style>
