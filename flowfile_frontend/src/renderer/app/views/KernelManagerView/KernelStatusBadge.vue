<template>
  <span class="kernel-status-badge" :class="`status-${state}`">
    <i :class="iconClass"></i>
    {{ label }}
  </span>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { KernelState } from "../../types";

const props = defineProps<{
  state: KernelState;
}>();

const config = computed(() => {
  const map: Record<KernelState, { icon: string; label: string }> = {
    stopped: { icon: "fa-solid fa-circle-stop", label: "Stopped" },
    starting: { icon: "fa-solid fa-spinner fa-spin", label: "Starting" },
    idle: { icon: "fa-solid fa-circle-check", label: "Ready" },
    executing: { icon: "fa-solid fa-gear fa-spin", label: "Executing" },
    error: { icon: "fa-solid fa-circle-exclamation", label: "Error" },
  };
  return map[props.state] ?? { icon: "fa-solid fa-question", label: props.state };
});

const iconClass = computed(() => config.value.icon);
const label = computed(() => config.value.label);
</script>

<style scoped>
.kernel-status-badge {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-0-5) var(--spacing-2);
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  line-height: var(--line-height-normal);
  white-space: nowrap;
}

.status-stopped {
  background-color: var(--color-background-tertiary);
  color: var(--color-text-secondary);
}

.status-starting {
  background-color: var(--color-warning-light);
  color: var(--color-warning-dark);
}

.status-idle {
  background-color: var(--color-success-light);
  color: var(--color-success-hover);
}

.status-executing {
  background-color: var(--color-info-light);
  color: var(--color-info-hover);
}

.status-error {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
}
</style>
