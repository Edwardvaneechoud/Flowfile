<template>
  <div class="kernel-card" :class="{ 'kernel-card--error': kernel.state === 'error' }">
    <div class="kernel-card__header">
      <div class="kernel-card__title-row">
        <h4 class="kernel-card__name">{{ kernel.name }}</h4>
        <KernelStatusBadge :state="kernel.state" />
      </div>
      <div class="kernel-card__meta">
        <p class="kernel-card__id">{{ kernel.id }}</p>
        <span v-if="kernel.kernel_version" class="kernel-card__version" title="Kernel runtime version">
          v{{ kernel.kernel_version }}
        </span>
      </div>
    </div>

    <div class="kernel-card__body">
      <div class="kernel-card__resources">
        <span class="kernel-card__resource" title="CPU cores">
          <i class="fa-solid fa-microchip"></i> {{ kernel.cpu_cores }} CPU
        </span>
        <span class="kernel-card__resource" title="Memory">
          <i class="fa-solid fa-memory"></i> {{ kernel.memory_gb }} GB
        </span>
        <span v-if="kernel.gpu" class="kernel-card__resource" title="GPU enabled">
          <i class="fa-solid fa-display"></i> GPU
        </span>
      </div>

      <div v-if="kernel.packages.length > 0" class="kernel-card__packages">
        <span v-for="pkg in displayedPackages" :key="pkg" class="kernel-card__package-tag">
          {{ pkg }}
        </span>
        <span
          v-if="kernel.packages.length > maxPackagesShown"
          class="kernel-card__package-tag kernel-card__package-tag--more"
          :title="kernel.packages.slice(maxPackagesShown).join(', ')"
        >
          +{{ kernel.packages.length - maxPackagesShown }} more
        </span>
      </div>
      <div v-else class="kernel-card__no-packages">No extra packages</div>

      <div v-if="kernel.state === 'error' && kernel.error_message" class="kernel-card__error">
        <i class="fa-solid fa-triangle-exclamation"></i>
        <span>{{ kernel.error_message }}</span>
      </div>
    </div>

    <div class="kernel-card__actions">
      <button
        v-if="kernel.state === 'stopped' || kernel.state === 'error'"
        class="btn btn-primary btn-sm"
        :disabled="busy"
        @click="$emit('start', kernel.id)"
      >
        <i class="fa-solid fa-play"></i> Start
      </button>
      <button
        v-if="kernel.state === 'idle' || kernel.state === 'executing'"
        class="btn btn-secondary btn-sm"
        :disabled="busy"
        @click="$emit('stop', kernel.id)"
      >
        <i class="fa-solid fa-stop"></i> Stop
      </button>
      <button
        class="btn btn-danger btn-sm"
        :disabled="busy || kernel.state === 'starting'"
        @click="$emit('delete', kernel.id, kernel.name)"
      >
        <i class="fa-solid fa-trash-alt"></i> Delete
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { KernelInfo } from "../../types";
import KernelStatusBadge from "./KernelStatusBadge.vue";

const props = defineProps<{
  kernel: KernelInfo;
  busy: boolean;
}>();

defineEmits<{
  start: [id: string];
  stop: [id: string];
  delete: [id: string, name: string];
}>();

const maxPackagesShown = 5;

const displayedPackages = computed(() => props.kernel.packages.slice(0, maxPackagesShown));
</script>

<style scoped>
.kernel-card {
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  padding: var(--spacing-4);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  transition: border-color var(--transition-base) var(--transition-timing);
}

.kernel-card:hover {
  border-color: var(--color-border-secondary);
}

.kernel-card--error {
  border-color: var(--color-danger);
}

.kernel-card__header {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-0-5);
}

.kernel-card__title-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-2);
}

.kernel-card__name {
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
  margin: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.kernel-card__meta {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.kernel-card__id {
  font-size: var(--font-size-2xs);
  color: var(--color-text-muted);
  font-family: var(--font-family-mono);
  margin: 0;
}

.kernel-card__version {
  font-size: var(--font-size-2xs);
  color: var(--color-accent);
  font-family: var(--font-family-mono);
  font-weight: var(--font-weight-medium);
  background-color: var(--color-accent-subtle);
  padding: 0 var(--spacing-1);
  border-radius: var(--border-radius-sm);
}

.kernel-card__body {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  flex: 1;
}

.kernel-card__resources {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.kernel-card__resource {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
}

.kernel-card__resource i {
  color: var(--color-text-muted);
  font-size: var(--font-size-2xs);
}

.kernel-card__packages {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-1);
}

.kernel-card__package-tag {
  display: inline-block;
  background-color: var(--color-accent-subtle);
  color: var(--color-accent);
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-medium);
  padding: var(--spacing-0-5) var(--spacing-1-5);
  border-radius: var(--border-radius-sm);
}

.kernel-card__package-tag--more {
  background-color: var(--color-background-tertiary);
  color: var(--color-text-tertiary);
  cursor: default;
}

.kernel-card__no-packages {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  font-style: italic;
}

.kernel-card__error {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-1-5);
  background-color: var(--color-danger-light);
  color: var(--color-danger);
  font-size: var(--font-size-xs);
  padding: var(--spacing-2);
  border-radius: var(--border-radius-sm);
  word-break: break-word;
}

.kernel-card__error i {
  flex-shrink: 0;
  margin-top: 1px;
}

.kernel-card__actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding-top: var(--spacing-2);
  border-top: 1px solid var(--color-border-light);
}

.btn-sm {
  padding: var(--spacing-1) var(--spacing-2-5);
  font-size: var(--font-size-xs);
}
</style>
