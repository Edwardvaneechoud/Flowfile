<template>
  <div class="connections-overview">
    <header class="overview-header">
      <h2 class="page-title">Connections</h2>
      <p class="page-description">
        Manage all your external integrations in one place. Pick a connection type to get started —
        every connection you save is available across all your flows.
      </p>
    </header>

    <div class="overview-grid">
      <button
        v-for="type in connectionTypes"
        :key="type.key"
        type="button"
        class="overview-card"
        @click="emit('select', type.key)"
      >
        <div class="overview-card-icon"><i :class="type.icon"></i></div>
        <span v-if="statuses[type.key]?.configured" class="overview-card-badge">
          <i class="fa-solid fa-circle-check"></i>
          {{ statuses[type.key]!.text }}
        </span>
        <h5 class="overview-card-title">{{ type.label }}</h5>
        <p class="overview-card-desc">{{ type.description }}</p>
        <span class="overview-card-cta">
          {{ ctaLabel(type.key) }}
          <i class="fa-solid fa-arrow-right"></i>
        </span>
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { connectionTypes } from "./connectionTypes";
import type { ConnectionType, ConnectionTypeKey } from "./connectionTypes";

const props = defineProps<{
  // Per-type "already set up" counts. Undefined for a type = unknown (still
  // loading or the lookup failed) — no badge is shown in that case.
  counts?: Partial<Record<ConnectionTypeKey, number>>;
}>();

const emit = defineEmits<{
  (e: "select", tab: ConnectionTypeKey): void;
}>();

type CardStatus = { configured: boolean; text: string };

function statusFor(type: ConnectionType): CardStatus | null {
  const n = props.counts?.[type.key];
  if (n === undefined) return null;
  if (n > 0) {
    return { configured: true, text: `${n} ${type.countUnit}${n === 1 ? "" : "s"}` };
  }
  return { configured: false, text: "Not set up yet" };
}

const statuses = computed(
  () =>
    Object.fromEntries(connectionTypes.map((type) => [type.key, statusFor(type)])) as Record<
      ConnectionTypeKey,
      CardStatus | null
    >,
);

function ctaLabel(key: ConnectionTypeKey): string {
  const status = statuses.value[key];
  if (!status) return "Open";
  return status.configured ? "Manage" : "Set up";
}
</script>

<style scoped>
.connections-overview {
  max-width: 920px;
  margin: 0 auto;
}

.overview-header {
  margin-bottom: var(--spacing-5);
}

.overview-header .page-description {
  max-width: 660px;
  margin-top: var(--spacing-1);
}

.overview-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
  gap: var(--spacing-4);
}

.overview-card {
  position: relative;
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  width: 100%;
  padding: var(--spacing-4);
  font: inherit;
  text-align: left;
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  cursor: pointer;
  transition:
    border-color var(--transition-fast),
    transform var(--transition-fast),
    box-shadow var(--transition-fast);
}

.overview-card:hover {
  border-color: var(--color-accent);
  transform: translateY(-2px);
  box-shadow: var(--shadow-sm);
}

.overview-card:focus-visible {
  outline: none;
  border-color: var(--color-accent);
  box-shadow: 0 0 0 2px var(--color-accent-subtle);
}

.overview-card-icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 2.5rem;
  height: 2.5rem;
  margin-bottom: var(--spacing-3);
  background: var(--color-accent-subtle);
  border-radius: 50%;
  color: var(--color-accent);
  font-size: var(--font-size-lg);
}

.overview-card-badge {
  position: absolute;
  top: var(--spacing-4);
  right: var(--spacing-4);
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  line-height: 1.4;
  color: var(--color-success);
  background: var(--color-success-light, rgba(16, 185, 129, 0.12));
}

.overview-card-title {
  margin: 0 0 var(--spacing-1);
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.overview-card-desc {
  margin: 0 0 var(--spacing-3);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.5;
}

.overview-card-cta {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  margin-top: auto;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-accent);
}

.overview-card-cta i {
  transition: transform var(--transition-fast);
}

.overview-card:hover .overview-card-cta i {
  transform: translateX(3px);
}
</style>
