<template>
  <div
    v-if="visible"
    class="local-setup-card"
    :class="{ 'local-setup-card--compact': compact }"
    data-testid="local-model-setup-card"
  >
    <template v-if="loading && !status">
      <span class="local-setup-card__muted">Checking this computer…</span>
    </template>

    <template v-else-if="!available">
      <span class="local-setup-card__muted">
        On-device AI is not available on this operating system.
      </span>
    </template>

    <template v-else-if="installing">
      <div class="local-setup-card__progress">
        <div class="local-setup-card__label">
          <span>{{ phaseLabel }}</span>
          <span v-if="installPct !== null">{{ installPct }}%</span>
        </div>
        <div class="local-setup-card__track">
          <div
            class="local-setup-card__bar"
            :class="{ 'is-indeterminate': installPct === null }"
            :style="installPct !== null ? { width: installPct + '%' } : undefined"
          ></div>
        </div>
      </div>
    </template>

    <template v-else-if="installed">
      <span class="local-setup-card__ready">
        <i class="fa-solid fa-circle-check"></i>
        AI on this computer is ready
      </span>
    </template>

    <template v-else>
      <div class="local-setup-card__text">
        <div class="local-setup-card__title">{{ title ?? "Use AI on this computer" }}</div>
        <div v-if="!compact" class="local-setup-card__desc">
          {{ description ?? defaultDescription }}
        </div>
        <div v-if="installError" class="local-setup-card__error" role="alert">
          Install failed: {{ installError }}
        </div>
        <button
          v-if="!compact"
          type="button"
          class="local-setup-card__link"
          @click="emit('open-settings')"
        >
          Use an online provider instead
        </button>
      </div>
      <div class="local-setup-card__actions">
        <button type="button" class="local-setup-card__btn" @click="install()">
          <i class="fa-solid fa-download"></i>
          <span>{{ installError ? "Retry" : buttonLabel }}</span>
        </button>
        <button
          v-if="compact"
          type="button"
          class="local-setup-card__link"
          @click="emit('open-settings')"
        >
          Use an online provider instead
        </button>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted } from "vue";
import { useLocalModelInstall } from "../../composables/useLocalModelInstall";

const props = withDefaults(
  defineProps<{
    compact?: boolean;
    title?: string;
    description?: string;
    showUnavailable?: boolean;
  }>(),
  { compact: false, title: undefined, description: undefined, showUnavailable: false },
);

const emit = defineEmits<{
  (e: "installed"): void;
  (e: "open-settings"): void;
}>();

const {
  status,
  loading,
  installing,
  installPct,
  installError,
  phaseLabel,
  available,
  installed,
  downloadLabel,
  load,
  install,
} = useLocalModelInstall({ onInstalled: () => emit("installed") });

const visible = computed(() => {
  if (loading.value && !status.value) return true;
  if (!status.value) return false;
  return available.value || props.showUnavailable;
});

const defaultDescription = computed(
  () =>
    "It runs on this computer and nothing leaves it." +
    (downloadLabel.value ? ` One-time download of about ${downloadLabel.value}.` : ""),
);

const buttonLabel = computed(() =>
  downloadLabel.value ? `Install (${downloadLabel.value})` : "Install",
);

onMounted(() => {
  void load();
});
</script>

<style scoped>
.local-setup-card {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  padding: var(--spacing-4);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-secondary);
}

.local-setup-card--compact {
  flex-direction: row;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-2) var(--spacing-3);
}

.local-setup-card__title {
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.local-setup-card__desc,
.local-setup-card__muted {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.local-setup-card__ready {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.local-setup-card__error {
  margin-top: var(--spacing-1);
  font-size: var(--font-size-sm);
  color: var(--color-danger, #c0392b);
}

.local-setup-card__actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
}

.local-setup-card__btn {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-4);
  border: none;
  border-radius: var(--border-radius-md);
  background-color: var(--color-accent);
  color: #fff;
  font-weight: var(--font-weight-medium);
  cursor: pointer;
}

.local-setup-card__btn:hover {
  filter: brightness(1.05);
}

.local-setup-card__link {
  padding: 0;
  border: none;
  background: none;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  text-decoration: underline;
  cursor: pointer;
}

.local-setup-card__progress {
  width: 100%;
}

.local-setup-card__label {
  display: flex;
  justify-content: space-between;
  margin-bottom: var(--spacing-1);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.local-setup-card__track {
  height: 6px;
  overflow: hidden;
  border-radius: var(--border-radius-full);
  background-color: var(--color-background-muted);
}

.local-setup-card__bar {
  height: 100%;
  border-radius: var(--border-radius-full);
  background-color: var(--color-accent);
  transition: width 0.2s ease;
}

.local-setup-card__bar.is-indeterminate {
  width: 40%;
  animation: local-setup-slide 1.2s ease-in-out infinite;
}

@keyframes local-setup-slide {
  0% {
    margin-left: -40%;
  }
  100% {
    margin-left: 100%;
  }
}
</style>
