<template>
  <div class="telemetry-card">
    <div class="telemetry-card__header">
      <div>
        <h3 class="telemetry-card-title">Anonymous usage telemetry</h3>
        <p class="telemetry-card-description">
          Share anonymous usage events to help prioritise Flowfile development. Turned off by
          default. Events describe features you use — never your data, file paths, column names,
          formulas, or anything you type.
        </p>
      </div>
      <div class="telemetry-toggle">
        <span class="telemetry-toggle__word">{{ toggleWord }}</span>
        <el-switch
          :model-value="consentOn"
          :disabled="switchDisabled"
          aria-label="Anonymous usage telemetry"
          @change="onToggle"
        />
      </div>
    </div>

    <p class="telemetry-sent-line">
      What is sent: event names, bucketed counts, and app version — nothing you type.
      <button class="link-btn" type="button" @click="openDocs">
        <i class="fa-solid fa-arrow-up-right-from-square"></i>
        Read exactly what is sent
      </button>
    </p>

    <p v-if="statusUnknown" class="pool-hint--warning" role="alert">
      <i class="fa-solid fa-triangle-exclamation"></i>
      <span>
        Couldn't read the telemetry setting, so its current state is unknown — it may be on or off.
        <button class="link-btn" type="button" :disabled="loading" @click="refresh">Retry</button>
      </span>
    </p>

    <div v-else-if="status && !status.canManage" class="telemetry-locked">
      <i class="fa-solid fa-lock"></i>
      <h4>Administrator setting</h4>
      <p>
        Telemetry is server-wide and managed by administrators. Nothing is shared unless an
        administrator turns it on.
      </p>
    </div>

    <template v-else-if="status">
      <p v-if="status.envKillSwitch" class="pool-hint--warning">
        <i class="fa-solid fa-triangle-exclamation"></i>
        <span>
          <code>FLOWFILE_TELEMETRY</code> is set in the environment and disables telemetry
          regardless of this setting. Unset it to manage telemetry here.
        </span>
      </p>
      <p v-else-if="!status.endpointConfigured" class="pool-hint--warning">
        <i class="fa-solid fa-triangle-exclamation"></i>
        <span>
          No telemetry endpoint is configured (<code>FLOWFILE_TELEMETRY_ENDPOINT</code>); nothing is
          ever sent.
        </span>
      </p>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { ElMessage, ElSwitch } from "element-plus";
import { desktop } from "../../../lib/desktop";
import { useTelemetryStore } from "../../stores/telemetry-store";
import { TELEMETRY_DOCS_URL } from "./telemetryConsent";

const telemetryStore = useTelemetryStore();

const applying = ref(false);
const loading = ref(true);

const status = computed(() => telemetryStore.status);
const consentOn = computed(() => status.value?.consent === true);
// A failed read is "unknown", never "Off" — never claim telemetry is off while it may be on.
const statusUnknown = computed(
  () => !loading.value && (telemetryStore.loadFailed || !status.value),
);
const toggleWord = computed(() => {
  if (loading.value) return "…";
  if (statusUnknown.value) return "Unknown";
  return consentOn.value ? "On" : "Off";
});
const switchDisabled = computed(
  () =>
    applying.value ||
    loading.value ||
    statusUnknown.value ||
    !status.value ||
    !status.value.available ||
    !status.value.canManage,
);

const refresh = async () => {
  loading.value = true;
  try {
    await telemetryStore.loadStatus(true);
  } finally {
    loading.value = false;
  }
};

onMounted(() => {
  // Refetch every visit: the endpoint/kill-switch state can change across backend restarts.
  void refresh();
});

const onToggle = async (enabled: boolean | string | number) => {
  applying.value = true;
  try {
    const next = await telemetryStore.setConsent(Boolean(enabled));
    ElMessage.success(
      next.consent ? "Anonymous usage telemetry turned on" : "Anonymous usage telemetry turned off",
    );
  } catch {
    // Covers the backend's 503 when telemetry.yaml can't be written: nothing was saved.
    ElMessage.error("Couldn't save the telemetry setting");
  } finally {
    applying.value = false;
  }
};

function openDocs() {
  void desktop.openExternal(TELEMETRY_DOCS_URL);
}
</script>

<style scoped>
.telemetry-card {
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  box-shadow: var(--shadow-xs);
  padding: var(--spacing-4) var(--spacing-5);
}

.telemetry-card__header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: var(--spacing-4);
}

.telemetry-card-title {
  margin: 0 0 var(--spacing-1);
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.telemetry-card-description {
  margin: 0;
  font-size: var(--font-size-md);
  color: var(--color-text-secondary);
  text-wrap: pretty;
}

.telemetry-toggle {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  flex-shrink: 0;
  white-space: nowrap;
}

.telemetry-toggle__word {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
  min-width: 2ch;
  text-align: right;
}

.telemetry-sent-line {
  margin: var(--spacing-3) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.link-btn {
  background: none;
  border: none;
  padding: 0;
  cursor: pointer;
  color: var(--color-accent);
  font-size: var(--font-size-sm);
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  margin-left: var(--spacing-1);
}

.link-btn:hover {
  text-decoration: underline;
}

.link-btn:disabled {
  cursor: default;
  opacity: 0.6;
  text-decoration: none;
}

.telemetry-locked {
  text-align: center;
  padding: var(--spacing-5) var(--spacing-4);
  color: var(--color-text-secondary);
}

.telemetry-locked i {
  font-size: 2em;
  opacity: 0.4;
}

.telemetry-locked h4 {
  margin: var(--spacing-2) 0 var(--spacing-1);
  font-size: var(--font-size-base);
  color: var(--color-text-primary);
}

.telemetry-locked p {
  margin: 0;
  font-size: var(--font-size-sm);
}

.pool-hint--warning {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  margin: var(--spacing-3) 0 0;
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-warning-light);
  color: var(--color-warning-dark);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
}

.pool-hint--warning i {
  margin-top: 2px;
  flex-shrink: 0;
}
</style>
