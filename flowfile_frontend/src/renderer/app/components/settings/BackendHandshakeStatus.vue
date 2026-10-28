<template>
  <div class="handshake" :class="{ 'handshake--compact': compact }">
    <div v-if="!compact" class="handshake__overview" aria-live="polite">
      <span class="handshake__status" :class="`handshake__status--${status.tone}`">
        <i :class="status.icon" aria-hidden="true"></i>
        {{ status.label }}
      </span>
      <span v-if="!verdict" class="handshake__hint">Run a handshake to check compatibility.</span>
      <span v-else-if="!accepted" class="handshake__hint">
        This worker cannot run flows. See details below.
      </span>
    </div>

    <div v-if="!compact && accepted && unavailable.length" class="handshake__unavailable">
      <span>Unavailable on this worker</span>
      <ul aria-label="Unavailable capabilities">
        <li v-for="name in unavailable" :key="name">{{ absentLabel(name) }}</li>
      </ul>
    </div>

    <details v-if="verdict" class="handshake__details">
      <summary>{{ compact ? "Compatibility details" : "Handshake details" }}</summary>
      <div class="handshake__detail-content">
        <dl v-if="verdict.backend" class="handshake__versions">
          <div v-for="(value, label) in versions" :key="label">
            <dt>{{ label }}</dt>
            <dd>{{ value }}</dd>
          </div>
        </dl>

        <div v-if="verdict.error" class="handshake__diagnostic">
          <h5>Handshake error</h5>
          <p>{{ verdict.error }}</p>
        </div>

        <div v-if="verdict.differences.length" class="handshake__diagnostic">
          <h5>Differences from this server</h5>
          <ul>
            <li v-for="difference in verdict.differences" :key="difference">
              {{ difference }}
            </li>
          </ul>
        </div>

        <div v-if="unavailable.length" class="handshake__diagnostic">
          <h5>Unavailable capabilities</h5>
          <dl class="handshake__reasons">
            <div v-for="(reason, name) in verdict.absent" :key="name">
              <dt>{{ absentLabel(String(name)) }}</dt>
              <dd>{{ reason }}</dd>
            </div>
          </dl>
        </div>
      </div>
    </details>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { HandshakeVerdict } from "../../api/computeBackends.api";
import { absentLabel } from "../../utils/executionLocation";

const props = defineProps<{ verdict?: HandshakeVerdict | null; compact?: boolean }>();

const accepted = computed(() => props.verdict?.ok && !!props.verdict.backend);
const unavailable = computed(() => Object.keys(props.verdict?.absent ?? {}));
const status = computed(() => {
  const verdict = props.verdict;
  if (!verdict) {
    return { tone: "neutral", icon: "fa-regular fa-circle", label: "Not checked" };
  }
  if (!accepted.value) {
    return {
      tone: "error",
      icon: "fa-solid fa-circle-xmark",
      label: verdict.backend ? "Not accepted by this server" : "Handshake failed",
    };
  }
  if (unavailable.value.length || verdict.differences.length) {
    return {
      tone: "warning",
      icon: "fa-solid fa-triangle-exclamation",
      label: unavailable.value.length ? "Accepted with limitations" : "Accepted with differences",
    };
  }
  return { tone: "success", icon: "fa-solid fa-circle-check", label: "Matches this server" };
});

const versions = computed(() => ({
  Flowfile: props.verdict?.backend?.app_version,
  Polars: props.verdict?.backend?.polars_version,
  Python: props.verdict?.backend?.python_version,
  Platform: props.verdict?.backend?.platform,
}));
</script>

<style scoped>
.handshake {
  border-top: 1px solid var(--color-border-light);
  padding-top: var(--spacing-3);
  font-size: var(--font-size-sm);
  line-height: var(--line-height-normal);
}

.handshake__overview {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-2);
}

.handshake--compact {
  min-width: 0;
  border-top: 0;
  padding-top: 0;
}

.handshake__status {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1-5);
  padding: var(--spacing-1) var(--spacing-2);
  border-radius: var(--border-radius-sm);
  font-weight: var(--font-weight-medium);
}

.handshake__status--neutral {
  background: var(--color-background-soft);
  color: var(--color-text-secondary);
}

.handshake__status--success {
  background: var(--color-success-light);
  color: var(--color-success-dark);
}

.handshake__status--warning {
  background: var(--color-warning-light);
  color: var(--color-warning-dark);
}

.handshake__status--error {
  background: var(--color-danger-light);
  color: var(--color-danger-dark);
}

.handshake__hint,
.handshake__unavailable {
  color: var(--color-text-secondary);
}

.handshake__unavailable {
  display: flex;
  align-items: baseline;
  flex-wrap: wrap;
  gap: var(--spacing-2);
  margin-top: var(--spacing-2);
}

.handshake__unavailable ul {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-1);
  list-style: none;
  margin: 0;
  padding: 0;
  min-width: 0;
}

.handshake__unavailable li {
  padding: 2px var(--spacing-2);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-sm);
  color: var(--color-text-primary);
  overflow-wrap: anywhere;
}

.handshake__details {
  margin-top: var(--spacing-2);
}

.handshake__details summary {
  width: fit-content;
  padding: var(--spacing-1) 0;
  color: var(--color-text-secondary);
  cursor: pointer;
}

.handshake__details summary:hover {
  color: var(--color-text-primary);
}

.handshake__details summary:focus-visible {
  outline: 2px solid var(--color-border-focus);
  outline-offset: 3px;
  border-radius: var(--border-radius-sm);
}

.handshake__detail-content {
  display: grid;
  gap: var(--spacing-4);
  margin-top: var(--spacing-2);
  padding: var(--spacing-3);
  border-radius: var(--border-radius-sm);
  background: var(--color-background-secondary);
  color: var(--color-text-primary);
}

.handshake__versions {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(min(150px, 100%), 1fr));
  gap: var(--spacing-3);
  margin: 0;
}

.handshake__versions dt {
  margin-bottom: var(--spacing-1);
  color: var(--color-text-secondary);
}

.handshake__versions dd {
  margin: 0;
  overflow-wrap: anywhere;
}

.handshake__diagnostic h5 {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  margin: 0 0 var(--spacing-2);
}

.handshake__diagnostic p,
.handshake__diagnostic ul,
.handshake__reasons {
  margin: 0;
}

.handshake__diagnostic ul {
  padding-left: var(--spacing-4);
}

.handshake__diagnostic li + li {
  margin-top: var(--spacing-2);
}

.handshake__reasons > div + div {
  margin-top: var(--spacing-3);
  padding-top: var(--spacing-3);
  border-top: 1px solid var(--color-border-primary);
}

.handshake__reasons dt {
  margin-bottom: var(--spacing-1);
  font-weight: var(--font-weight-medium);
}

.handshake__reasons dd {
  margin: 0;
}

.handshake__diagnostic p,
.handshake__diagnostic li,
.handshake__reasons dd {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}
</style>
