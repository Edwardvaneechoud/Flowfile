<template>
  <el-dialog
    :model-value="visible"
    :title="CONSENT_COPY.headline"
    width="min(520px, calc(100vw - 2rem))"
    align-center
    append-to-body
    :close-on-click-modal="true"
    :close-on-press-escape="true"
    :show-close="true"
    :before-close="onUserClose"
  >
    <div class="telemetry-consent-body">
      <p class="consent-text">{{ CONSENT_COPY.body }}</p>
      <p v-if="isMultiUser" class="consent-text">{{ CONSENT_COPY.serverWideLine }}</p>

      <div class="consent-example">
        <button
          class="example-toggle"
          type="button"
          :aria-expanded="exampleOpen"
          aria-controls="telemetry-example-json"
          @click="exampleOpen = !exampleOpen"
        >
          <i :class="exampleOpen ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'"></i>
          {{ CONSENT_COPY.exampleToggleLabel }}
        </button>
        <pre v-if="exampleOpen" id="telemetry-example-json" class="example-json">{{
          exampleJson
        }}</pre>
      </div>

      <p class="consent-env">
        {{ CONSENT_COPY.envVarLine.prefix }}<code>{{ CONSENT_COPY.envVarLine.code }}</code
        >{{ CONSENT_COPY.envVarLine.suffix }}
      </p>

      <p class="consent-env">{{ CONSENT_COPY.recoveryLine }}</p>

      <button class="link-btn" type="button" @click="openDocs">
        <i class="fa-solid fa-arrow-up-right-from-square"></i>
        {{ CONSENT_COPY.docsLinkLabel }}
      </button>

      <p v-if="saveFailed" class="consent-error" role="alert">
        <i class="fa-solid fa-triangle-exclamation"></i>
        {{ CONSENT_COPY.saveErrorLine }}
      </p>
    </div>

    <template #footer>
      <el-button :disabled="saving" @click="decline">{{ CONSENT_COPY.declineLabel }}</el-button>
      <el-button type="primary" :loading="saving" @click="accept">
        {{ saveFailed ? CONSENT_COPY.retryLabel : CONSENT_COPY.acceptLabel }}
      </el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { ElButton, ElDialog } from "element-plus";
import { useRoute } from "vue-router";
import { desktop } from "../../../lib/desktop";
import { useMultiUser } from "../../composables/useMultiUser";
import { useTelemetryStore } from "../../stores/telemetry-store";
import { useTutorialStore } from "../../stores/tutorial-store";
import {
  CONSENT_COPY,
  EXAMPLE_EVENT,
  TELEMETRY_DOCS_URL,
  decideConsentClose,
  shouldShowConsentModal,
  type ConsentCloseReason,
} from "./telemetryConsent";

const ANSWERED_KEY = "flowfile-telemetry-consent-answered";

function loadAnsweredTombstone(): boolean {
  try {
    return globalThis.localStorage?.getItem(ANSWERED_KEY) === "1";
  } catch {
    return false;
  }
}

const route = useRoute();
const telemetryStore = useTelemetryStore();
const tutorialStore = useTutorialStore();
const { isMultiUser } = useMultiUser();

// Tombstoned in localStorage: a backend that cannot persist consent must still never re-prompt.
const answered = ref(loadAnsweredTombstone());
const saving = ref(false);
const saveFailed = ref(false);
const exampleOpen = ref(false);

function markAnswered() {
  answered.value = true;
  try {
    globalThis.localStorage?.setItem(ANSWERED_KEY, "1");
  } catch {
    /* localStorage unavailable */
  }
}

const gatesOpen = computed(() =>
  shouldShowConsentModal({
    loaded: telemetryStore.loaded,
    available: telemetryStore.status?.available ?? false,
    consent: telemetryStore.status?.consent ?? null,
    canManage: telemetryStore.status?.canManage ?? false,
    routeName: route.name,
    tutorialActive: tutorialStore.isActive,
  }),
);

const visible = computed(() => !answered.value && (gatesOpen.value || saving.value));

// el-dialog's @open does not fire when it mounts already-open, so reset on the visibility flip.
watch(
  visible,
  (open) => {
    if (open) {
      exampleOpen.value = false;
      saveFailed.value = false;
    }
  },
  { immediate: true },
);

const exampleJson = JSON.stringify(EXAMPLE_EVENT, null, 2);

async function close(reason: ConsentCloseReason) {
  const decision = decideConsentClose(reason, answered.value);
  if (decision.consent === null) return;
  if (decision.tombstone === "now") markAnswered();
  if (decision.closeImmediately) {
    void telemetryStore.setConsent(decision.consent).catch(() => undefined);
    return;
  }
  saveFailed.value = false;
  saving.value = true;
  try {
    await telemetryStore.setConsent(decision.consent);
    markAnswered();
  } catch {
    // Keep the opt-in recoverable: no tombstone, dialog stays up to retry.
    saveFailed.value = true;
  } finally {
    saving.value = false;
  }
}

function accept() {
  void close("accept");
}

function decline() {
  void close("decline");
}

function onUserClose(done: () => void) {
  if (saving.value) return;
  decline();
  done();
}

function openDocs() {
  void desktop.openExternal(TELEMETRY_DOCS_URL);
}
</script>

<style scoped>
.telemetry-consent-body {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
}

.consent-text {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.5;
}

.example-toggle {
  background: none;
  border: none;
  padding: 0;
  cursor: pointer;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
}

.example-toggle:hover {
  color: var(--color-text-primary);
}

.example-toggle i {
  font-size: var(--font-size-xs);
  width: 12px;
}

.example-json {
  margin: var(--spacing-2) 0 0;
  padding: var(--spacing-3);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  overflow-x: auto;
  white-space: pre;
}

.consent-env {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.consent-env code {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  background: var(--color-background-secondary);
  padding: 1px 4px;
  border-radius: var(--border-radius-sm);
}

.consent-error {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  margin: 0;
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-warning-light);
  color: var(--color-warning-dark);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
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
  align-self: flex-start;
}

.link-btn:hover {
  text-decoration: underline;
}
</style>
