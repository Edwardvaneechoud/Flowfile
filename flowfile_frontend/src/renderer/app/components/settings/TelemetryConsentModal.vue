<template>
  <el-dialog
    :model-value="visible"
    :title="CONSENT_COPY.headline"
    width="520px"
    align-center
    append-to-body
    :close-on-click-modal="true"
    :close-on-press-escape="true"
    :show-close="true"
    @update:model-value="(v: boolean) => !v && decline()"
  >
    <div class="telemetry-consent-body">
      <p class="consent-text">{{ CONSENT_COPY.body }}</p>

      <div class="consent-example">
        <button class="example-toggle" type="button" @click="exampleOpen = !exampleOpen">
          <i :class="exampleOpen ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'"></i>
          {{ CONSENT_COPY.exampleToggleLabel }}
        </button>
        <pre v-if="exampleOpen" class="example-json">{{ exampleJson }}</pre>
      </div>

      <p class="consent-env">
        {{ CONSENT_COPY.envVarLine.prefix }}<code>{{ CONSENT_COPY.envVarLine.code }}</code
        >{{ CONSENT_COPY.envVarLine.suffix }}
      </p>

      <button class="link-btn" type="button" @click="openDocs">
        <i class="fa-solid fa-arrow-up-right-from-square"></i>
        {{ CONSENT_COPY.docsLinkLabel }}
      </button>
    </div>

    <template #footer>
      <el-button @click="decline">{{ CONSENT_COPY.declineLabel }}</el-button>
      <el-button type="primary" @click="accept">{{ CONSENT_COPY.acceptLabel }}</el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { ElButton, ElDialog } from "element-plus";
import { useRoute } from "vue-router";
import { desktop } from "../../../lib/desktop";
import { useTelemetryStore } from "../../stores/telemetry-store";
import { useTutorialStore } from "../../stores/tutorial-store";
import {
  CONSENT_COPY,
  EXAMPLE_EVENT,
  TELEMETRY_DOCS_URL,
  shouldShowConsentModal,
} from "./telemetryConsent";

const ANSWERED_KEY = "flowfile-telemetry-consent-answered";

function loadAnsweredTombstone(): boolean {
  try {
    return globalThis.localStorage?.getItem(ANSWERED_KEY) === "1";
  } catch {
    return false;
  }
}

function persistAnsweredTombstone() {
  try {
    globalThis.localStorage?.setItem(ANSWERED_KEY, "1");
  } catch {
    /* localStorage unavailable */
  }
}

const route = useRoute();
const telemetryStore = useTelemetryStore();
const tutorialStore = useTutorialStore();

// Local guard: an answer (either way, from any dismissal path) closes the
// dialog immediately and makes sure decline is never double-posted while the
// server response is still in flight. Seeded from a localStorage tombstone so a
// backend that cannot persist consent (yaml write failure -> consent stays
// null) still never re-prompts; the settings card remains the way to change it.
const answered = ref(loadAnsweredTombstone());
const exampleOpen = ref(false);

const visible = computed(
  () =>
    !answered.value &&
    shouldShowConsentModal({
      loaded: telemetryStore.loaded,
      available: telemetryStore.status?.available ?? false,
      consent: telemetryStore.status?.consent ?? null,
      canManage: telemetryStore.status?.canManage ?? false,
      routeName: route.name,
      tutorialActive: tutorialStore.isActive,
    }),
);

// el-dialog's @open does not fire on mount-while-open, so reset per-open
// state on the visibility flip instead (ShareDialog pattern).
watch(
  visible,
  (open) => {
    if (open) exampleOpen.value = false;
  },
  { immediate: true },
);

const exampleJson = JSON.stringify(EXAMPLE_EVENT, null, 2);

function answer(enabled: boolean) {
  if (answered.value) return;
  answered.value = true;
  persistAnsweredTombstone();
  // The modal path never nags: a failed save is dropped silently and the
  // settings card remains the way to change the choice.
  void telemetryStore.setConsent(enabled).catch(() => undefined);
}

function accept() {
  answer(true);
}

// Any dismissal — "No thanks", the X, escape, clicking outside — is a
// permanent decline; the settings toggle is the way back.
function decline() {
  answer(false);
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
