<template>
  <div
    class="connection-item connection-item--stacked"
    :class="{ 'connection-item--active': isRunning }"
  >
    <div class="local-row">
      <div class="connection-info">
        <div class="connection-name">
          <i class="fa-solid fa-microchip"></i>
          <span>{{ LOCAL_PROVIDER_LABEL }}</span>
          <span class="badge" :class="badgeClass">{{ badgeLabel }}</span>
        </div>
        <div class="connection-details">
          <template v-if="localLoading">Checking this machine…</template>
          <template v-else-if="localError">{{ localError }}</template>
          <template v-else-if="status && !status.available">
            No prebuilt runtime exists for this OS / architecture.
          </template>
          <template v-else-if="status && !status.anyModelInstalled">
            <span>A small model that runs on this machine — offline, no key, no account.</span>
            <span class="separator">•</span>
            <span class="muted">Chat and simple builds only, no Agent</span>
          </template>
          <template v-else-if="status">
            <span>{{ status.modelName }}</span>
            <span class="separator">•</span>
            <span>runs on this machine, offline, no key</span>
            <span class="separator">•</span>
            <span class="muted">Chat and simple builds only, no Agent</span>
          </template>
        </div>
      </div>
      <div class="connection-actions">
        <button v-if="localError" type="button" class="btn btn-secondary" @click="loadLocalModel">
          <i class="fa-solid fa-rotate-right"></i>
          <span>Retry</span>
        </button>
        <template v-else-if="status?.available">
          <!-- Nothing installed yet → one-click setup of the recommended model. -->
          <button
            v-if="!status.anyModelInstalled"
            type="button"
            class="btn local-setup-btn"
            :disabled="localBusy || installing"
            @click="handleLocalSetup"
          >
            <i class="fa-solid fa-download"></i>
            <span>Set up (~{{ recommendedDownloadLabel }})</span>
          </button>
          <button
            v-else-if="!status.running"
            type="button"
            class="btn btn-secondary"
            :disabled="localBusy"
            @click="handleLocalStart"
          >
            <i class="fa-solid fa-play"></i>
            <span>Start</span>
          </button>
          <button
            v-else
            type="button"
            class="btn btn-secondary"
            :disabled="localBusy"
            @click="handleLocalStop"
          >
            <i class="fa-solid fa-stop"></i>
            <span>Stop</span>
          </button>
          <button
            type="button"
            class="btn btn-secondary"
            :aria-expanded="expanded"
            @click="expanded = !expanded"
          >
            <i :class="expanded ? 'fa-solid fa-chevron-up' : 'fa-solid fa-chevron-down'"></i>
            <span>{{ expanded ? "Hide" : "Manage" }}</span>
          </button>
        </template>
      </div>
    </div>

    <div v-if="installing" class="local-progress">
      <div class="local-progress__label">
        <span
          >{{ installPhaseLabel
          }}<template v-if="installingModelName"> · {{ installingModelName }}</template></span
        >
        <span v-if="installPct !== null">{{ installPct }}%</span>
      </div>
      <div class="local-progress__track">
        <div
          class="local-progress__bar"
          :class="{ 'is-indeterminate': installPct === null }"
          :style="installPct !== null ? { width: installPct + '%' } : undefined"
        ></div>
      </div>
    </div>

    <div v-if="expanded && status?.available" class="local-details">
      <!-- Plain-language expectations. A model this size is genuinely useful
           for some things and genuinely not for others; say so up front. -->
      <section class="local-expect">
        <h4 class="local-expect__title">What to expect from a model this size</h4>
        <ul class="local-expect__list">
          <li class="local-expect__item local-expect__item--yes">
            <i class="fa-solid fa-check"></i>
            <span>
              <strong>Good at:</strong> chatting about your flow, explaining nodes, writing
              descriptions, and building a simple flow from one sentence (Simple build). Nothing
              leaves your machine.
            </span>
          </li>
          <li class="local-expect__item local-expect__item--no">
            <i class="fa-solid fa-xmark"></i>
            <span>
              <strong>Can't do:</strong> the Agent. Auto-agent and Agent modes build step by step
              through tool calls, which these small models don't support, so the chat offers only
              Chat and Simple build while On-device AI is selected.
            </span>
          </li>
          <li class="local-expect__item local-expect__item--meh">
            <i class="fa-solid fa-minus"></i>
            <span>
              <strong>Rougher edges:</strong> simpler answers than a cloud model, the occasional
              wrong column name, and slower replies on CPU. Flowfile sends it a trimmed view of your
              flow (at most 12 columns per node) to fit its memory, and ⌘K / next-node suggestions
              miss more often. For bigger flows, add a cloud provider or Ollama.
            </span>
          </li>
        </ul>
      </section>

      <!-- The catalog: install another size, switch the active one (Use), or delete. -->
      <h4 class="local-section__title">Models</h4>
      <div class="connections-list">
        <div
          v-for="m in status.models"
          :key="m.id"
          class="connection-item"
          :class="{ 'connection-item--active': m.id === status.selectedModelId }"
        >
          <div class="connection-info">
            <div class="connection-name">
              <i class="fa-solid fa-microchip"></i>
              <span>{{ m.name }}</span>
              <span class="badge" :class="modelBadgeClass(m)">{{ modelBadgeLabel(m) }}</span>
            </div>
            <div class="connection-details">
              <span>{{ m.description }}</span>
              <span class="separator">•</span>
              <span class="muted">~{{ m.approxDownloadMb }} MB download</span>
            </div>
          </div>
          <div class="connection-actions">
            <button
              v-if="!m.installed"
              type="button"
              class="btn btn-secondary"
              :disabled="localBusy || installing"
              @click="handleLocalInstall(m.id)"
            >
              <i class="fa-solid fa-download"></i>
              <span>Install</span>
            </button>
            <button
              v-if="m.installed && m.id !== status.selectedModelId"
              type="button"
              class="btn btn-secondary"
              :disabled="localBusy || installing"
              @click="handleLocalSelect(m.id)"
            >
              <i class="fa-solid fa-circle-check"></i>
              <span>Use</span>
            </button>
            <button
              v-if="m.installed"
              type="button"
              class="btn btn-danger"
              :disabled="localBusy || installing"
              @click="handleLocalDelete(m.id)"
            >
              <i class="fa-solid fa-trash-alt"></i>
              <span>Delete</span>
            </button>
          </div>
        </div>
      </div>

      <!-- Context window: tokens the model can hold. Bigger = more flow
           context fits, but more RAM. Applies on the next model start. -->
      <div v-if="status.anyModelInstalled" class="local-ctx">
        <label class="local-ctx__label" for="local-ctx-size">Context window (tokens)</label>
        <div class="local-ctx__row">
          <input
            id="local-ctx-size"
            v-model.number="ctxSizeInput"
            type="number"
            class="form-input local-ctx__input"
            :min="status.ctxSizeMin"
            :max="status.ctxSizeMax"
            step="1024"
            :disabled="localBusy"
            @keyup.enter="handleSetCtxSize"
          />
          <button
            type="button"
            class="btn btn-secondary"
            :disabled="localBusy || ctxSizeInput === status.ctxSize"
            @click="handleSetCtxSize"
          >
            Apply
          </button>
        </div>
        <p class="hint-text">
          {{ status.ctxSizeMin }}–{{ status.ctxSizeMax }}. Larger fits more flow context but uses
          more RAM.
          <template v-if="status.running"> Applying restarts the local server.</template>
        </p>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
// The on-device model as one row in the Providers list. It sits beside the
// cloud providers because it *is* a provider (the same "On-device AI" the
// model pickers offer); Manage expands the catalog and context settings.
import { computed, onMounted, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import { useAiStore } from "../../stores/ai-store";
import { AiDisabledError } from "./api";
import {
  LOCAL_PROVIDER_LABEL,
  deleteLocalModel,
  fetchLocalModelStatus,
  selectLocalModel,
  setLocalCtxSize,
  startLocalModel,
  stopLocalModel,
  streamLocalModelInstall,
} from "./localModelApi";
import type { LocalModelEntry } from "./localModelApi";

// Single source of truth: the ai-store owns local status (so the Assistant
// tab's pickers and this row never disagree). This is a read-only mirror;
// every mutation below routes its fresh status back through
// ``applyLocalModelStatus``.
const aiStore = useAiStore();
const status = computed(() => aiStore.localModelStatus);
const localLoading = ref(true);
const localBusy = ref(false);
const localError = ref<string | null>(null);
const installing = ref(false);
const installPhase = ref("");
const installPct = ref<number | null>(null);
// Which model is mid-install, so the progress bar can name it.
const installingModelId = ref<string | null>(null);
const expanded = ref(false);
// Editable copy of the context-window size; synced from status on each load
// and applied via handleSetCtxSize.
const ctxSizeInput = ref<number>(status.value?.ctxSize ?? 16384);

const isRunning = computed(() => status.value?.running === true);

const badgeLabel = computed((): string => {
  const st = status.value;
  if (localLoading.value) return "checking";
  if (localError.value) return "unavailable";
  if (!st || !st.available) return "not available";
  if (!st.anyModelInstalled) return "not installed";
  return st.running ? "running" : "ready";
});

const badgeClass = computed((): string => {
  const st = status.value;
  if (localLoading.value || localError.value || !st || !st.available) return "badge--unconfigured";
  if (!st.anyModelInstalled) return "badge--unconfigured";
  return st.running ? "badge--configured" : "badge--env_fallback";
});

const installingModelName = computed(() => {
  const id = installingModelId.value;
  if (!id || !status.value) return "";
  return status.value.models.find((m) => m.id === id)?.name ?? "";
});

// "~1960 MB" → "2.0 GB", "~1100 MB" → "1.1 GB", small values stay in MB.
const formatDownloadMb = (mb: number | null | undefined): string => {
  if (!mb || mb <= 0) return "";
  return mb >= 1000 ? `${(mb / 1000).toFixed(1)} GB` : `${mb} MB`;
};

const installingModel = computed(
  () => status.value?.models.find((m) => m.id === installingModelId.value) ?? null,
);

// The model the one-click setup installs: the backend's selected default
// (Qwen2.5-Coder 3B) before anything is installed.
const recommendedModel = computed(
  () =>
    status.value?.models.find((m) => m.id === status.value?.selectedModelId) ??
    status.value?.models[0] ??
    null,
);
const recommendedDownloadLabel = computed(
  () => formatDownloadMb(recommendedModel.value?.approxDownloadMb) || "small download",
);

// Per-model badge: running > selected > installed > not installed. Only the
// selected model can be "running" (one server at a time).
const modelBadgeLabel = (m: LocalModelEntry): string => {
  const st = status.value;
  if (!st) return "";
  if (m.id === st.selectedModelId && st.running) return "running";
  if (m.id === st.selectedModelId && m.installed) return "active";
  if (m.installed) return "installed";
  return "not installed";
};

const modelBadgeClass = (m: LocalModelEntry): string => {
  const st = status.value;
  if (st && m.id === st.selectedModelId && st.running) return "badge--configured";
  if (st && m.id === st.selectedModelId && m.installed) return "badge--env_fallback";
  if (m.installed) return "badge--test badge--ok";
  return "badge--unconfigured";
};

const installPhaseLabel = computed(() => {
  switch (installPhase.value) {
    case "downloading_binary":
      return "Downloading runtime…";
    case "extracting":
      return "Extracting…";
    case "downloading_model": {
      const size = formatDownloadMb(installingModel.value?.approxDownloadMb);
      return size ? `Downloading model (~${size})…` : "Downloading model…";
    }
    case "verifying":
      return "Verifying…";
    case "done":
      return "Done";
    default:
      return "Installing…";
  }
});

// Mirror the backend's ctx size into the editable input whenever status
// changes (load, select, install, apply) so the field always reflects truth.
watch(
  () => status.value?.ctxSize,
  (size) => {
    if (typeof size === "number") ctxSizeInput.value = size;
  },
);

const handleSetCtxSize = async () => {
  const st = status.value;
  if (!st) return;
  const want = Math.round(Number(ctxSizeInput.value));
  if (!Number.isFinite(want) || want === st.ctxSize) return;
  localBusy.value = true;
  try {
    aiStore.applyLocalModelStatus(await setLocalCtxSize(want));
    ElMessage.success("Context window updated");
  } catch (error) {
    ElMessage.error((error as Error).message || "Failed to update context window");
  } finally {
    localBusy.value = false;
  }
};

const loadLocalModel = async () => {
  localLoading.value = true;
  localError.value = null;
  try {
    aiStore.applyLocalModelStatus(await fetchLocalModelStatus());
  } catch (error) {
    if (error instanceof AiDisabledError) {
      await aiStore.loadProviders();
    } else {
      // Most commonly: the running backend predates the local-model routes
      // (needs a restart) or is unreachable. Surface it with a Retry rather
      // than rendering an empty row.
      aiStore.applyLocalModelStatus(null);
      localError.value =
        (error as { response?: { status?: number } })?.response?.status === 404
          ? "On-device AI endpoints not found. Restart flowfile_core to pick up this feature, then Retry."
          : (error as Error)?.message || "Couldn't reach the on-device AI service.";
    }
  } finally {
    localLoading.value = false;
  }
};

// One-click onboarding: install the recommended default. Manage is there if
// the user wants a different size.
const handleLocalSetup = async () => {
  const id = recommendedModel.value?.id ?? status.value?.selectedModelId;
  if (!id) return;
  await handleLocalInstall(id);
};

const handleLocalInstall = async (modelId: string) => {
  installing.value = true;
  localBusy.value = true;
  installingModelId.value = modelId;
  installPhase.value = "";
  installPct.value = null;
  let failed: string | null = null;
  try {
    await streamLocalModelInstall(
      {
        onProgress: (ev) => {
          installPhase.value = ev.phase;
          if (typeof ev.received === "number" && typeof ev.total === "number" && ev.total > 0) {
            installPct.value = Math.min(100, Math.round((ev.received / ev.total) * 100));
          } else {
            installPct.value = null;
          }
        },
        onError: (msg) => {
          failed = msg;
        },
      },
      undefined,
      modelId,
    );
  } catch (error) {
    failed = (error as Error).message || "Install failed";
  } finally {
    installing.value = false;
    localBusy.value = false;
    installingModelId.value = null;
    await loadLocalModel();
  }
  if (failed) {
    ElMessage.error(`Install failed: ${failed}`);
  } else {
    ElMessage.success("Model installed");
  }
};

const handleLocalSelect = async (modelId: string) => {
  localBusy.value = true;
  try {
    aiStore.applyLocalModelStatus(await selectLocalModel(modelId));
    ElMessage.success("Model selected");
  } catch (error) {
    ElMessage.error((error as Error).message || "Failed to select model");
  } finally {
    localBusy.value = false;
  }
};

const handleLocalStart = async () => {
  localBusy.value = true;
  try {
    aiStore.applyLocalModelStatus(await startLocalModel());
    ElMessage.success("On-device AI started");
  } catch (error) {
    ElMessage.error((error as Error).message || "Failed to start on-device AI");
  } finally {
    localBusy.value = false;
  }
};

const handleLocalStop = async () => {
  localBusy.value = true;
  try {
    aiStore.applyLocalModelStatus(await stopLocalModel());
    ElMessage.success("On-device AI stopped");
  } catch {
    ElMessage.error("Failed to stop on-device AI");
  } finally {
    localBusy.value = false;
  }
};

const handleLocalDelete = async (modelId: string) => {
  localBusy.value = true;
  try {
    aiStore.applyLocalModelStatus(await deleteLocalModel(modelId));
    ElMessage.success("Model removed");
  } catch {
    ElMessage.error("Failed to remove model");
  } finally {
    localBusy.value = false;
  }
};

onMounted(() => {
  // The view's loadProviders already fetched status once; only re-fetch when
  // it hasn't landed yet (direct deep link) so the error/Retry path exists.
  if (status.value === null) {
    void loadLocalModel();
  } else {
    localLoading.value = false;
  }
});
</script>

<style scoped>
.local-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-4);
  width: 100%;
}

/* Primary one-click onboarding button (accent-filled, like the chat Build CTA). */
.local-setup-btn {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  border: none;
  background-color: var(--color-accent);
  color: #fff;
  font-weight: var(--font-weight-medium);
}

.local-setup-btn:hover:not(:disabled) {
  filter: brightness(1.05);
}

.local-setup-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.local-progress {
  width: 100%;
  margin-top: var(--spacing-3);
}

.local-progress__label {
  display: flex;
  justify-content: space-between;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  margin-bottom: var(--spacing-1);
}

.local-progress__track {
  height: 6px;
  background-color: var(--color-background-muted);
  border-radius: var(--border-radius-full);
  overflow: hidden;
}

.local-progress__bar {
  height: 100%;
  background-color: var(--color-accent);
  border-radius: var(--border-radius-full);
  transition: width 0.2s ease;
}

.local-progress__bar.is-indeterminate {
  width: 40%;
  animation: local-progress-slide 1.2s ease-in-out infinite;
}

@keyframes local-progress-slide {
  0% {
    margin-left: -40%;
  }
  100% {
    margin-left: 100%;
  }
}

.local-details {
  width: 100%;
  margin-top: var(--spacing-4);
  padding-top: var(--spacing-4);
  border-top: 1px solid var(--color-border-light);
}

.local-expect {
  margin-bottom: var(--spacing-4);
  padding: var(--spacing-3) var(--spacing-4);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-muted);
}

.local-expect__title,
.local-section__title {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.local-expect__list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  margin: 0;
  padding: 0;
  list-style: none;
}

.local-expect__item {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  font-size: var(--font-size-xs);
  line-height: 1.5;
  color: var(--color-text-secondary);
}

.local-expect__item strong {
  color: var(--color-text-primary);
}

.local-expect__item i {
  flex-shrink: 0;
  width: 14px;
  margin-top: 3px;
  text-align: center;
}

.local-expect__item--yes i {
  color: var(--color-success, #16a34a);
}

.local-expect__item--no i {
  color: var(--color-danger, #dc2626);
}

.local-expect__item--meh i {
  color: var(--color-warning, #ca8a04);
}

.local-ctx {
  margin-top: var(--spacing-4);
  padding-top: var(--spacing-3);
  border-top: 1px solid var(--color-border-light);
}

.local-ctx__label {
  display: block;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  margin-bottom: var(--spacing-1);
}

.local-ctx__row {
  display: flex;
  gap: var(--spacing-2);
  align-items: center;
}

.local-ctx__input {
  max-width: 140px;
}
</style>
