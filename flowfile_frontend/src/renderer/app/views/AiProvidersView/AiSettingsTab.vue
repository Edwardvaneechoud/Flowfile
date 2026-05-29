<template>
  <div class="ai-providers-container">
    <div class="mb-3">
      <h2 class="page-title">AI Providers</h2>
      <p class="description-text">
        Bring your own keys for the LLM providers Flowfile's AI features use. Local Ollama needs no
        key.
      </p>
    </div>

    <!-- Disabled-state: matches the AI subsystem's 503 contract.
         The in-app admin button means non-technical users don't need
         to touch .env. The hint about FEATURE_FLAG_AI in .env stays
         (admin-only, below the button) because process-memory toggles
         don't survive a restart. -->
    <div v-if="isDisabled" class="card mb-3">
      <div class="card-content">
        <div class="info-box">
          <i class="fa-solid fa-circle-info"></i>
          <div class="info-body">
            <p><strong>AI features are off</strong></p>
            <p>{{ AI_DISABLED_DETAIL }}</p>

            <template v-if="isAdmin">
              <div class="info-actions">
                <el-button
                  type="primary"
                  :loading="isEnablingFlag"
                  :disabled="isEnablingFlag"
                  @click="handleEnableFlag"
                >
                  <i class="fa-solid fa-wand-magic-sparkles"></i>
                  <span>Enable AI features</span>
                </el-button>
              </div>
              <p class="hint-text">
                This enables AI for the running process. To persist across restarts, add
                <code>FEATURE_FLAG_AI=true</code> to your <code>.env</code> file.
              </p>
            </template>
            <template v-else>
              <p class="hint-text">
                Ask your administrator to enable AI features for this Flowfile install.
              </p>
            </template>
          </div>
        </div>
      </div>
    </div>

    <!-- The chat drawer's mode dropdown is the single entry point for
         the send-mode preference (Chat / Auto-agent / Agent). This tab
         keeps the provider list + admin controls. -->

    <div v-if="!isDisabled" class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Providers ({{ providers.length }})</h3>
      </div>
      <div class="card-content">
        <div v-if="isLoading" class="loading-state">
          <div class="loading-spinner"></div>
          <p>Loading providers...</p>
        </div>

        <div v-else-if="providers.length === 0" class="empty-state">
          <i class="fa-solid fa-wand-magic-sparkles"></i>
          <p>No providers available</p>
          <p class="hint-text">
            Check that <code>flowfile_core</code> is reachable and that the AI subsystem is mounted.
          </p>
        </div>

        <div v-else class="connections-list">
          <div v-for="provider in providers" :key="provider.provider" class="connection-item">
            <div class="connection-info">
              <div class="connection-name">
                <i class="fa-solid fa-wand-magic-sparkles"></i>
                <span>{{ provider.provider }}</span>
                <span class="badge" :class="`badge--${provider.status}`">
                  {{ statusLabel(provider.status) }}
                </span>
                <span
                  v-if="provider.credential?.lastTestStatus"
                  class="badge badge--test"
                  :class="`badge--${provider.credential.lastTestStatus}`"
                >
                  test: {{ provider.credential.lastTestStatus }}
                </span>
              </div>
              <div class="connection-details">
                <span
                  >Default: {{ provider.credential?.defaultModel || provider.defaultModel }}</span
                >
                <span v-if="provider.credential?.apiBase" class="separator">•</span>
                <span v-if="provider.credential?.apiBase">{{ provider.credential.apiBase }}</span>
                <span v-if="provider.supportsTools" class="separator">•</span>
                <span v-if="provider.supportsTools" class="muted">tools</span>
                <span v-if="provider.supportsStreaming" class="separator">•</span>
                <span v-if="provider.supportsStreaming" class="muted">streaming</span>
              </div>
            </div>
            <div class="connection-actions">
              <button
                type="button"
                class="btn btn-secondary"
                :disabled="busyProvider === provider.provider"
                @click="showEditModal(provider)"
              >
                <i class="fa-solid fa-edit"></i>
                <span>{{ provider.credential ? "Modify" : "Configure" }}</span>
              </button>
              <button
                type="button"
                class="btn btn-secondary"
                :disabled="!canTest(provider) || busyProvider === provider.provider"
                @click="handleTest(provider)"
              >
                <i class="fa-solid fa-bolt"></i>
                <span>Test</span>
              </button>
              <button
                v-if="provider.credential"
                type="button"
                class="btn btn-danger"
                :disabled="busyProvider === provider.provider"
                @click="showDeleteModal(provider)"
              >
                <i class="fa-solid fa-trash-alt"></i>
                <span>Delete</span>
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Local model: install an offline llama.cpp runtime + small GGUF on
         demand. Nothing downloads until the user clicks Install. -->
    <div v-if="!isDisabled" class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Local model (offline)</h3>
      </div>
      <div class="card-content">
        <div v-if="localLoading" class="loading-state">
          <div class="loading-spinner"></div>
          <p>Checking local model…</p>
        </div>

        <div v-else-if="localModel && !localModel.available" class="info-box">
          <i class="fa-solid fa-circle-info"></i>
          <div class="info-body">
            <p><strong>Not available on this platform</strong></p>
            <p>No prebuilt local-model runtime exists for your OS / architecture.</p>
          </div>
        </div>

        <template v-else-if="localModel">
          <div class="connection-item">
            <div class="connection-info">
              <div class="connection-name">
                <i class="fa-solid fa-microchip"></i>
                <span>{{ localModel.modelName }}</span>
                <span class="badge" :class="localBadgeClass">{{ localBadgeLabel }}</span>
              </div>
              <div class="connection-details">
                <span>Runs fully offline on your CPU — no API key, no cloud.</span>
                <span class="separator">•</span>
                <span class="muted">~{{ localModel.approxDownloadMb }} MB download</span>
              </div>
            </div>
            <div class="connection-actions">
              <button
                v-if="!localModel.installed"
                type="button"
                class="btn btn-secondary"
                :disabled="localBusy"
                @click="handleLocalInstall"
              >
                <i class="fa-solid fa-download"></i>
                <span>Install</span>
              </button>
              <button
                v-if="localModel.installed && !localModel.running"
                type="button"
                class="btn btn-secondary"
                :disabled="localBusy"
                @click="handleLocalStart"
              >
                <i class="fa-solid fa-play"></i>
                <span>Start</span>
              </button>
              <button
                v-if="localModel.running"
                type="button"
                class="btn btn-secondary"
                :disabled="localBusy"
                @click="handleLocalStop"
              >
                <i class="fa-solid fa-stop"></i>
                <span>Stop</span>
              </button>
              <button
                v-if="localModel.installed"
                type="button"
                class="btn btn-danger"
                :disabled="localBusy"
                @click="handleLocalDelete"
              >
                <i class="fa-solid fa-trash-alt"></i>
                <span>Delete</span>
              </button>
            </div>
          </div>

          <div v-if="installing" class="local-progress">
            <div class="local-progress__label">
              <span>{{ installPhaseLabel }}</span>
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

          <p v-if="!localModel.installed && !installing" class="hint-text">
            Installs Qwen 2.5 Coder 1.5B via llama.cpp into your Flowfile data directory. Only
            downloaded when you click Install.
          </p>
        </template>
      </div>
    </div>

    <el-dialog
      v-model="dialogVisible"
      :title="`Configure ${editingProvider?.provider ?? ''}`"
      width="500px"
      :before-close="handleCloseDialog"
    >
      <div v-if="editingProvider" class="form">
        <div class="form-field">
          <label class="form-label" for="api-key">API key</label>
          <div class="password-field">
            <input
              id="api-key"
              v-model="formApiKey"
              :type="showApiKey ? 'text' : 'password'"
              class="form-input"
              :placeholder="
                editingProvider.credential?.hasKey ? 'Leave blank to keep existing key' : 'sk-...'
              "
              :disabled="formClearApiKey"
              autocomplete="off"
              @keyup.enter="handleSubmit"
            />
            <button
              type="button"
              class="toggle-visibility"
              aria-label="Toggle API key visibility"
              @click="showApiKey = !showApiKey"
            >
              <i :class="showApiKey ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
            </button>
          </div>
          <p class="hint-text">
            {{
              editingProvider.provider === "ollama"
                ? "Ollama runs locally and doesn't need a key."
                : "Stored encrypted via the same Fernet pipeline as your other secrets."
            }}
          </p>
        </div>

        <div v-if="editingProvider.credential?.hasKey" class="form-field">
          <label class="checkbox-label">
            <input v-model="formClearApiKey" type="checkbox" />
            <span>Remove the stored key (fall back to env var if set)</span>
          </label>
        </div>

        <div class="form-field">
          <label class="form-label" for="default-model">Default model (optional)</label>
          <input
            id="default-model"
            v-model="formDefaultModel"
            type="text"
            class="form-input"
            :placeholder="editingProvider.defaultModel"
          />
          <p class="hint-text">
            Overrides the provider's class default for surfaces without a per-surface mapping.
          </p>
        </div>

        <div class="form-field">
          <label class="form-label">Available models (optional)</label>
          <div class="chip-input">
            <el-tag
              v-for="(model, index) in formModels"
              :key="`${model}-${index}`"
              closable
              :disable-transitions="false"
              class="chip-input__tag"
              @close="handleRemoveModel(index)"
            >
              {{ model }}
            </el-tag>
            <input
              v-model="formNewModel"
              type="text"
              class="chip-input__input"
              :placeholder="
                formModels.length === 0
                  ? 'e.g. moonshotai/kimi-k2:free — press Enter to add'
                  : 'Add another…'
              "
              @keydown.enter.prevent="handleAddModel"
              @keydown.delete="handleBackspaceTrim"
            />
          </div>
          <p class="hint-text">
            Lets the chat drawer switch between several models on this credential. Empty = chat
            drawer offers only the default model.
          </p>
        </div>

        <div class="form-field">
          <label class="form-label" for="api-base">API base URL (optional)</label>
          <input
            id="api-base"
            v-model="formApiBase"
            type="text"
            class="form-input"
            placeholder="https://..."
          />
          <p class="hint-text">Set this for self-hosted gateways or proxies.</p>
        </div>
      </div>

      <template #footer>
        <div class="dialog-footer">
          <el-button :disabled="isSubmitting" @click="dialogVisible = false">Cancel</el-button>
          <el-button type="primary" :loading="isSubmitting" @click="handleSubmit"> Save </el-button>
        </div>
      </template>
    </el-dialog>

    <el-dialog
      v-model="deleteDialogVisible"
      title="Delete provider credential"
      width="400px"
      :before-close="handleCloseDeleteDialog"
    >
      <p>
        Remove the credential for <strong>{{ providerToDelete?.provider }}</strong
        >?
      </p>
      <p class="warning-text">
        Stored API key (if any) will be deleted. Flows already running won't be interrupted; new AI
        requests will fall back to env-var detection or fail closed.
      </p>
      <template #footer>
        <div class="dialog-footer">
          <el-button @click="deleteDialogVisible = false">Cancel</el-button>
          <el-button type="danger" :loading="isDeleting" @click="handleDelete">Delete</el-button>
        </div>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { ElButton, ElDialog, ElMessage, ElTag } from "element-plus";
import { useAuthStore } from "../../stores/auth-store";
import {
  AiDisabledError,
  AI_DISABLED_DETAIL,
  deleteAiProvider,
  fetchAiProviders,
  setAiFeatureFlag,
  testAiProvider,
  upsertAiProvider,
} from "./api";
import type { AiProvider, AiProviderCredentialInput, AiProviderStatus } from "./aiProviderTypes";
import {
  deleteLocalModel,
  fetchLocalModelStatus,
  startLocalModel,
  stopLocalModel,
  streamLocalModelInstall,
} from "./localModelApi";
import type { LocalModelStatus } from "./localModelApi";

const authStore = useAuthStore();
const isAdmin = computed(() => authStore.isAdmin);

const providers = ref<AiProvider[]>([]);
const isLoading = ref(true);
const isDisabled = ref(false);
const isEnablingFlag = ref(false);
const busyProvider = ref<string | null>(null);

// Edit dialog
const dialogVisible = ref(false);
const editingProvider = ref<AiProvider | null>(null);
const formApiKey = ref("");
const formClearApiKey = ref(false);
const formDefaultModel = ref("");
const formApiBase = ref("");
// Curated models list, mirrors AiProviderCredential.models. The
// editor always works with an array; submit collapses [] to
// clearModels=true so the backend round-trips to NULL and reads stay
// null-vs-non-null (no [] in the wild).
const formModels = ref<string[]>([]);
const formNewModel = ref("");
const showApiKey = ref(false);
const isSubmitting = ref(false);

// Delete dialog
const deleteDialogVisible = ref(false);
const providerToDelete = ref<AiProvider | null>(null);
const isDeleting = ref(false);

const loadProviders = async () => {
  isLoading.value = true;
  try {
    providers.value = await fetchAiProviders();
    isDisabled.value = false;
  } catch (error) {
    if (error instanceof AiDisabledError) {
      isDisabled.value = true;
      providers.value = [];
    } else {
      ElMessage.error("Failed to load AI providers");
    }
  } finally {
    isLoading.value = false;
  }
};

const statusLabel = (status: AiProviderStatus): string => {
  switch (status) {
    case "configured":
      return "configured";
    case "env_fallback":
      return "env fallback";
    case "unconfigured":
      return "unconfigured";
  }
};

const canTest = (provider: AiProvider): boolean => {
  // Ollama needs no key; the rest need either a stored key or an env var fallback.
  return provider.provider === "ollama" || provider.status !== "unconfigured";
};

const showEditModal = (provider: AiProvider) => {
  editingProvider.value = provider;
  formApiKey.value = "";
  formClearApiKey.value = false;
  formDefaultModel.value = provider.credential?.defaultModel ?? "";
  formApiBase.value = provider.credential?.apiBase ?? "";
  formModels.value = provider.credential?.models ? [...provider.credential.models] : [];
  formNewModel.value = "";
  showApiKey.value = false;
  dialogVisible.value = true;
};

const handleAddModel = () => {
  const candidate = formNewModel.value.trim();
  if (!candidate) return;
  if (formModels.value.includes(candidate)) {
    formNewModel.value = "";
    return;
  }
  formModels.value = [...formModels.value, candidate];
  formNewModel.value = "";
};

const handleRemoveModel = (index: number) => {
  formModels.value = formModels.value.filter((_, i) => i !== index);
};

const handleBackspaceTrim = (event: KeyboardEvent) => {
  // Backspace on an empty input pops the last chip — common chip-input UX.
  if (event.key !== "Backspace") return;
  if (formNewModel.value.length > 0) return;
  if (formModels.value.length === 0) return;
  event.preventDefault();
  formModels.value = formModels.value.slice(0, -1);
};

const showDeleteModal = (provider: AiProvider) => {
  providerToDelete.value = provider;
  deleteDialogVisible.value = true;
};

const handleSubmit = async () => {
  if (!editingProvider.value) return;
  if (formApiKey.value && formClearApiKey.value) {
    ElMessage.error("Choose either a new key or clear — not both.");
    return;
  }
  // Auto-commit a pending typed-but-unconfirmed model so we don't silently
  // drop the user's last entry on Save.
  if (formNewModel.value.trim()) {
    handleAddModel();
  }

  const existingModels = editingProvider.value.credential?.models ?? null;
  const hadModels = existingModels !== null && existingModels.length > 0;
  // null = "leave the curated list alone" (matches apiKey=null semantics).
  // [] would be the same thing on the wire (collapses to NULL backend-side),
  // but we send clearModels=true explicitly when the user emptied a list they
  // previously had, so it reads naturally in the audit / network log.
  let modelsField: string[] | null = null;
  let clearModels = false;
  if (formModels.value.length > 0) {
    modelsField = [...formModels.value];
  } else if (hadModels) {
    clearModels = true;
  }

  const payload: AiProviderCredentialInput = {
    apiKey: formApiKey.value ? formApiKey.value : null,
    clearApiKey: formClearApiKey.value,
    apiBase: formApiBase.value ? formApiBase.value : null,
    defaultModel: formDefaultModel.value ? formDefaultModel.value : null,
    models: modelsField,
    clearModels,
  };

  isSubmitting.value = true;
  busyProvider.value = editingProvider.value.provider;
  try {
    await upsertAiProvider(editingProvider.value.provider, payload);
    await loadProviders();
    dialogVisible.value = false;
    ElMessage.success(`Saved ${editingProvider.value.provider}`);
  } catch (error) {
    if (error instanceof AiDisabledError) {
      isDisabled.value = true;
      dialogVisible.value = false;
    } else {
      ElMessage.error((error as Error).message || "Failed to save provider");
    }
  } finally {
    isSubmitting.value = false;
    busyProvider.value = null;
  }
};

const handleDelete = async () => {
  if (!providerToDelete.value) return;
  isDeleting.value = true;
  busyProvider.value = providerToDelete.value.provider;
  try {
    await deleteAiProvider(providerToDelete.value.provider);
    await loadProviders();
    deleteDialogVisible.value = false;
    ElMessage.success(`Removed ${providerToDelete.value.provider}`);
  } catch (error) {
    if (error instanceof AiDisabledError) {
      isDisabled.value = true;
      deleteDialogVisible.value = false;
    } else {
      ElMessage.error("Failed to delete provider");
    }
  } finally {
    isDeleting.value = false;
    busyProvider.value = null;
    providerToDelete.value = null;
  }
};

const handleTest = async (provider: AiProvider) => {
  busyProvider.value = provider.provider;
  try {
    const result = await testAiProvider(provider.provider);
    if (result.ok) {
      ElMessage.success(`${provider.provider}: connection ok`);
    } else {
      ElMessage.error(`${provider.provider}: ${result.error ?? "test failed"}`);
    }
    await loadProviders();
  } catch (error) {
    if (error instanceof AiDisabledError) {
      isDisabled.value = true;
    } else {
      ElMessage.error(`Failed to test ${provider.provider}`);
    }
  } finally {
    busyProvider.value = null;
  }
};

const handleEnableFlag = async () => {
  // Admin-only path. Backend rejects with 403 for non-admin, 401 for
  // unauth — the UI gates on isAdmin first so we never make those
  // calls.
  isEnablingFlag.value = true;
  try {
    const state = await setAiFeatureFlag(true);
    if (state.enabled) {
      ElMessage.success("AI features enabled for this process");
      // Reload providers — the previous attempt 503'd; this one should populate.
      await loadProviders();
    } else {
      ElMessage.error("Failed to enable AI features");
    }
  } catch (error) {
    ElMessage.error((error as Error).message || "Failed to enable AI features");
  } finally {
    isEnablingFlag.value = false;
  }
};

const handleCloseDialog = (done: () => void) => {
  if (isSubmitting.value) return;
  done();
};

const handleCloseDeleteDialog = (done: () => void) => {
  if (isDeleting.value) return;
  done();
};

// --- Local model (offline llama.cpp + small GGUF) ---
const localModel = ref<LocalModelStatus | null>(null);
const localLoading = ref(true);
const localBusy = ref(false);
const installing = ref(false);
const installPhase = ref("");
const installPct = ref<number | null>(null);

const localBadgeLabel = computed(() => {
  if (!localModel.value) return "";
  if (localModel.value.running) return "running";
  if (localModel.value.installed) return "installed";
  return "not installed";
});

const localBadgeClass = computed(() => {
  if (localModel.value?.running) return "badge--configured";
  if (localModel.value?.installed) return "badge--env_fallback";
  return "badge--unconfigured";
});

const installPhaseLabel = computed(() => {
  switch (installPhase.value) {
    case "downloading_binary":
      return "Downloading runtime…";
    case "extracting":
      return "Extracting…";
    case "downloading_model":
      return "Downloading model (~1 GB)…";
    case "verifying":
      return "Verifying…";
    case "done":
      return "Done";
    default:
      return "Installing…";
  }
});

const loadLocalModel = async () => {
  localLoading.value = true;
  try {
    localModel.value = await fetchLocalModelStatus();
  } catch (error) {
    if (error instanceof AiDisabledError) {
      isDisabled.value = true;
    }
    // Otherwise leave the card hidden — a missing local-model status is non-fatal.
  } finally {
    localLoading.value = false;
  }
};

const handleLocalInstall = async () => {
  installing.value = true;
  localBusy.value = true;
  installPhase.value = "";
  installPct.value = null;
  let failed: string | null = null;
  try {
    await streamLocalModelInstall({
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
    });
  } catch (error) {
    failed = (error as Error).message || "Install failed";
  } finally {
    installing.value = false;
    localBusy.value = false;
    await loadLocalModel();
  }
  if (failed) {
    ElMessage.error(`Install failed: ${failed}`);
  } else if (localModel.value?.installed) {
    ElMessage.success("Local model installed");
  }
};

const handleLocalStart = async () => {
  localBusy.value = true;
  try {
    localModel.value = await startLocalModel();
    ElMessage.success("Local model started");
  } catch (error) {
    ElMessage.error((error as Error).message || "Failed to start local model");
  } finally {
    localBusy.value = false;
  }
};

const handleLocalStop = async () => {
  localBusy.value = true;
  try {
    localModel.value = await stopLocalModel();
    ElMessage.success("Local model stopped");
  } catch {
    ElMessage.error("Failed to stop local model");
  } finally {
    localBusy.value = false;
  }
};

const handleLocalDelete = async () => {
  localBusy.value = true;
  try {
    localModel.value = await deleteLocalModel();
    ElMessage.success("Local model removed");
  } catch {
    ElMessage.error("Failed to remove local model");
  } finally {
    localBusy.value = false;
  }
};

// Render-time use to avoid the "unused" complaint when we expose AI_DISABLED_DETAIL
// only through the template.
const _ai_disabled_detail = computed(() => AI_DISABLED_DETAIL);
void _ai_disabled_detail.value;

onMounted(() => {
  void loadProviders();
  void loadLocalModel();
});
</script>

<style scoped>
.ai-providers-container {
  padding: 0;
}

.description-text {
  color: var(--color-text-secondary);
  margin-top: var(--spacing-2);
  font-size: var(--font-size-sm);
}

.connections-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.connection-item {
  display: flex;
  align-items: center;
  padding: var(--spacing-3) var(--spacing-4);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  gap: var(--spacing-4);
}

.connection-info {
  flex: 1;
  min-width: 0;
}

.connection-name {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.connection-name i {
  color: var(--color-accent);
}

.connection-details {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  margin-top: var(--spacing-1);
}

.muted {
  color: var(--color-text-tertiary);
}

.separator {
  margin: 0 var(--spacing-2);
}

.connection-actions {
  flex-shrink: 0;
  display: flex;
  gap: var(--spacing-2);
}

.badge {
  background-color: var(--color-background-muted);
  color: var(--color-text-secondary);
  border-radius: var(--border-radius-full);
  padding: var(--spacing-1) var(--spacing-3);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
}

.badge--configured {
  background-color: var(--color-success-subtle, rgba(34, 197, 94, 0.15));
  color: var(--color-success, #16a34a);
}

.badge--env_fallback {
  background-color: var(--color-warning-subtle, rgba(234, 179, 8, 0.15));
  color: var(--color-warning, #ca8a04);
}

.badge--unconfigured {
  background-color: var(--color-background-muted);
  color: var(--color-text-tertiary);
}

.badge--test.badge--ok {
  background-color: var(--color-success-subtle, rgba(34, 197, 94, 0.15));
  color: var(--color-success, #16a34a);
}

.badge--test.badge--error {
  background-color: var(--color-danger-subtle, rgba(239, 68, 68, 0.15));
  color: var(--color-danger, #dc2626);
}

.info-box {
  display: flex;
  gap: var(--spacing-4);
  padding: var(--spacing-4);
  background-color: var(--color-background-muted);
  border-left: 4px solid var(--color-accent);
  border-radius: var(--border-radius-md);
}

.info-box i {
  color: var(--color-accent);
  font-size: var(--font-size-2xl);
  margin-top: var(--spacing-1);
}

.info-body {
  flex: 1;
}

.info-body p {
  margin: 0;
  margin-bottom: var(--spacing-2);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.info-body p:last-child {
  margin-bottom: 0;
}

.info-box strong {
  color: var(--color-text-primary);
}

.info-actions {
  margin: var(--spacing-3) 0;
}

.hint-text {
  color: var(--color-text-tertiary);
  font-size: var(--font-size-xs);
  margin-top: var(--spacing-1);
}

.warning-text {
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  margin-top: var(--spacing-2);
}

.password-field {
  display: flex;
  align-items: center;
  position: relative;
}

.password-field .form-input {
  flex: 1;
  padding-right: 2.5rem;
}

.toggle-visibility {
  position: absolute;
  right: var(--spacing-2);
  background: transparent;
  border: none;
  cursor: pointer;
  color: var(--color-text-tertiary);
}

.checkbox-label {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.chip-input {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-2);
  padding: var(--spacing-2);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-primary);
  min-height: 38px;
  align-items: center;
}

.chip-input__tag {
  margin: 0;
}

.chip-input__input {
  flex: 1;
  min-width: 200px;
  border: none;
  outline: none;
  background: transparent;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  padding: 4px 6px;
}

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-2);
}

/* Chat → agent auto-promotion toggle row. */
.behavior-row {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  cursor: pointer;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

.behavior-row input[type="checkbox"] {
  margin-top: 4px;
  cursor: pointer;
}

.behavior-row__label {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}

.behavior-row__hint {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  font-weight: normal;
}

.local-progress {
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
</style>
