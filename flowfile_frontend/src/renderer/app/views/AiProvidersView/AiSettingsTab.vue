<template>
  <div class="ai-providers-container">
    <div class="mb-3">
      <h2 class="page-title">AI Providers</h2>
      <p class="description-text">
        Bring your own keys for the LLM providers Flowfile's AI features use. Keys are encrypted at
        rest using the same Fernet pipeline as your other secrets. Local Ollama needs no key.
      </p>
    </div>

    <!-- Disabled-state: matches W17's 503 contract -->
    <div v-if="isDisabled" class="card mb-3">
      <div class="card-content">
        <div class="info-box">
          <i class="fa-solid fa-circle-info"></i>
          <div>
            <p><strong>AI features are off</strong></p>
            <p>{{ AI_DISABLED_DETAIL }}</p>
            <p class="hint-text">
              AI ships off by default during the Phase 0 rollout. To enable: add
              <code>FEATURE_FLAG_AI=true</code> to your <code>.env</code> file (Flowfile project
              root for local dev, or your container env for Docker), then restart
              <code>flowfile_core</code>.
            </p>
          </div>
        </div>
      </div>
    </div>

    <div v-else class="card mb-3">
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
import { ElButton, ElDialog, ElMessage } from "element-plus";
import {
  AiDisabledError,
  AI_DISABLED_DETAIL,
  deleteAiProvider,
  fetchAiProviders,
  testAiProvider,
  upsertAiProvider,
} from "./api";
import type { AiProvider, AiProviderCredentialInput, AiProviderStatus } from "./aiProviderTypes";

const providers = ref<AiProvider[]>([]);
const isLoading = ref(true);
const isDisabled = ref(false);
const busyProvider = ref<string | null>(null);

// Edit dialog
const dialogVisible = ref(false);
const editingProvider = ref<AiProvider | null>(null);
const formApiKey = ref("");
const formClearApiKey = ref(false);
const formDefaultModel = ref("");
const formApiBase = ref("");
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
  showApiKey.value = false;
  dialogVisible.value = true;
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

  const payload: AiProviderCredentialInput = {
    apiKey: formApiKey.value ? formApiKey.value : null,
    clearApiKey: formClearApiKey.value,
    apiBase: formApiBase.value ? formApiBase.value : null,
    defaultModel: formDefaultModel.value ? formDefaultModel.value : null,
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

const handleCloseDialog = (done: () => void) => {
  if (isSubmitting.value) return;
  done();
};

const handleCloseDeleteDialog = (done: () => void) => {
  if (isDeleting.value) return;
  done();
};

// Render-time use to avoid the "unused" complaint when we expose AI_DISABLED_DETAIL
// only through the template.
const _ai_disabled_detail = computed(() => AI_DISABLED_DETAIL);
void _ai_disabled_detail.value;

onMounted(loadProviders);
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

.info-box p {
  margin: 0;
  margin-bottom: var(--spacing-2);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.info-box p:last-child {
  margin-bottom: 0;
}

.info-box strong {
  color: var(--color-text-primary);
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

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-2);
}
</style>
