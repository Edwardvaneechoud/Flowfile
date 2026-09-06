<template>
  <div>
    <div class="mb-3">
      <h2 class="page-title">Providers</h2>
      <p class="page-description">
        Bring your own keys for the LLM providers Flowfile's AI features use. Keys are stored
        encrypted with your other secrets; a local Ollama server needs no key.
      </p>
    </div>

    <div class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Providers ({{ providers.length + 1 }})</h3>
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
          <!-- The on-device model is a provider too — the same "On-device AI"
               the model pickers offer — so it sits in this list, first, as the
               no-key way in. -->
          <OnDeviceProviderRow />
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
            The models offered by the pickers on the Assistant tab and in the chat drawer. Empty =
            only the default model is offered.
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
import { computed, ref } from "vue";
import { ElButton, ElDialog, ElMessage, ElTag } from "element-plus";
import { useAiStore } from "../../stores/ai-store";
import OnDeviceProviderRow from "./OnDeviceProviderRow.vue";
import { AiDisabledError, deleteAiProvider, testAiProvider, upsertAiProvider } from "./api";
import type { AiProvider, AiProviderCredentialInput, AiProviderStatus } from "./aiProviderTypes";
import { LOCAL_PROVIDER_ID } from "./localModelApi";

// The store's provider list is the single fetch for every AI tab; this
// panel only hides the synthetic on-device entry (it has no credential).
const aiStore = useAiStore();
const providers = computed(() => aiStore.providers.filter((p) => p.provider !== LOCAL_PROVIDER_ID));
const isLoading = computed(() => aiStore.providersLoading);
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

// Every mutation re-pulls through the store so the Assistant tab's pickers
// and the chat drawer see the change; a 503 mid-session flips the view's
// disabled card through the same path.
const reload = () => aiStore.loadProviders();

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
    await reload();
    dialogVisible.value = false;
    ElMessage.success(`Saved ${editingProvider.value.provider}`);
  } catch (error) {
    if (error instanceof AiDisabledError) {
      dialogVisible.value = false;
      await reload();
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
    await reload();
    deleteDialogVisible.value = false;
    ElMessage.success(`Removed ${providerToDelete.value.provider}`);
  } catch (error) {
    if (error instanceof AiDisabledError) {
      deleteDialogVisible.value = false;
      await reload();
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
    await reload();
  } catch (error) {
    if (error instanceof AiDisabledError) {
      await reload();
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
</script>

<style scoped>
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

/* The dialog teleports out of .ai-settings-view, so its hints are styled here. */
.hint-text {
  color: var(--color-text-tertiary);
  font-size: var(--font-size-xs);
  margin-top: var(--spacing-1);
}
</style>
