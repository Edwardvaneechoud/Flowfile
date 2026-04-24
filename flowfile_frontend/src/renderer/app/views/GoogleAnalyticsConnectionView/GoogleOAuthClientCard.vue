<template>
  <div class="card mb-3">
    <button type="button" class="card-header card-header-button" @click="expanded = !expanded">
      <h3 class="card-title">
        <i class="fa-brands fa-google"></i>&nbsp;Google OAuth
        <span v-if="isConfigured" class="badge badge-success">Configured</span>
        <span v-else class="badge badge-warning">Not configured</span>
      </h3>
      <i
        class="fa-solid chevron"
        :class="expanded ? 'fa-chevron-up' : 'fa-chevron-down'"
        aria-hidden="true"
      ></i>
    </button>
    <div v-if="expanded" class="card-content">
      <p class="section-description">
        OAuth client used by the Google Analytics connector. Create a
        <strong>Web application</strong> OAuth 2.0 client at
        <a href="https://console.cloud.google.com/apis/credentials" target="_blank" rel="noopener"
          >console.cloud.google.com/apis/credentials</a
        >
        and paste the values here.
      </p>

      <form class="form" @submit.prevent="handleSave">
        <div class="form-field">
          <label for="goauth-client-id" class="form-label">Client ID</label>
          <input
            id="goauth-client-id"
            v-model="form.clientId"
            type="text"
            class="form-input"
            placeholder="xxxxxxxxxxxx.apps.googleusercontent.com"
            required
          />
        </div>

        <div class="form-field">
          <label for="goauth-client-secret" class="form-label">
            Client Secret
            <span v-if="isConfigured" class="label-hint">
              (leave blank to keep the stored value)
            </span>
          </label>
          <div class="password-field">
            <input
              id="goauth-client-secret"
              v-model="form.clientSecret"
              :type="showSecret ? 'text' : 'password'"
              class="form-input"
              placeholder="GOCSPX-..."
              :required="!isConfigured"
            />
            <button
              type="button"
              class="toggle-visibility"
              aria-label="Toggle client secret visibility"
              @click="showSecret = !showSecret"
            >
              <i :class="showSecret ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
            </button>
          </div>
        </div>

        <div class="form-field">
          <label for="goauth-redirect" class="form-label">Redirect URI</label>
          <input
            id="goauth-redirect"
            v-model="form.redirectUri"
            type="text"
            class="form-input"
            placeholder="http://localhost:63578/ga_connections/oauth/callback"
            required
          />
          <p class="hint-text">
            Must be added verbatim to your OAuth client's
            <em>Authorised redirect URIs</em> in the Google Cloud console.
          </p>
        </div>

        <div class="form-actions">
          <button
            type="button"
            class="btn btn-secondary"
            :disabled="!isConfigured || isSaving"
            @click="handleClear"
          >
            Clear
          </button>
          <button type="submit" class="btn btn-primary" :disabled="isSaving">
            <i v-if="isSaving" class="fas fa-spinner fa-spin"></i>
            {{ isSaving ? "Saving..." : "Save" }}
          </button>
        </div>
      </form>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from "vue";
import { ElMessage } from "element-plus";
import {
  clearGoogleOAuthConfig,
  fetchGoogleOAuthConfig,
  saveGoogleOAuthConfig,
} from "./oauthClientApi";

const form = reactive({
  clientId: "",
  clientSecret: "",
  redirectUri: "http://localhost:63578/ga_connections/oauth/callback",
});

const isConfigured = ref(false);
const isSaving = ref(false);
const showSecret = ref(false);
// Default-collapsed once configured so it stops taking up space, expanded
// on first load so new users see the form they need to fill.
const expanded = ref(true);

const loadConfig = async () => {
  try {
    const cfg = await fetchGoogleOAuthConfig();
    form.clientId = cfg.clientId;
    if (cfg.redirectUri) form.redirectUri = cfg.redirectUri;
    isConfigured.value = cfg.isConfigured;
    expanded.value = !cfg.isConfigured;
  } catch (error) {
    console.error("Failed to load Google OAuth config", error);
  }
};

const handleSave = async () => {
  if (!isConfigured.value && !form.clientSecret.trim()) {
    ElMessage.error("Client secret is required the first time you save");
    return;
  }
  isSaving.value = true;
  try {
    await saveGoogleOAuthConfig({
      clientId: form.clientId.trim(),
      clientSecret: form.clientSecret.trim(),
      redirectUri: form.redirectUri.trim(),
    });
    form.clientSecret = "";
    await loadConfig();
    ElMessage.success("Google OAuth config saved");
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    ElMessage.error(`Failed to save: ${message}`);
  } finally {
    isSaving.value = false;
  }
};

const handleClear = async () => {
  if (
    !window.confirm(
      "Clear the stored Google OAuth client? Existing flows will fail until it is reconfigured.",
    )
  )
    return;
  isSaving.value = true;
  try {
    await clearGoogleOAuthConfig();
    form.clientId = "";
    form.clientSecret = "";
    await loadConfig();
    ElMessage.success("Google OAuth config cleared");
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    ElMessage.error(`Failed to clear: ${message}`);
  } finally {
    isSaving.value = false;
  }
};

onMounted(loadConfig);
</script>

<style scoped>
.card-header-button {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  background: transparent;
  border: none;
  cursor: pointer;
  padding: var(--spacing-3) var(--spacing-4);
  text-align: left;
}

.card-header-button:hover {
  background: var(--color-background-muted, #f7fafc);
}

.chevron {
  color: var(--color-text-tertiary);
  font-size: 0.875rem;
}

.section-description {
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  margin-bottom: var(--spacing-3);
}

.badge {
  margin-left: var(--spacing-2);
  padding: 2px 8px;
  border-radius: 9999px;
  font-size: 0.75rem;
  font-weight: 500;
}

.badge-success {
  background: #d1fae5;
  color: #065f46;
}

.badge-warning {
  background: #fef3c7;
  color: #92400e;
}

.label-hint {
  color: var(--color-text-tertiary);
  font-size: 0.75rem;
  font-weight: 400;
}

.hint-text {
  color: var(--color-text-tertiary);
  font-size: 0.75rem;
  margin-top: var(--spacing-1);
}

.password-field {
  position: relative;
}

.toggle-visibility {
  position: absolute;
  right: 0.5rem;
  top: 50%;
  transform: translateY(-50%);
  background: transparent;
  border: none;
  cursor: pointer;
  padding: 0.25rem;
  color: var(--color-text-tertiary);
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: 0.5rem;
  margin-top: 1rem;
}
</style>
