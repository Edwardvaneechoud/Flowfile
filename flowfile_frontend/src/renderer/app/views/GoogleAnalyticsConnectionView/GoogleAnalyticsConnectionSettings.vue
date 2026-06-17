<template>
  <el-form :model="form" label-position="top" @submit.prevent>
    <el-form-item label="Connection Name" required>
      <el-input
        v-model="form.connectionName"
        placeholder="e.g. marketing-ga4"
        :disabled="isEditing"
      />
      <div class="hint-text">A unique name used to reference this connection from nodes.</div>
    </el-form-item>

    <el-form-item label="Authentication method">
      <el-radio-group v-model="form.authMethod">
        <el-radio-button value="service_account">Service account</el-radio-button>
        <el-radio-button value="oauth">Sign in with Google</el-radio-button>
      </el-radio-group>
      <div class="hint-text">
        <strong>Service account</strong> is recommended for unattended and scheduled flows — it runs
        headless and never needs a token refresh. <strong>Sign in with Google</strong> (OAuth) links
        a specific Google user for per-analyst access.
      </div>
    </el-form-item>

    <el-form-item label="Description">
      <el-input v-model="form.description" placeholder="Optional description" />
    </el-form-item>

    <el-form-item label="Default GA4 Property ID">
      <el-input v-model="form.defaultPropertyId" placeholder="e.g. 123456789" />
      <div class="hint-text">Optional default property ID. Nodes can override this value.</div>
    </el-form-item>

    <el-form-item
      v-if="form.authMethod === 'service_account'"
      :required="!isEditing || initialConnection?.authMethod !== 'service_account'"
    >
      <template #label>
        <span class="field-label-row">
          <span>Service Account Key (JSON)</span>
          <el-popover
            placement="bottom-end"
            :width="468"
            trigger="click"
            popper-class="sa-help-popover"
          >
            <template #reference>
              <button type="button" class="field-help-trigger">
                <i class="fa-solid fa-circle-question"></i>
                How do I create one?
              </button>
            </template>
            <div class="sa-help">
              <p class="sa-help-title">How to create a service account key</p>
              <p class="sa-help-intro">
                A service account is a "robot" Google login that Flowfile uses to read your reports.
                No coding — about 5 minutes in Google's websites.
              </p>
              <ol class="sa-help-steps">
                <li>
                  <strong>Create the robot account.</strong> Open
                  <SetupLink href="https://console.cloud.google.com/iam-admin/serviceaccounts">
                    Service Accounts
                  </SetupLink>
                  and click <em>Create service account</em> (or reuse one). Give it a name and click
                  <em>Done</em>. It gets its own email address.
                </li>
                <li>
                  <strong>Let it read Analytics.</strong> Open the
                  <SetupLink
                    href="https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com"
                  >
                    Analytics Data API
                  </SetupLink>
                  page and click <em>Enable</em>.
                  <span class="sa-help-note">
                    <i class="fa-solid fa-circle-check"></i>
                    How to tell it's on: the blue <em>Enable</em> button turns into a
                    <em>Manage</em> button (the page header reads "API enabled"). If you still see
                    <em>Enable</em>, click it and wait a few seconds.
                  </span>
                </li>
                <li>
                  <strong>Download the key file.</strong> Open the service account you made, go to
                  its <em>Keys</em> tab, and choose <em>Add key → Create new key → JSON</em>. A
                  small <code>.json</code> file downloads — keep it safe, it works like a password.
                </li>
                <li>
                  <strong>Give it access to your data.</strong> In
                  <SetupLink href="https://analytics.google.com/analytics/web/">
                    Google Analytics → Admin → Property access management
                  </SetupLink>
                  , click <em>+ → Add users</em>, paste the robot account's email address, and pick
                  the <strong>Viewer</strong> role — exactly like inviting a colleague.
                </li>
                <li>
                  <strong>Paste the key here.</strong> Open the downloaded <code>.json</code> file
                  in any text editor (Notepad, TextEdit…), select all, copy, and paste it into the
                  box below.
                </li>
              </ol>
              <p class="sa-help-tip">
                <i class="fa-solid fa-lightbulb"></i>
                <span>
                  <strong>Which email do I add in step 4?</strong> Open the downloaded file — one
                  line reads <code>"client_email": "…@….iam.gserviceaccount.com"</code>. That
                  address is the robot account's identity; copy it and give <em>that</em> Viewer
                  access in Google Analytics.
                </span>
              </p>
              <button type="button" class="sa-help-wizard-link" @click="emit('open-wizard')">
                <i class="fa-solid fa-wand-magic-sparkles"></i>
                Prefer step-by-step? Use guided setup
              </button>
            </div>
          </el-popover>
        </span>
      </template>
      <el-input
        v-model="form.serviceAccountKey"
        type="textarea"
        :rows="5"
        :placeholder="
          isEditing && initialConnection?.authMethod === 'service_account'
            ? 'Leave blank to keep the existing key'
            : 'Paste your service account JSON key here'
        "
      />
      <div class="hint-text">
        Grant the service account's email <strong>Viewer</strong> on your GA4 property, then paste
        its JSON key. The key is encrypted at rest with your user-derived key and is never returned
        to the browser.
        <span
          v-if="
            isEditing &&
            initialConnection?.authMethod === 'service_account' &&
            initialConnection?.oauthUserEmail
          "
        >
          Current service account: {{ initialConnection.oauthUserEmail }}
        </span>
      </div>
    </el-form-item>

    <el-form-item v-else label="Google Account">
      <!-- OAuth isn't set up on this instance: block the connect and point the
           user to where they can fix it (or to the zero-setup alternative). -->
      <div v-if="!oauthConfigured" class="oauth-warning">
        <div class="oauth-warning-head">
          <i class="fa-solid fa-triangle-exclamation"></i>
          <span>Google sign-in isn't set up on this instance yet</span>
        </div>
        <p class="oauth-warning-body">
          <template v-if="isEditing && initialConnection?.oauthUserEmail">
            This connection is currently linked as
            <strong>{{ initialConnection.oauthUserEmail }}</strong
            >, but reconnecting needs a Google OAuth client.
          </template>
          <template v-else>
            The <em>Sign in with Google</em> method needs a one-time Google OAuth client.
          </template>
          Add yours in the <strong>Google OAuth</strong> panel on this page — or switch to a
          <strong>service account</strong>, which needs no setup at all.
        </p>
        <div class="oauth-warning-actions">
          <el-button size="small" type="primary" @click="emit('setup-oauth')">
            <i class="fa-brands fa-google"></i>&nbsp;Set up Google OAuth
          </el-button>
          <el-button size="small" @click="form.authMethod = 'service_account'">
            Use a service account instead
          </el-button>
        </div>
      </div>

      <template v-else>
        <div
          v-if="
            isEditing &&
            initialConnection?.authMethod === 'oauth' &&
            initialConnection?.oauthUserEmail
          "
          class="connected-row"
        >
          <span class="connected-pill">
            <i class="fa-solid fa-circle-check"></i>
            Connected as {{ initialConnection.oauthUserEmail }}
          </span>
          <el-button size="small" :loading="isConnecting" @click="handleConnect">
            <i class="fa-solid fa-rotate-right"></i>&nbsp;Reconnect
          </el-button>
        </div>
        <div v-else>
          <el-button
            type="primary"
            :loading="isConnecting"
            :disabled="!form.connectionName.trim()"
            @click="handleConnect"
          >
            <i class="fa-brands fa-google"></i>&nbsp;Connect Google Account
          </el-button>
          <div class="hint-text">
            Opens Google sign-in in a new window. Flowfile stores a refresh token (encrypted at
            rest) so it can read GA4 data on your behalf.
          </div>
        </div>
      </template>
    </el-form-item>

    <div class="form-actions">
      <el-button @click="$emit('cancel')">Cancel</el-button>
      <el-button
        v-if="form.authMethod === 'service_account'"
        type="primary"
        :loading="isSubmitting"
        :disabled="!form.connectionName.trim()"
        @click="handleSaveServiceAccount"
      >
        Save connection
      </el-button>
      <el-button
        v-else-if="isEditing && initialConnection?.authMethod === 'oauth'"
        type="primary"
        :loading="isSubmitting"
        @click="handleSaveMetadata"
      >
        Save
      </el-button>
    </div>
  </el-form>
</template>

<script setup lang="ts">
import { reactive, watch } from "vue";
import {
  ElButton,
  ElForm,
  ElFormItem,
  ElInput,
  ElMessage,
  ElPopover,
  ElRadioButton,
  ElRadioGroup,
} from "element-plus";
import SetupLink from "./SetupLink.vue";
import type {
  GoogleAnalyticsAuthMethod,
  GoogleAnalyticsConnectionInterface,
  GoogleAnalyticsConnectionMetadata,
  GoogleAnalyticsServiceAccountInput,
} from "./GoogleAnalyticsConnectionTypes";

const props = defineProps<{
  initialConnection?: GoogleAnalyticsConnectionInterface;
  isEditing: boolean;
  isSubmitting: boolean;
  isConnecting: boolean;
  // Whether a Google OAuth client is configured on this instance. When false,
  // the OAuth sign-in is blocked and the user is pointed to the OAuth panel.
  oauthConfigured: boolean;
}>();

const emit = defineEmits<{
  (e: "save-metadata", metadata: GoogleAnalyticsConnectionMetadata): void;
  (e: "save-service-account", input: GoogleAnalyticsServiceAccountInput): void;
  (e: "connect-oauth", metadata: GoogleAnalyticsConnectionMetadata): void;
  (e: "setup-oauth"): void;
  (e: "open-wizard"): void;
  (e: "cancel"): void;
}>();

interface GaConnectionForm {
  connectionName: string;
  description?: string | null;
  defaultPropertyId?: string | null;
  authMethod: GoogleAnalyticsAuthMethod;
  serviceAccountKey: string;
}

// New connections default to service account — the recommended headless path.
const form = reactive<GaConnectionForm>({
  connectionName: "",
  description: "",
  defaultPropertyId: "",
  authMethod: "service_account",
  serviceAccountKey: "",
});

const metadata = (): GoogleAnalyticsConnectionMetadata => ({
  connectionName: form.connectionName,
  description: form.description,
  defaultPropertyId: form.defaultPropertyId,
});

watch(
  () => props.initialConnection,
  (value) => {
    form.connectionName = value?.connectionName ?? "";
    form.description = value?.description ?? "";
    form.defaultPropertyId = value?.defaultPropertyId ?? "";
    form.authMethod = value?.authMethod ?? "service_account";
    form.serviceAccountKey = "";
  },
  { immediate: true },
);

function handleConnect() {
  if (!form.connectionName.trim()) {
    ElMessage.error("Connection name is required");
    return;
  }
  emit("connect-oauth", metadata());
}

function handleSaveMetadata() {
  if (!form.connectionName.trim()) {
    ElMessage.error("Connection name is required");
    return;
  }
  emit("save-metadata", metadata());
}

function handleSaveServiceAccount() {
  if (!form.connectionName.trim()) {
    ElMessage.error("Connection name is required");
    return;
  }
  const key = form.serviceAccountKey.trim();
  const editingExistingSa =
    props.isEditing && props.initialConnection?.authMethod === "service_account";
  if (!key) {
    if (editingExistingSa) {
      // Blank on an existing service-account connection: keep the stored key,
      // just update the metadata.
      emit("save-metadata", metadata());
      return;
    }
    ElMessage.error("Paste the service account JSON key");
    return;
  }
  emit("save-service-account", { ...metadata(), serviceAccountKey: key });
}
</script>

<style scoped>
.hint-text {
  color: var(--color-text-tertiary);
  font-size: var(--font-size-xs);
  margin-top: var(--spacing-1);
}

.connected-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  flex-wrap: wrap;
}

.connected-pill {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-3);
  background: var(--color-accent-subtle, #ebf4ff);
  color: var(--color-accent, #2b6cb0);
  border-radius: var(--border-radius-full, 9999px);
  font-size: var(--font-size-sm);
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-2);
  margin-top: var(--spacing-4);
}

.field-label-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-3);
  width: 100%;
}

.field-help-trigger {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: 0;
  background: transparent;
  border: none;
  color: var(--color-accent);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  white-space: nowrap;
}

.field-help-trigger:hover {
  text-decoration: underline;
  text-underline-offset: 2px;
}

.sa-help {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  max-height: min(62vh, 560px);
  overflow-y: auto;
}

.sa-help-title {
  margin: 0 0 var(--spacing-1);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.sa-help-intro {
  margin: 0 0 var(--spacing-3);
  color: var(--color-text-tertiary);
  font-size: var(--font-size-xs);
  line-height: 1.45;
}

.sa-help-steps {
  margin: 0;
  padding-left: var(--spacing-5);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
}

.sa-help-steps li {
  line-height: 1.5;
}

.sa-help-note {
  display: block;
  margin-top: var(--spacing-1);
  padding-left: var(--spacing-3);
  border-left: 2px solid var(--color-success, #10b981);
  color: var(--color-text-tertiary);
  font-size: var(--font-size-xs);
  line-height: 1.45;
}

.sa-help-note i {
  color: var(--color-success, #10b981);
  margin-right: 4px;
}

.sa-help code {
  background: var(--color-background-muted);
  padding: 1px 5px;
  border-radius: 4px;
  font-size: 0.85em;
  word-break: break-word;
}

.sa-help-tip {
  display: flex;
  gap: var(--spacing-2);
  margin: var(--spacing-3) 0 0;
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-background-muted);
  border-left: 3px solid var(--color-accent);
  border-radius: var(--border-radius-sm);
  font-size: var(--font-size-xs);
  line-height: 1.45;
}

.sa-help-tip > i {
  color: var(--color-accent);
  margin-top: 2px;
}

.sa-help-wizard-link {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  margin-top: var(--spacing-3);
  padding: 0;
  background: transparent;
  border: none;
  color: var(--color-accent);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
}

.sa-help-wizard-link:hover {
  text-decoration: underline;
  text-underline-offset: 2px;
}

/* OAuth-not-configured callout */
.oauth-warning {
  padding: var(--spacing-3);
  background: var(--color-warning-light, #fef3c7);
  border: 1px solid var(--color-warning, #f59e0b);
  border-radius: var(--border-radius-md, 8px);
}

.oauth-warning-head {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
}

.oauth-warning-head i {
  color: var(--color-warning, #f59e0b);
}

.oauth-warning-body {
  margin: var(--spacing-2) 0 var(--spacing-3);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  line-height: 1.5;
}

.oauth-warning-actions {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-2);
}
</style>
