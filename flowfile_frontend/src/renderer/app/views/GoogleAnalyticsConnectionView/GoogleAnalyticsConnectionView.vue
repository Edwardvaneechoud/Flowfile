<template>
  <div class="ga-connection-manager-container">
    <div class="mb-3">
      <div class="page-header">
        <h2 class="page-title">Google Analytics Connections</h2>
        <button type="button" class="btn btn-primary btn-sm" @click="openWizard">
          <i class="fa-solid fa-wand-magic-sparkles"></i>
          Set up a connection
        </button>
      </div>
      <p class="description-text">
        Connect Google Analytics with a <strong>service account</strong> (recommended for unattended
        and scheduled flows) or by <strong>signing in with Google</strong>. Credentials are
        encrypted at rest with your user-derived key and are never transmitted back to the browser
        after creation.
      </p>
    </div>

    <div ref="oauthCardRef">
      <GoogleOAuthClientCard
        @saved="refreshOAuthConfigured"
        @show-guide="setupGuideVisible = true"
      />
    </div>

    <div class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">
          Your Connections ({{ connections.length }})
          <el-popover
            placement="bottom-start"
            :width="360"
            trigger="click"
            popper-class="how-it-works-popover"
          >
            <template #reference>
              <button type="button" class="how-it-works-trigger" aria-label="How connections work">
                <i class="fa-solid fa-circle-info"></i>
              </button>
            </template>
            <div class="how-it-works-content">
              <p class="how-it-works-title">How it works</p>
              <p>
                Click <em>Add Connection</em> and choose an authentication method. A
                <strong>service account</strong> (recommended for scheduled flows) reads GA4
                headlessly — paste its JSON key and grant its email Viewer on your property.
                <strong>Sign in with Google</strong> links a specific user via OAuth instead. Either
                credential is encrypted at rest with your user-derived key.
              </p>
            </div>
          </el-popover>
        </h3>
        <button class="btn btn-primary" @click="showAddModal">
          <i class="fa-solid fa-plus"></i> Add Connection
        </button>
      </div>
      <div class="card-content">
        <div v-if="isLoading" class="loading-state">
          <div class="loading-spinner"></div>
          <p>Loading connections...</p>
        </div>

        <div v-else-if="connections.length === 0" class="empty-state">
          <i class="fa-solid fa-chart-line"></i>
          <p>You haven't added any Google Analytics connections yet</p>
          <p class="hint-text">New here? Let us walk you through it step by step.</p>
          <button type="button" class="btn btn-primary" @click="openWizard">
            <i class="fa-solid fa-wand-magic-sparkles"></i> Start guided setup
          </button>
        </div>

        <div v-else class="connections-list">
          <div
            v-for="connection in connections"
            :key="connection.connectionName"
            class="connection-item"
          >
            <div class="connection-info">
              <div class="connection-name">
                <i class="fa-solid fa-chart-line"></i>
                <span>{{ connection.connectionName }}</span>
                <span v-if="connection.defaultPropertyId" class="badge">
                  Property {{ connection.defaultPropertyId }}
                </span>
              </div>
              <div v-if="connection.oauthUserEmail" class="connection-details">
                {{
                  connection.authMethod === "service_account" ? "Service account:" : "Connected as"
                }}
                {{ connection.oauthUserEmail }}
              </div>
              <div v-if="connection.description" class="connection-details">
                {{ connection.description }}
              </div>
            </div>
            <div class="connection-actions">
              <button type="button" class="btn btn-secondary" @click="showEditModal(connection)">
                <i class="fa-solid fa-edit"></i>
                <span>Modify</span>
              </button>
              <button type="button" class="btn btn-danger" @click="showDeleteModal(connection)">
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
      :title="isEditing ? 'Edit Google Analytics Connection' : 'Add Google Analytics Connection'"
      width="640px"
      :before-close="handleCloseDialog"
    >
      <GoogleAnalyticsConnectionSettings
        :initial-connection="activeConnection"
        :is-editing="isEditing"
        :is-submitting="isSubmitting"
        :is-connecting="isConnecting"
        :oauth-configured="oauthConfigured"
        @save-metadata="handleMetadataSave"
        @save-service-account="handleSaveServiceAccount"
        @connect-oauth="handleConnectOAuth"
        @setup-oauth="handleSetupOAuth"
        @open-wizard="openWizardFromForm"
        @cancel="dialogVisible = false"
      />
    </el-dialog>

    <GoogleAnalyticsSetupWizard
      v-model="wizardVisible"
      :oauth-configured="oauthConfigured"
      :is-submitting="isSubmitting"
      :is-connecting="isConnecting"
      :redirect-uri="redirectUri"
      :connections="connections"
      @save-service-account="handleSaveServiceAccount"
      @connect-oauth="handleConnectOAuth"
      @refresh-oauth-configured="refreshOAuthConfigured"
      @open-oauth-guide="setupGuideVisible = true"
    />

    <el-dialog v-model="setupGuideVisible" title="Set up Google OAuth for Flowfile" width="720px">
      <div class="setup-guide">
        <p class="setup-intro">
          These steps configure the <strong>Sign in with Google</strong> (OAuth) method using your
          own Google OAuth client — about 5 minutes in the Google Cloud console. Prefer a hands-off
          setup? Use a <strong>service account</strong> instead: create one, download its JSON key,
          and grant its email Viewer on your GA4 property — no OAuth client needed.
        </p>

        <ol class="setup-steps">
          <li>
            <strong>Choose your Google Cloud project.</strong>
            <ul class="setup-substeps">
              <li>
                <strong>Already use Google Cloud?</strong>
                Open the
                <SetupLink href="https://console.cloud.google.com/projectselector2/home/dashboard">
                  project selector
                </SetupLink>
                and pick the project you want to use, then skip ahead to <strong>step 4</strong> to
                create the OAuth client. First confirm the Analytics Data API is enabled (step 2)
                and your OAuth consent screen is configured (step 3) — do those if you haven't yet.
              </li>
              <li>
                <strong>New to Google Cloud?</strong>
                <SetupLink href="https://console.cloud.google.com/projectcreate">
                  Create a project
                </SetupLink>
                , then continue with step 2 below.
              </li>
            </ul>
          </li>
          <li>
            <strong>Enable the Google Analytics Data API.</strong>
            Open
            <SetupLink
              href="https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com"
            >
              the Analytics Data API page
            </SetupLink>
            and click <em>Enable</em>.
          </li>
          <li>
            <strong>Set up the Google Auth Platform.</strong>
            Configure
            <SetupLink href="https://console.cloud.google.com/auth/branding">Branding</SetupLink>
            (app name + support email) and
            <SetupLink href="https://console.cloud.google.com/auth/audience">Audience</SetupLink>
            (Workspace org + colleagues only? choose <em>Internal</em>. Otherwise <em>External</em>,
            then <em>Publish app</em> → <em>In production</em>; a Testing app's sign-in expires
            after 7 days). If you've set this up before, skip ahead.
          </li>
          <li>
            <strong>Create the OAuth client.</strong>
            Go to
            <SetupLink href="https://console.cloud.google.com/auth/clients">
              Google Auth Platform → Clients
            </SetupLink>
            , click <em>Create client</em>, and pick <strong>Web application</strong>.
          </li>
          <li>
            <strong>Add the redirect URI.</strong>
            Under <em>Authorized redirect URIs</em>, add exactly:
            <div class="setup-code-row">
              <pre class="setup-code">{{ redirectUri }}</pre>
              <button
                type="button"
                class="setup-copy-btn"
                :aria-label="`Copy ${redirectUri}`"
                @click="copyRedirectUri"
              >
                <i class="fa-solid fa-copy"></i>
                Copy
              </button>
            </div>
            It must match verbatim — no trailing slash, no extra path.
          </li>
          <li>
            <strong>Copy the Client ID and Client Secret.</strong>
            Paste them into the <em>Google OAuth</em> card below, along with the same redirect URI,
            and click <em>Save</em>.
          </li>
          <li>
            <strong>Add a connection.</strong>
            Click <em>Add Connection</em>, name it, then <em>Connect Google Account</em>. Sign in
            with a Google account that has Viewer access to your GA4 property. Flowfile stores only
            the refresh token, encrypted with your user-derived key.
            <div class="setup-action">
              <button type="button" class="btn btn-primary btn-sm" @click="openAddFromGuide">
                <i class="fa-solid fa-plus"></i>
                Open Add Connection
              </button>
            </div>
          </li>
        </ol>

        <div class="setup-note">
          <i class="fa-solid fa-info-circle"></i>
          <div>
            <strong>Why your own OAuth client?</strong>
            GA4 data is account-scoped and Google requires the consent screen to belong to whoever
            is reading the data. Flowfile never sees your client secret after you save it — it's
            encrypted at rest with your master key.
          </div>
        </div>
      </div>
      <template #footer>
        <el-button type="primary" @click="setupGuideVisible = false">Got it</el-button>
      </template>
    </el-dialog>

    <el-dialog
      v-model="deleteDialogVisible"
      title="Delete Connection"
      width="400px"
      :before-close="handleCloseDeleteDialog"
    >
      <p>
        Are you sure you want to delete the connection
        <strong>{{ connectionToDelete?.connectionName }}</strong
        >?
      </p>
      <p class="warning-text">
        This action cannot be undone and may break any flows that use this connection.
      </p>
      <template #footer>
        <div class="dialog-footer">
          <el-button @click="deleteDialogVisible = false">Cancel</el-button>
          <el-button type="danger" :loading="isDeleting" @click="handleDeleteConnection">
            Delete
          </el-button>
        </div>
      </template>
    </el-dialog>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted, onBeforeUnmount, nextTick } from "vue";
import { useRoute, useRouter } from "vue-router";
import { ElDialog, ElButton, ElMessage, ElPopover } from "element-plus";
import {
  createGoogleAnalyticsServiceAccountConnection,
  deleteGoogleAnalyticsConnection,
  fetchGoogleAnalyticsConnections,
  startGoogleAnalyticsOAuth,
  updateGoogleAnalyticsConnectionMetadata,
} from "./api";
import { fetchGoogleOAuthConfig } from "./oauthClientApi";
import type {
  GoogleAnalyticsConnectionInterface,
  GoogleAnalyticsConnectionMetadata,
  GoogleAnalyticsServiceAccountInput,
} from "./GoogleAnalyticsConnectionTypes";
import GoogleAnalyticsConnectionSettings from "./GoogleAnalyticsConnectionSettings.vue";
import GoogleAnalyticsSetupWizard from "./GoogleAnalyticsSetupWizard.vue";
import GoogleOAuthClientCard from "./GoogleOAuthClientCard.vue";
import SetupLink from "./SetupLink.vue";
import { desktop, isDesktop } from "../../../lib/desktop";
import { copyToClipboard } from "../../utils/clipboardUtils";
import { gaOAuthCallbackUrl } from "../../../config/constants";

const route = useRoute();
const router = useRouter();

const connections = ref<GoogleAnalyticsConnectionInterface[]>([]);
const isLoading = ref(true);
const dialogVisible = ref(false);
const wizardVisible = ref(false);
const deleteDialogVisible = ref(false);
const setupGuideVisible = ref(false);
const redirectUri = gaOAuthCallbackUrl;
// Whether a Google OAuth client is configured (gates OAuth sign-in in the modal).
const oauthConfigured = ref(false);
const oauthCardRef = ref<HTMLElement | null>(null);

const refreshOAuthConfigured = async () => {
  try {
    oauthConfigured.value = (await fetchGoogleOAuthConfig()).isConfigured;
  } catch (error) {
    console.error("Failed to load Google OAuth config status:", error);
  }
};

// Close the dialog and scroll the Google OAuth panel into view so the user can
// set it up. The panel auto-expands when it isn't configured.
const handleSetupOAuth = () => {
  dialogVisible.value = false;
  nextTick(() => {
    oauthCardRef.value?.scrollIntoView({ behavior: "smooth", block: "start" });
  });
};

const copyRedirectUri = async () => {
  if (await copyToClipboard(redirectUri)) {
    ElMessage.success("Redirect URI copied");
  } else {
    ElMessage.error("Could not access clipboard — copy it manually");
  }
};
const isEditing = ref(false);
const isSubmitting = ref(false);
const isDeleting = ref(false);
const isConnecting = ref(false);
const connectionToDelete = ref<GoogleAnalyticsConnectionInterface | null>(null);
const activeConnection = ref<GoogleAnalyticsConnectionInterface | undefined>(undefined);

let oauthPopup: Window | null = null;
// Desktop OAuth runs in the system browser (Google blocks embedded webviews),
// so we can't postMessage back. Instead we poll the connection list until the
// backend callback has stored the credential. This timer drives that poll.
let gaOauthPollTimer: ReturnType<typeof setTimeout> | null = null;

const stopGaOauthPolling = () => {
  if (gaOauthPollTimer !== null) {
    clearTimeout(gaOauthPollTimer);
    gaOauthPollTimer = null;
  }
};

// Web OAuth opens a popup we control. If the user closes that window before
// Google redirects back (i.e. cancels), no message ever arrives — this timer
// watches for the close so we reset state instead of hanging on "connecting".
let oauthPopupCloseTimer: ReturnType<typeof setInterval> | null = null;

const stopOauthPopupWatch = () => {
  if (oauthPopupCloseTimer !== null) {
    clearInterval(oauthPopupCloseTimer);
    oauthPopupCloseTimer = null;
  }
};

// After opening the browser, refresh the connection list until the target
// connection shows a linked Google account (the callback upserts it), or we
// hit the timeout. Reports success only for a freshly-linked connection so a
// re-auth of an already-linked one doesn't trigger a false positive.
const pollForGaOauthCompletion = (connectionName: string, wasLinkedBefore: boolean) => {
  stopGaOauthPolling();
  const deadline = Date.now() + 3 * 60 * 1000;
  let consecutiveErrors = 0;
  const giveUp = (message: string) => {
    stopGaOauthPolling();
    isConnecting.value = false;
    ElMessage.warning(message);
  };
  const tick = async () => {
    gaOauthPollTimer = null;
    try {
      const latest = await fetchGoogleAnalyticsConnections();
      connections.value = latest;
      consecutiveErrors = 0;
      // "Linked" means linked under OAuth: a service-account row also carries an
      // oauthUserEmail (its client_email), so we must require authMethod === "oauth"
      // to detect that the callback has actually completed the OAuth sign-in.
      const match = latest.find(
        (c) => c.connectionName === connectionName && c.authMethod === "oauth" && c.oauthUserEmail,
      );
      if (match && !wasLinkedBefore) {
        isConnecting.value = false;
        dialogVisible.value = false;
        ElMessage.success(`Connected as ${match.oauthUserEmail}`);
        return;
      }
    } catch (error) {
      console.error("Error polling GA connections:", error);
      consecutiveErrors += 1;
      if (consecutiveErrors >= 5) {
        giveUp("Lost contact with the server while finishing Google sign-in. Please try again.");
        return;
      }
    }
    // The system-browser OAuth flow can't postMessage back, so a true cancel
    // emits no signal — on timeout we can only report that sign-in never
    // completed (rather than reset silently and leave the user guessing).
    if (Date.now() > deadline) {
      giveUp("Google sign-in wasn't completed — the connection wasn't linked. Try again.");
      return;
    }
    gaOauthPollTimer = setTimeout(tick, 2000);
  };
  gaOauthPollTimer = setTimeout(tick, 2000);
};

const fetchConnections = async () => {
  isLoading.value = true;
  try {
    connections.value = await fetchGoogleAnalyticsConnections();
  } catch (error) {
    console.error("Error fetching GA connections:", error);
    ElMessage.error("Failed to load Google Analytics connections");
  } finally {
    isLoading.value = false;
  }
};

const showAddModal = () => {
  isEditing.value = false;
  activeConnection.value = undefined;
  refreshOAuthConfigured();
  dialogVisible.value = true;
};

const openAddFromGuide = () => {
  setupGuideVisible.value = false;
  showAddModal();
};

// Guided setup is the primary entry point. Refresh the OAuth-configured state so
// the wizard's OAuth branch can skip the console steps when a client already exists.
const openWizard = () => {
  refreshOAuthConfigured();
  wizardVisible.value = true;
};

const openWizardFromForm = () => {
  dialogVisible.value = false;
  openWizard();
};

const showEditModal = (connection: GoogleAnalyticsConnectionInterface) => {
  isEditing.value = true;
  activeConnection.value = { ...connection };
  refreshOAuthConfigured();
  dialogVisible.value = true;
};

const showDeleteModal = (connection: GoogleAnalyticsConnectionInterface) => {
  connectionToDelete.value = connection;
  deleteDialogVisible.value = true;
};

const handleMetadataSave = async (metadata: GoogleAnalyticsConnectionMetadata) => {
  isSubmitting.value = true;
  try {
    await updateGoogleAnalyticsConnectionMetadata(metadata);
    await fetchConnections();
    dialogVisible.value = false;
    ElMessage.success("Connection updated");
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    ElMessage.error(`Failed to update connection: ${message}`);
  } finally {
    isSubmitting.value = false;
  }
};

const handleSaveServiceAccount = async (input: GoogleAnalyticsServiceAccountInput) => {
  isSubmitting.value = true;
  try {
    await createGoogleAnalyticsServiceAccountConnection(input);
    await fetchConnections();
    dialogVisible.value = false;
    ElMessage.success(isEditing.value ? "Connection updated" : "Connection created");
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    ElMessage.error(`Failed to save connection: ${message}`);
  } finally {
    isSubmitting.value = false;
  }
};

const handleConnectOAuth = async (metadata: GoogleAnalyticsConnectionMetadata) => {
  if (!metadata.connectionName.trim()) {
    ElMessage.error("Connection name is required before connecting");
    return;
  }
  isConnecting.value = true;
  try {
    const { authUrl } = await startGoogleAnalyticsOAuth(metadata);

    if (isDesktop) {
      // Embedded webviews are rejected by Google's OAuth (disallowed_useragent),
      // so hand the consent flow to the system browser. The backend callback
      // stores the credential; we poll the list to reflect completion.
      const wasLinkedBefore = connections.value.some(
        (c) =>
          c.connectionName === metadata.connectionName &&
          c.authMethod === "oauth" &&
          c.oauthUserEmail,
      );
      await desktop.openExternal(authUrl);
      ElMessage.info("Continue signing in with Google in your browser…");
      pollForGaOauthCompletion(metadata.connectionName, wasLinkedBefore);
      return;
    }

    oauthPopup = window.open(authUrl, "flowfile-ga-oauth", "width=520,height=720");
    if (!oauthPopup) {
      ElMessage.error("Popup blocked — allow popups for this site and try again");
      isConnecting.value = false;
    } else {
      // A manual popup close is the only cancel signal we get — watch for it.
      stopOauthPopupWatch();
      oauthPopupCloseTimer = setInterval(() => {
        if (oauthPopup && oauthPopup.closed) {
          stopOauthPopupWatch();
          oauthPopup = null;
          isConnecting.value = false;
          ElMessage.info("Google sign-in was cancelled");
        }
      }, 500);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    ElMessage.error(`Could not start OAuth: ${message}`);
    isConnecting.value = false;
  }
};

const handleOAuthMessage = async (event: MessageEvent) => {
  // The OAuth callback and the frontend sit on different origins in most
  // deployments (core on :63578, frontend on :5173/:8080), so we can't check
  // event.origin against window.location.origin. Verifying event.source ===
  // oauthPopup proves the message came from the popup we actually opened,
  // not from some other page posting spoofed OAuth results.
  if (!oauthPopup || event.source !== oauthPopup) return;
  const data = event.data;
  if (!data || data.source !== "flowfile-ga-oauth") return;
  // A result arrived — stop the close-watch so the popup auto-closing (500ms
  // after it postMessages) isn't mistaken for a cancellation.
  stopOauthPopupWatch();
  isConnecting.value = false;
  if (data.status === "ok") {
    await fetchConnections();
    dialogVisible.value = false;
    ElMessage.success(data.message || "Connected");
  } else {
    ElMessage.error(data.message || "Google sign-in failed");
  }
  oauthPopup = null;
};

const handleDeleteConnection = async () => {
  if (!connectionToDelete.value) return;
  isDeleting.value = true;
  try {
    await deleteGoogleAnalyticsConnection(connectionToDelete.value.connectionName);
    await fetchConnections();
    deleteDialogVisible.value = false;
    ElMessage.success("Connection deleted successfully");
  } catch (error) {
    console.error(error);
    ElMessage.error("Failed to delete connection");
  } finally {
    isDeleting.value = false;
    connectionToDelete.value = null;
  }
};

const handleCloseDialog = (done: () => void) => {
  if (isSubmitting.value || isConnecting.value) return;
  done();
};

const handleCloseDeleteDialog = (done: () => void) => {
  if (isDeleting.value) return;
  done();
};

onMounted(() => {
  fetchConnections();
  refreshOAuthConfigured();
  window.addEventListener("message", handleOAuthMessage);
  // Deep link (from the GA reader node or the setup guide): open the Add
  // Connection dialog straight away, then drop the param so a refresh or
  // tab-switch doesn't reopen it.
  if (route.query.action === "add") {
    showAddModal();
    const query = { ...route.query };
    delete query.action;
    router.replace({ query });
  }
});

onBeforeUnmount(() => {
  window.removeEventListener("message", handleOAuthMessage);
  stopGaOauthPolling();
  stopOauthPopupWatch();
});
</script>

<style scoped>
.page-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-3);
}

.btn-sm {
  padding: var(--spacing-1) var(--spacing-3);
  font-size: var(--font-size-sm);
}

.description-text {
  color: var(--color-text-secondary);
  margin-top: var(--spacing-2);
  font-size: var(--font-size-sm);
}

.setup-guide {
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

.setup-intro {
  margin: 0 0 var(--spacing-4);
  color: var(--color-text-secondary);
}

.setup-steps {
  margin: 0;
  padding-left: var(--spacing-5);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
}

.setup-steps li {
  line-height: 1.55;
}

.setup-substeps {
  margin: var(--spacing-2) 0 0;
  padding-left: var(--spacing-4);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  list-style: disc;
}

.setup-action {
  margin-top: var(--spacing-3);
}

.setup-steps code {
  background: var(--color-background-muted);
  padding: 1px 6px;
  border-radius: 4px;
  font-size: 0.85em;
}

.setup-code-row {
  display: flex;
  align-items: stretch;
  gap: var(--spacing-2);
  margin: var(--spacing-2) 0;
}

.setup-code {
  margin: 0;
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-background-muted);
  border-radius: var(--border-radius-sm);
  font-size: 0.85em;
  overflow-x: auto;
  flex: 1;
  user-select: all;
}

.setup-copy-btn {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: 0 var(--spacing-3);
  background: var(--color-background-muted);
  border: 1px solid var(--color-border);
  border-radius: var(--border-radius-sm);
  color: var(--color-text-secondary);
  font-size: 0.8em;
  cursor: pointer;
  white-space: nowrap;
}

.setup-copy-btn:hover {
  background: var(--color-accent-subtle);
  color: var(--color-accent);
}

.setup-note {
  display: flex;
  gap: var(--spacing-3);
  margin-top: var(--spacing-4);
  padding: var(--spacing-3) var(--spacing-4);
  background: var(--color-background-muted);
  border-left: 3px solid var(--color-accent);
  border-radius: var(--border-radius-sm);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.setup-note i {
  color: var(--color-accent);
  margin-top: 2px;
}

.how-it-works-trigger {
  background: transparent;
  border: none;
  cursor: pointer;
  padding: 0;
  margin-left: var(--spacing-2);
  color: var(--color-text-tertiary);
  font-size: var(--font-size-sm);
}

.how-it-works-trigger:hover {
  color: var(--color-accent);
}

.how-it-works-content {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.how-it-works-content p {
  margin: 0 0 var(--spacing-2);
  line-height: 1.5;
}

.how-it-works-content p:last-child {
  margin-bottom: 0;
}

.how-it-works-title {
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
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
  margin-top: var(--spacing-2);
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

.info-box p strong {
  color: var(--color-text-primary);
}

.badge {
  background-color: var(--color-accent-subtle);
  color: var(--color-accent);
  border-radius: var(--border-radius-full);
  padding: var(--spacing-1) var(--spacing-3);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  margin-left: var(--spacing-2);
}

.connection-details {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  margin-top: var(--spacing-1);
}

.hint-text {
  color: var(--color-text-tertiary);
  font-size: var(--font-size-sm);
  margin-top: var(--spacing-2);
}

.fa-chart-line {
  color: var(--color-accent);
}
</style>
