<template>
  <div class="ga-connection-manager-container">
    <div class="mb-3">
      <div class="page-header">
        <h2 class="page-title">Google Analytics Connections</h2>
        <button type="button" class="btn btn-secondary btn-sm" @click="setupGuideVisible = true">
          <i class="fa-solid fa-circle-question"></i>
          How to set up
        </button>
      </div>
      <p class="description-text">
        Google Analytics connections store the OAuth refresh token minted when you sign in with
        Google. Tokens are encrypted at rest with your user-derived key and are never transmitted
        back to the browser after creation.
      </p>
    </div>

    <GoogleOAuthClientCard />

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
                Click <em>Add Connection</em>, give it a name, then click
                <em>Connect Google Account</em>. You'll sign in with the Google account that has
                Viewer access to your GA4 property. Flowfile stores a refresh token (encrypted at
                rest with your user-derived key) so it can read GA4 on your behalf — no
                service-account key is ever required.
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
          <p class="hint-text">Click "Add Connection" to create your first one.</p>
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
                Connected as {{ connection.oauthUserEmail }}
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
        @save-metadata="handleMetadataSave"
        @connect-oauth="handleConnectOAuth"
        @cancel="dialogVisible = false"
      />
    </el-dialog>

    <el-dialog v-model="setupGuideVisible" title="Set up Google OAuth for Flowfile" width="720px">
      <div class="setup-guide">
        <p class="setup-intro">
          Flowfile talks to GA4 using your own Google OAuth client. It takes about 5 minutes in the
          Google Cloud console.
        </p>

        <ol class="setup-steps">
          <li>
            <strong>Create (or pick) a Google Cloud project.</strong>
            Go to
            <SetupLink href="https://console.cloud.google.com/projectcreate">
              console.cloud.google.com/projectcreate
            </SetupLink>
            and create a project, or select any existing one from the project picker.
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
            <strong>Configure the OAuth consent screen.</strong>
            Go to
            <SetupLink href="https://console.cloud.google.com/apis/credentials/consent">
              APIs &amp; Services → OAuth consent screen
            </SetupLink>
            . Choose <em>External</em> (unless you have a Workspace), fill in the app name and your
            email, and add the scope
            <code>https://www.googleapis.com/auth/analytics.readonly</code>. Add your Google account
            as a test user while the app is in <em>Testing</em> mode.
          </li>
          <li>
            <strong>Create the OAuth client.</strong>
            Go to
            <SetupLink href="https://console.cloud.google.com/apis/credentials">
              APIs &amp; Services → Credentials
            </SetupLink>
            , click <em>Create credentials → OAuth client ID</em>, and pick
            <strong>Web application</strong>.
          </li>
          <li>
            <strong>Add the redirect URI.</strong>
            Under <em>Authorised redirect URIs</em>, add exactly:
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
import { ref, onMounted, onBeforeUnmount } from "vue";
import { ElDialog, ElButton, ElMessage, ElPopover } from "element-plus";
import {
  deleteGoogleAnalyticsConnection,
  fetchGoogleAnalyticsConnections,
  startGoogleAnalyticsOAuth,
  updateGoogleAnalyticsConnectionMetadata,
} from "./api";
import type {
  GoogleAnalyticsConnectionInterface,
  GoogleAnalyticsConnectionMetadata,
} from "./GoogleAnalyticsConnectionTypes";
import GoogleAnalyticsConnectionSettings from "./GoogleAnalyticsConnectionSettings.vue";
import GoogleOAuthClientCard from "./GoogleOAuthClientCard.vue";
import SetupLink from "./SetupLink.vue";

const connections = ref<GoogleAnalyticsConnectionInterface[]>([]);
const isLoading = ref(true);
const dialogVisible = ref(false);
const deleteDialogVisible = ref(false);
const setupGuideVisible = ref(false);
const redirectUri = "http://localhost:63578/ga_connections/oauth/callback";

const copyRedirectUri = async () => {
  try {
    await navigator.clipboard.writeText(redirectUri);
    ElMessage.success("Redirect URI copied");
  } catch {
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
  dialogVisible.value = true;
};

const showEditModal = (connection: GoogleAnalyticsConnectionInterface) => {
  isEditing.value = true;
  activeConnection.value = { ...connection };
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

const handleConnectOAuth = async (metadata: GoogleAnalyticsConnectionMetadata) => {
  if (!metadata.connectionName.trim()) {
    ElMessage.error("Connection name is required before connecting");
    return;
  }
  isConnecting.value = true;
  try {
    const { authUrl } = await startGoogleAnalyticsOAuth(metadata);
    oauthPopup = window.open(authUrl, "flowfile-ga-oauth", "width=520,height=720");
    if (!oauthPopup) {
      ElMessage.error("Popup blocked — allow popups for this site and try again");
      isConnecting.value = false;
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
  window.addEventListener("message", handleOAuthMessage);
});

onBeforeUnmount(() => {
  window.removeEventListener("message", handleOAuthMessage);
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
