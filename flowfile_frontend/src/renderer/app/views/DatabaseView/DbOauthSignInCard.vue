<template>
  <div class="oauth-card" :class="connected ? 'oauth-card--connected' : ''">
    <div class="oauth-card-status">
      <template v-if="connected">
        <i class="fa-solid fa-circle-check status-icon connected"></i>
        <div>
          <p class="status-title">Signed in</p>
          <p class="status-hint">Scheduled and interactive runs use this sign-in.</p>
        </div>
      </template>
      <template v-else>
        <i class="fa-solid fa-circle-exclamation status-icon"></i>
        <div>
          <p class="status-title">Not signed in</p>
          <p class="status-hint">
            Sign in with your identity provider to finish setting up this connection.
          </p>
        </div>
      </template>
    </div>
    <button type="button" class="btn btn-primary" :disabled="isConnecting" @click="startSignIn">
      <i class="fa-solid fa-right-to-bracket"></i>
      {{ isConnecting ? "Waiting for sign-in…" : connected ? "Re-authenticate" : "Sign in" }}
    </button>
  </div>
</template>

<script lang="ts" setup>
import { ref, onBeforeUnmount } from "vue";
import { ElMessage } from "element-plus";
import { isDesktop, desktop } from "../../../lib/desktop";
import { startDbOauthApi, fetchDatabaseConnectionsInterfaces } from "./api";

const props = defineProps<{
  connectionName: string;
  connected?: boolean;
}>();

const emit = defineEmits<{
  (e: "changed"): void;
}>();

const isConnecting = ref(false);

let oauthPopup: Window | null = null;
// Web mode: watch for a manual popup close (the only cancel signal we get).
let popupCloseTimer: ReturnType<typeof setInterval> | null = null;
// Desktop mode: the system browser can't postMessage back, so poll the
// connection list until the backend callback has stored the refresh token.
let pollTimer: ReturnType<typeof setTimeout> | null = null;

const stopPopupWatch = () => {
  if (popupCloseTimer !== null) {
    clearInterval(popupCloseTimer);
    popupCloseTimer = null;
  }
};

const stopPolling = () => {
  if (pollTimer !== null) {
    clearTimeout(pollTimer);
    pollTimer = null;
  }
};

const pollForCompletion = () => {
  stopPolling();
  const deadline = Date.now() + 3 * 60 * 1000;
  const tick = async () => {
    pollTimer = null;
    try {
      const latest = await fetchDatabaseConnectionsInterfaces();
      const match = latest.find(
        (c) => c.connectionName === props.connectionName && c.oauthConnected,
      );
      // Re-authentication of an already-connected row can't be distinguished
      // here, so only the not-yet-connected -> connected transition reports.
      if (match && !props.connected) {
        isConnecting.value = false;
        ElMessage.success(`Connection '${props.connectionName}' is signed in`);
        emit("changed");
        return;
      }
    } catch (error) {
      console.error("Error polling database connections:", error);
    }
    if (Date.now() > deadline) {
      isConnecting.value = false;
      ElMessage.warning("Sign-in wasn't completed — the connection isn't linked yet. Try again.");
      return;
    }
    pollTimer = setTimeout(tick, 2000);
  };
  pollTimer = setTimeout(tick, 2000);
};

const handleOAuthMessage = (event: MessageEvent) => {
  // Core and the frontend sit on different origins, so verify event.source
  // against the popup we opened instead of checking event.origin.
  if (!oauthPopup || event.source !== oauthPopup) return;
  const data = event.data;
  if (!data || data.source !== "flowfile-db-oauth") return;
  stopPopupWatch();
  isConnecting.value = false;
  oauthPopup = null;
  if (data.status === "ok") {
    ElMessage.success(data.message || "Signed in");
    emit("changed");
  } else {
    ElMessage.error(data.message || "Sign-in failed");
  }
};

window.addEventListener("message", handleOAuthMessage);

const startSignIn = async () => {
  isConnecting.value = true;
  try {
    const authUrl = await startDbOauthApi(props.connectionName);
    if (isDesktop) {
      await desktop.openExternal(authUrl);
      ElMessage.info("Continue signing in with your identity provider in the browser…");
      pollForCompletion();
      return;
    }
    oauthPopup = window.open(authUrl, "flowfile-db-oauth", "width=520,height=720");
    if (!oauthPopup) {
      ElMessage.error("Popup blocked — allow popups for this site and try again");
      isConnecting.value = false;
      return;
    }
    stopPopupWatch();
    popupCloseTimer = setInterval(() => {
      if (oauthPopup && oauthPopup.closed) {
        stopPopupWatch();
        oauthPopup = null;
        isConnecting.value = false;
        ElMessage.info("Sign-in was cancelled");
      }
    }, 500);
  } catch (error: any) {
    const detail = error?.response?.data?.detail;
    const message =
      (typeof detail === "string" ? detail : detail?.message) ||
      (error instanceof Error ? error.message : "Unknown error");
    ElMessage.error(`Could not start sign-in: ${message}`);
    isConnecting.value = false;
  }
};

onBeforeUnmount(() => {
  window.removeEventListener("message", handleOAuthMessage);
  stopPopupWatch();
  stopPolling();
});
</script>

<style scoped>
.oauth-card {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  border: 1px solid var(--color-border);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-muted);
}

.oauth-card--connected {
  border-color: var(--color-success, #67c23a);
}

.oauth-card-status {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
}

.status-icon {
  font-size: var(--font-size-xl);
  color: var(--color-warning, #e6a23c);
}

.status-icon.connected {
  color: var(--color-success, #67c23a);
}

.status-title {
  margin: 0;
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.status-hint {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}
</style>
