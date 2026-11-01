<template>
  <div class="hub-container">
    <div class="mb-3">
      <h2 class="page-title">Hub</h2>
      <p class="page-description">
        Connect this Flowfile to your organisation's hub. You sign in once with your hub account;
        the password is sent to the hub and never kept here. Your local workspace stays as it is.
      </p>
    </div>

    <ComputeCard card-id="hub-connection" title="Hub connection" icon="fa-solid fa-tower-broadcast">
      <template #meta="{ open }">
        <span
          v-if="!open && summary"
          :class="['compute-chip', `compute-chip--${summary.tone}`]"
          data-testid="hub-summary"
          >{{ summary.label }}</span
        >
      </template>

      <div v-if="loadError" class="hub-card__banner">
        <i class="fa-solid fa-circle-exclamation"></i>
        <span>{{ loadError }}</span>
      </div>

      <p v-if="loading && !state" class="hub-card__loading">Loading…</p>

      <template v-if="state?.connected">
        <div
          class="hub-card__status"
          :class="state.reachable ? 'hub-card__status--success' : 'hub-card__status--warning'"
          data-testid="hub-status"
        >
          <i
            :class="
              state.reachable ? 'fa-solid fa-circle-check' : 'fa-solid fa-triangle-exclamation'
            "
            aria-hidden="true"
          ></i>
          <div>
            <strong>{{ state.orgName ?? state.hubUrl }}</strong>
            <p>
              Connected to <code>{{ state.hubUrl }}</code> as the device
              <code>{{ state.deviceName }}</code
              >.
            </p>
            <p v-if="!state.reachable">The hub is not answering: {{ state.error }}</p>
          </div>
        </div>

        <p
          v-if="state.vendMode === 'static'"
          class="hub-card__warning"
          data-testid="hub-static-warning"
        >
          <i class="fa-solid fa-triangle-exclamation"></i>
          <span>
            This hub runs in <code>static</code> vend mode, a development setup. Don't treat it as a
            production hub.
          </span>
        </p>

        <dl class="hub-card__facts">
          <dt>Organisation</dt>
          <dd>{{ state.orgName ?? "—" }}</dd>
          <dt>Hub version</dt>
          <dd>{{ state.hubVersion ?? "—" }}</dd>
          <dt>Vend mode</dt>
          <dd>{{ state.vendMode ?? "—" }}</dd>
          <dt>Device</dt>
          <dd>{{ state.deviceName }}</dd>
          <dt>Session expires</dt>
          <dd>{{ expiresLabel }}</dd>
        </dl>
      </template>

      <div v-else-if="state" class="hub-card__fields">
        <label class="compute-field hub-card__wide">
          <span class="compute-field__label">Hub URL</span>
          <el-input
            v-model="form.hubUrl"
            v-bind="NO_AUTOFILL"
            placeholder="https://hub.example.com"
            data-testid="hub-url"
          />
        </label>
        <label class="compute-field">
          <span class="compute-field__label">Username</span>
          <el-input v-model="form.username" autocomplete="username" data-testid="hub-username" />
        </label>
        <label class="compute-field">
          <span class="compute-field__label">Password</span>
          <el-input
            v-model="form.password"
            type="password"
            show-password
            autocomplete="current-password"
            data-testid="hub-password"
          />
        </label>
        <label class="compute-field hub-card__wide">
          <span class="compute-field__label">Device name</span>
          <el-input v-model="form.deviceName" v-bind="NO_AUTOFILL" data-testid="hub-device-name" />
          <span class="compute-field__hint">
            How this machine shows on the hub. The same name under the same account is the same
            device.
          </span>
        </label>
      </div>

      <template v-if="state" #footer>
        <el-button
          v-if="state.connected"
          size="small"
          plain
          :loading="busy"
          data-testid="hub-disconnect"
          @click="onDisconnect"
        >
          Disconnect
        </el-button>
        <el-button
          v-else
          type="primary"
          size="small"
          :loading="busy"
          :disabled="!canConnect"
          data-testid="hub-connect"
          @click="onConnect"
        >
          Connect
        </el-button>
      </template>
    </ComputeCard>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, watch } from "vue";
import { ElMessage } from "element-plus";
import { NO_AUTOFILL } from "../../utils/noAutofill";
import ComputeCard from "./ComputeCard.vue";
import { useHubConnection } from "./useHubConnection";
import type { StatusSummary } from "../../components/settings/handshakeStatus";

const { state, loading, loadError, busy, load, connect, disconnect } = useHubConnection();

const form = reactive({ hubUrl: "", username: "", password: "", deviceName: "" });

watch(state, (next) => {
  if (next && !next.connected && !form.deviceName) form.deviceName = next.deviceName ?? "";
});

const summary = computed<StatusSummary | null>(() => {
  if (!state.value) return null;
  if (!state.value.connected) return { tone: "neutral", label: "Not connected" };
  if (!state.value.reachable) return { tone: "warning", label: "Hub not answering" };
  return { tone: "success", label: "Connected" };
});

const expiresLabel = computed(() =>
  state.value?.expiresAt ? new Date(state.value.expiresAt).toLocaleString() : "—",
);

const canConnect = computed(
  () => form.hubUrl.trim() !== "" && form.username.trim() !== "" && form.password !== "",
);

const onConnect = async () => {
  if (!canConnect.value) return;
  try {
    const next = await connect({
      hubUrl: form.hubUrl.trim(),
      username: form.username.trim(),
      password: form.password,
      deviceName: form.deviceName.trim() || undefined,
    });
    ElMessage.success(`Connected to ${next.orgName ?? next.hubUrl}.`);
  } catch (error) {
    ElMessage.error((error as Error).message);
  } finally {
    form.password = "";
  }
};

const onDisconnect = async () => {
  try {
    await disconnect();
    ElMessage.success("Disconnected from the hub.");
  } catch (error) {
    ElMessage.error((error as Error).message);
  }
};

onMounted(() => {
  void load();
});
</script>

<style scoped src="./computeSettings.css"></style>
<style scoped>
.hub-container {
  max-width: 1320px;
  margin: 0 auto;
  padding: var(--spacing-5);
}
.hub-card__banner,
.hub-card__warning {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  padding: var(--spacing-3);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  line-height: var(--line-height-normal);
}
.hub-card__banner {
  margin-bottom: var(--spacing-3);
  background: var(--color-danger-light);
  color: var(--color-danger-dark);
}
.hub-card__warning {
  margin: var(--spacing-3) 0 0;
  background: var(--color-warning-light);
  color: var(--color-warning-dark);
}
.hub-card__banner i,
.hub-card__warning i,
.hub-card__status > i {
  margin-top: 3px;
  flex-shrink: 0;
}
.hub-card__loading {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}
.hub-card__status {
  display: flex;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
  font-size: var(--font-size-sm);
}
.hub-card__status > div {
  min-width: 0;
}
.hub-card__status strong {
  font-weight: var(--font-weight-medium);
}
.hub-card__status p {
  color: var(--color-text-secondary);
  margin: var(--spacing-1) 0 0;
  line-height: var(--line-height-normal);
  overflow-wrap: anywhere;
}
.hub-card__status--success {
  color: var(--color-success-dark);
  border-left: 3px solid var(--color-success);
}
.hub-card__status--warning {
  color: var(--color-warning-dark);
  border-left: 3px solid var(--color-warning);
}
.hub-card__facts {
  display: grid;
  grid-template-columns: max-content minmax(0, 1fr);
  gap: var(--spacing-1) var(--spacing-4);
  margin: var(--spacing-4) 0 0;
  font-size: var(--font-size-sm);
}
.hub-card__facts dt {
  color: var(--color-text-secondary);
}
.hub-card__facts dd {
  margin: 0;
  overflow-wrap: anywhere;
}
.hub-card__fields {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  align-items: start;
  gap: var(--spacing-4);
}
.hub-card__wide {
  grid-column: 1 / -1;
}
</style>
