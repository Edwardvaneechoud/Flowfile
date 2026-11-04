<template>
  <div class="hub-container">
    <div class="mb-3">
      <h2 class="page-title">Hub</h2>
      <p class="page-description">
        <template v-if="state?.joined">The hub this server belongs to.</template>
        <template v-else>
          Connect this Flowfile to your organisation's hub. You sign in once with your hub account;
          the password is sent to the hub and never kept here. Your local workspace stays as it is.
        </template>
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

      <template v-if="state?.joined">
        <div
          class="hub-card__status"
          :class="state.reachable ? 'hub-card__status--success' : 'hub-card__status--warning'"
          data-testid="hub-joined"
        >
          <i
            :class="
              state.reachable ? 'fa-solid fa-circle-check' : 'fa-solid fa-triangle-exclamation'
            "
            aria-hidden="true"
          ></i>
          <div>
            <strong>{{ belongsToLine(state) }}</strong>
            <p v-if="state.account" data-testid="hub-account">Signed in as {{ state.account }}.</p>
            <p v-if="!state.reachable">The hub is not answering: {{ state.error }}</p>
          </div>
        </div>
        <p v-if="isAdmin" class="hub-nodes__hint" data-testid="hub-servers-link">
          The hub's servers, with their state, are on
          <router-link :to="{ query: { tab: 'backends' } }">Backends</router-link>.
        </p>
      </template>

      <template v-else-if="state?.connected">
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
          <span>{{ STATIC_VEND_NOTE }}</span>
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

        <section class="hub-nodes" aria-labelledby="hub-nodes-title">
          <div class="hub-nodes__header">
            <h3 id="hub-nodes-title" class="hub-nodes__title">Team nodes</h3>
            <div class="hub-nodes__actions">
              <el-button
                size="small"
                plain
                :loading="syncing"
                data-testid="hub-sync-nodes"
                @click="onSyncNodes"
              >
                <i class="fa-solid fa-rotate" aria-hidden="true"></i> Sync nodes
              </el-button>
              <el-button
                size="small"
                plain
                :loading="checking"
                data-testid="hub-check-nodes"
                @click="onCheckNodes"
              >
                <i class="fa-solid fa-satellite-dish" aria-hidden="true"></i> Check
              </el-button>
            </div>
          </div>
          <p class="hub-nodes__hint">
            The hub's compute nodes, listed as workers a flow or node can pick. The first use asks
            the hub for a token that lasts an hour; nothing is stored here. After a sync each node
            is checked once from this computer; Check asks again.
          </p>
          <p v-if="syncNotice" class="hub-nodes__notice" data-testid="hub-sync-notice">
            {{ syncNotice }}
          </p>
          <ul v-if="teamNodes.length" class="hub-nodes__list" aria-label="Team nodes">
            <li
              v-for="row in teamNodes"
              :key="row.key"
              class="hub-node"
              :data-testid="`hub-node-${row.key}`"
            >
              <div class="hub-node__identity">
                <code>{{ row.name }}</code>
                <span v-if="row.label" class="hub-node__label">{{ row.label }}</span>
                <span v-if="row.backend" class="hub-node__url">{{ row.backend.url ?? "—" }}</span>
                <span
                  v-if="row.backend && reachabilityNote(row.backend)"
                  class="hub-node__reason"
                  :data-testid="`hub-node-reason-${row.key}`"
                  >{{ reachabilityNote(row.backend) }}</span
                >
              </div>
              <span
                v-if="row.backend && reachabilityStatus(row.backend)"
                :class="['compute-chip', `compute-chip--${reachabilityStatus(row.backend)?.tone}`]"
                :data-testid="`hub-node-reachability-${row.key}`"
                >{{ reachabilityStatus(row.backend)?.label }}</span
              >
            </li>
          </ul>
          <p v-else class="hub-nodes__empty" data-testid="hub-nodes-empty">
            No team nodes yet. Sync to read the hub's list.
          </p>
        </section>
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

      <template v-if="state && !state.joined" #footer>
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
import { teamNodeRows, useHubConnection } from "./useHubConnection";
import { reachabilityStatus, type StatusSummary } from "../../components/settings/handshakeStatus";
import { reachabilityNote } from "../../utils/executionLocation";
import { useAuthStore } from "../../stores/auth-store";
import { belongsToLine, STATIC_VEND_NOTE } from "../../components/layout/Header/hubIdentity";

const {
  state,
  loading,
  loadError,
  busy,
  nodes,
  syncing,
  checking,
  syncNotice,
  load,
  connect,
  disconnect,
  syncNodes,
  checkNodes,
} = useHubConnection();

const isAdmin = computed(() => useAuthStore().isAdmin);
const teamNodes = computed(() => teamNodeRows(state.value, nodes.value));

const form = reactive({ hubUrl: "", username: "", password: "", deviceName: "" });

watch(state, (next) => {
  if (next && !next.connected && !form.deviceName) form.deviceName = next.deviceName ?? "";
});

const summary = computed<StatusSummary | null>(() => {
  if (!state.value) return null;
  if (state.value.joined) return { tone: "success", label: "Joined" };
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

const onSyncNodes = async () => {
  try {
    await syncNodes();
  } catch (error) {
    ElMessage.error((error as Error).message);
  }
};

const onCheckNodes = async () => {
  try {
    await checkNodes();
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
.hub-nodes {
  margin-top: var(--spacing-5);
  padding-top: var(--spacing-4);
  border-top: 1px solid var(--color-border-light);
}
.hub-nodes__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-3);
}
.hub-nodes__title {
  margin: 0;
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-medium);
}
.hub-nodes__actions {
  display: flex;
  gap: var(--spacing-2);
}
.hub-nodes__actions .el-button + .el-button {
  margin-left: 0;
}
.hub-nodes__hint,
.hub-nodes__empty {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: var(--line-height-normal);
}
.hub-nodes__notice {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}
.hub-nodes__list {
  list-style: none;
  margin: var(--spacing-3) 0 0;
  padding: 0;
  display: grid;
  gap: var(--spacing-2);
}
.hub-node {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
  font-size: var(--font-size-sm);
}
.hub-node__identity {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
  min-width: 0;
}
.hub-node__url {
  color: var(--color-text-secondary);
  overflow-wrap: anywhere;
}
.hub-node__label {
  color: var(--color-text-secondary);
}
.hub-node__reason {
  color: var(--color-danger-dark);
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
